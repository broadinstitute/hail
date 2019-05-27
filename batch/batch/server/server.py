import asyncio
import concurrent
import functools
import logging
import os
import threading
import json
import uuid
from shlex import quote as shq

from aiohttp import web
import cerberus
import kubernetes as kube
import requests
import uvloop

from hailjwt import authenticated_users_only

from .globals import blocking_to_async
from .globals import write_gs_log_file, read_gs_log_file, delete_gs_log_file
from .database import BatchDatabase

from .. import schemas


def make_logger():
    fmt = logging.Formatter(
        # NB: no space after levename because WARNING is so long
        '%(levelname)s\t| %(asctime)s \t| %(filename)s \t| %(funcName)s:%(lineno)d | '
        '%(message)s')

    file_handler = logging.FileHandler('batch.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)

    log = logging.getLogger('batch')
    log.setLevel(logging.INFO)

    logging.basicConfig(handlers=[file_handler, stream_handler], level=logging.INFO)

    return log


log = make_logger()


uvloop.install()


def schedule(ttl, fun, args=(), kwargs=None):
    if kwargs is None:
        kwargs = {}
    asyncio.get_event_loop().call_later(ttl, functools.partial(fun, *args, **kwargs))


KUBERNETES_TIMEOUT_IN_SECONDS = float(os.environ.get('KUBERNETES_TIMEOUT_IN_SECONDS', 5.0))
REFRESH_INTERVAL_IN_SECONDS = int(os.environ.get('REFRESH_INTERVAL_IN_SECONDS', 5 * 60))
HAIL_POD_NAMESPACE = os.environ.get('HAIL_POD_NAMESPACE', 'batch-pods')
POD_VOLUME_SIZE = os.environ.get('POD_VOLUME_SIZE', '10Mi')
INSTANCE_ID = os.environ.get('HAIL_INSTANCE_ID', uuid.uuid4().hex)

log.info(f'KUBERNETES_TIMEOUT_IN_SECONDS {KUBERNETES_TIMEOUT_IN_SECONDS}')
log.info(f'REFRESH_INTERVAL_IN_SECONDS {REFRESH_INTERVAL_IN_SECONDS}')
log.info(f'HAIL_POD_NAMESPACE {HAIL_POD_NAMESPACE}')
log.info(f'POD_VOLUME_SIZE {POD_VOLUME_SIZE}')
log.info(f'INSTANCE_ID = {INSTANCE_ID}')

STORAGE_CLASS_NAME = 'batch'

if 'BATCH_USE_KUBE_CONFIG' in os.environ:
    kube.config.load_kube_config()
else:
    kube.config.load_incluster_config()
v1 = kube.client.CoreV1Api()

app = web.Application()
routes = web.RouteTableDef()

db = BatchDatabase.create_synchronous('/batch-user-secret/sql-config.json')


def abort(code, reason=None):
    if code == 400:
        raise web.HTTPBadRequest(reason=reason)
    if code == 404:
        raise web.HTTPNotFound(reason=reason)
    raise web.HTTPException(reason=reason)


def jsonify(data):
    return web.json_response(data)


class JobTask:  # pylint: disable=R0903
    @staticmethod
    def from_dict(d):
        name = d['name']
        pod_spec_dict = d['pod_spec_dict']
        return JobTask(name, pod_spec_dict)

    @staticmethod
    def copy_task(task_name, files):
        if files is not None:
            authenticate = 'set -ex; gcloud -q auth activate-service-account --key-file=/gsa-key/privateKeyData'

            def copy_command(src, dst):
                if not dst.startswith('gs://'):
                    mkdirs = f'mkdir -p {shq(os.path.dirname(dst))};'
                else:
                    mkdirs = ""
                return f'{mkdirs} gsutil -m cp -R {shq(src)} {shq(dst)}'

            copies = ' && '.join([copy_command(src, dst) for (src, dst) in files])
            sh_expression = f'{authenticate} && {copies}'
            container = kube.client.V1Container(
                image='google/cloud-sdk:237.0.0-alpine',
                name=task_name,
                command=['/bin/sh', '-c', sh_expression])
            spec = kube.client.V1PodSpec(
                containers=[container],
                restart_policy='Never')
            return JobTask.from_spec(task_name, spec)
        return None

    @staticmethod
    def from_spec(name, pod_spec):
        assert pod_spec is not None
        return JobTask(name, v1.api_client.sanitize_for_serialization(pod_spec))

    def __init__(self, name, pod_spec_dict):
        self.pod_spec_dict = pod_spec_dict
        self.name = name

    def to_dict(self):
        return {'name': self.name,
                'pod_spec_dict': self.pod_spec_dict}


class Job:
    def _has_next_task(self):
        return self._task_idx < len(self._tasks)

    async def _create_pvc(self):
        try:
            pvc = v1.create_namespaced_persistent_volume_claim(
                HAIL_POD_NAMESPACE,
                kube.client.V1PersistentVolumeClaim(
                    metadata=kube.client.V1ObjectMeta(
                        generate_name=f'job-{self.id}-',
                        labels={'app': 'batch-job',
                                'hail.is/batch-instance': INSTANCE_ID}),
                    spec=kube.client.V1PersistentVolumeClaimSpec(
                        access_modes=['ReadWriteOnce'],
                        volume_mode='Filesystem',
                        resources=kube.client.V1ResourceRequirements(
                            requests={'storage': POD_VOLUME_SIZE}),
                        storage_class_name=STORAGE_CLASS_NAME)),
                _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
            pvc_name = pvc.metadata.name
            await db.jobs.update_record(self.id, pvc_name=pvc_name)
            log.info(f'created pvc name: {pvc_name} for job {self.id}')
            return pvc_name
        except kube.client.rest.ApiException as err:
            log.info(f'persistent volume claim cannot be created for job {self.id} with the following error: {err}')
            return None

    # may be called twice with the same _current_task
    async def _create_pod(self):
        assert self._pod_name is None
        assert self._current_task is not None
        assert self.userdata is not None

        volumes = [
            kube.client.V1Volume(
                secret=kube.client.V1SecretVolumeSource(
                    secret_name=self.userdata['gsa_key_secret_name']),
                name='gsa-key')]
        volume_mounts = [
            kube.client.V1VolumeMount(
                mount_path='/gsa-key',
                name='gsa-key')]

        if len(self._tasks) > 1:
            if self._pvc_name is None:
                self._pvc_name = await self._create_pvc()
                if self._pvc_name is None:
                    log.info(f'could not create pod for job {self.id} due to pvc creation failure')
                    return
            volumes.append(kube.client.V1Volume(
                persistent_volume_claim=kube.client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=self._pvc_name),
                name=self._pvc_name))
            volume_mounts.append(kube.client.V1VolumeMount(
                mount_path='/io',
                name=self._pvc_name))

        pod_spec = v1.api_client._ApiClient__deserialize(self._current_task.pod_spec_dict, kube.client.V1PodSpec)
        if pod_spec.volumes is None:
            pod_spec.volumes = []
        pod_spec.volumes.extend(volumes)
        for container in pod_spec.containers:
            if container.volume_mounts is None:
                container.volume_mounts = []
            container.volume_mounts.extend(volume_mounts)

        pod_template = kube.client.V1Pod(
            metadata=kube.client.V1ObjectMeta(
                generate_name='job-{}-{}-'.format(self.id, self._current_task.name),
                labels={'app': 'batch-job',
                        'hail.is/batch-instance': INSTANCE_ID,
                        'uuid': uuid.uuid4().hex
                        }),
            spec=pod_spec)

        try:
            pod = v1.create_namespaced_pod(
                HAIL_POD_NAMESPACE,
                pod_template,
                _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
            self._pod_name = pod.metadata.name

            await db.jobs.update_record(self.id,
                                        pod_name=self._pod_name)

            log.info('created pod name: {} for job {}, task {}'.format(self._pod_name,
                                                                       self.id,
                                                                       self._current_task.name))
        except kube.client.rest.ApiException as err:
            log.info(f'pod creation failed for job {self.id} with the following error: {err}')

    async def _delete_pvc(self):
        if self._pvc_name is None:
            return

        log.info(f'deleting persistent volume claim {self._pvc_name}')
        try:
            v1.delete_namespaced_persistent_volume_claim(
                self._pvc_name,
                HAIL_POD_NAMESPACE,
                _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
        except kube.client.rest.ApiException as err:
            if err.status == 404:
                log.info(f'persistent volume claim {self._pvc_name} is already deleted')
                return
            raise
        finally:
            await db.jobs.update_record(self.id, pvc_name=None)
            self._pvc_name = None

    async def _delete_k8s_resources(self):
        await self._delete_pvc()
        if self._pod_name is not None:
            try:
                v1.delete_namespaced_pod(
                    self._pod_name,
                    HAIL_POD_NAMESPACE,
                    _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
            except kube.client.rest.ApiException as err:
                if err.status == 404:
                    pass
                raise
            finally:
                await db.jobs.update_record(self.id, pod_name=None)
                self._pod_name = None

    async def _read_logs(self):
        async def _read_log(jt):
            log_uri = await db.jobs.get_log_uri(self.id, jt.name)
            return jt.name, await read_gs_log_file(app['blocking_pool'], log_uri)

        future_logs = asyncio.gather(*[_read_log(jt) for idx, jt in enumerate(self._tasks)
                                       if idx < self._task_idx])
        logs = {k: v for k, v in await future_logs}

        if self._state == 'Ready':
            if self._pod_name:
                try:
                    log = v1.read_namespaced_pod_log(
                        self._pod_name,
                        HAIL_POD_NAMESPACE,
                        _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
                    logs[self._current_task.name] = log
                except kube.client.rest.ApiException:
                    pass
            return logs
        if self._state == 'Complete':
            return logs
        assert self._state == 'Cancelled' or self._state == 'Created'
        return None

    async def _mark_job_task_complete(self, task_name, log, exit_code):
        self.exit_codes[self._task_idx] = exit_code

        self._task_idx += 1
        self._current_task = self._tasks[self._task_idx] if self._task_idx < len(self._tasks) else None

        if self._pod_name:
            self._pod_name = None

        uri = None
        if log is not None:
            uri = await write_gs_log_file(app['blocking_pool'], INSTANCE_ID, self.id, task_name, log)

        await db.jobs.update_with_log_ec(self.id, task_name, uri, exit_code,
                                         task_idx=self._task_idx,
                                         pod_name=self._pod_name,
                                         duration=self.duration)

    async def _delete_logs(self):
        for idx, jt in enumerate(self._tasks):
            if idx < self._task_idx:
                await delete_gs_log_file(app['blocking_pool'], INSTANCE_ID, self.id, jt.name)

    @staticmethod
    def from_record(record):
        if record is not None:
            tasks = [JobTask.from_dict(item) for item in json.loads(record['tasks'])]
            attributes = json.loads(record['attributes'])
            userdata = json.loads(record['userdata'])

            exit_codes = [record[db.jobs.exit_code_field(t.name)] for t in tasks]
            ec_indices = [idx for idx, ec in enumerate(exit_codes) if ec is not None]
            assert record['task_idx'] == 1 + (ec_indices[-1] if len(ec_indices) else -1)

            return Job(id=record['id'], batch_id=record['batch_id'], attributes=attributes,
                       callback=record['callback'], userdata=userdata, user=record['user'],
                       always_run=record['always_run'], pvc_name=record['pvc_name'], pod_name=record['pod_name'],
                       exit_codes=exit_codes, duration=record['duration'], tasks=tasks,
                       task_idx=record['task_idx'], state=record['state'])
        return None

    @staticmethod
    async def from_db(id, user):
        jobs = await Job.from_db_multiple(id, user)
        if len(jobs) == 1:
            return jobs[0]
        return None

    @staticmethod
    async def from_db_multiple(ids, user):
        records = await db.jobs.get_undeleted_records(ids, user)
        jobs = [Job.from_record(record) for record in records]
        return jobs

    @staticmethod
    async def create_job(pod_spec, batch_id, attributes, callback, parent_ids,
                         input_files, output_files, userdata, always_run):
        pvc_name = None
        pod_name = None
        duration = 0
        task_idx = 0
        state = 'Created'
        user = userdata['username']

        tasks = [JobTask.copy_task('input', input_files),
                 JobTask.from_spec('main', pod_spec),
                 JobTask.copy_task('output', output_files)]

        tasks = [t for t in tasks if t is not None]
        exit_codes = [None for _ in tasks]

        id = await db.jobs.new_record(state=state,
                                      batch_id=batch_id,
                                      pod_name=pod_name,
                                      pvc_name=pvc_name,
                                      callback=callback,
                                      attributes=json.dumps(attributes),
                                      tasks=json.dumps([jt.to_dict() for jt in tasks]),
                                      task_idx=task_idx,
                                      always_run=always_run,
                                      duration=duration,
                                      userdata=json.dumps(userdata),
                                      user=user)

        job = Job(id=id, batch_id=batch_id, attributes=attributes, callback=callback,
                  userdata=userdata, user=user, always_run=always_run, pvc_name=pvc_name,
                  pod_name=pod_name, exit_codes=exit_codes, duration=duration, tasks=tasks,
                  task_idx=task_idx, state=state)

        for parent in parent_ids:
            await db.jobs_parents.new_record(job_id=id,
                                             parent_id=parent)

        log.info('created job {}'.format(id))

        if not parent_ids:
            await job.set_state('Ready')
            await job._create_pod()
        else:
            await job.refresh_parents_and_maybe_create()

        return job

    def __init__(self, id, batch_id, attributes, callback, userdata, user, always_run,
                 pvc_name, pod_name, exit_codes, duration, tasks, task_idx, state):
        self.id = id
        self.batch_id = batch_id
        self.attributes = attributes
        self.callback = callback
        self.always_run = always_run
        self.userdata = userdata
        self.user = user
        self.exit_codes = exit_codes
        self.duration = duration

        self._pvc_name = pvc_name
        self._pod_name = pod_name
        self._tasks = tasks
        self._task_idx = task_idx
        self._current_task = tasks[task_idx] if task_idx < len(tasks) else None
        self._state = state

    async def refresh_parents_and_maybe_create(self):
        for record in await db.jobs.get_parents(self.id):
            parent_job = Job.from_record(record)
            await self.parent_new_state(parent_job._state, parent_job.id)

    async def set_state(self, new_state):
        if self._state != new_state:
            await db.jobs.update_record(self.id, state=new_state)
            log.info('job {} changed state: {} -> {}'.format(
                self.id,
                self._state,
                new_state))
            self._state = new_state
            await self.notify_children(new_state)

    async def notify_children(self, new_state):
        children = [Job.from_record(record) for record in await db.jobs.get_children(self.id)]
        for child in children:
            if child:
                await child.parent_new_state(new_state, self.id)
            else:
                log.info(f'missing child: {child.id}')

    async def parent_new_state(self, new_state, parent_id):
        assert await db.jobs_parents.has_record(self.id, parent_id)
        if new_state in ('Cancelled', 'Complete'):
            await self.create_if_ready()

    async def create_if_ready(self):
        incomplete_parent_ids = await db.jobs.get_incomplete_parents(self.id)
        if self._state == 'Created' and not incomplete_parent_ids:
            parents = [Job.from_record(record) for record in await db.jobs.get_parents(self.id)]
            if self.always_run or all(p.is_successful() for p in parents):
                log.info(f'all parents complete for {self.id},'
                         f' creating pod')
                await self.set_state('Ready')
                await self._create_pod()
            else:
                log.info(f'parents deleted, cancelled, or failed: cancelling {self.id}')
                await self.set_state('Cancelled')

    async def cancel(self):
        # Cancelled, Complete
        if self.is_complete():
            return
        if self._state == 'Created':
            if not self.always_run:
                await self.set_state('Cancelled')
        else:
            assert self._state == 'Ready', self._state
            if not self.always_run:
                await self.set_state('Cancelled')  # must call before deleting resources to prevent race conditions
                await self._delete_k8s_resources()

    def is_complete(self):
        return self._state in ('Complete', 'Cancelled')

    def is_successful(self):
        return self._state == 'Complete' and all([ec == 0 for ec in self.exit_codes])

    async def mark_unscheduled(self):
        if self._pod_name:
            await db.jobs.update_record(self.id, pod_name=None)
            self._pod_name = None
        await self._create_pod()

    async def mark_complete(self, pod, failed=False):
        task_name = self._current_task.name

        if failed:
            exit_code = 999  # FIXME hack
            pod_log = None
        else:
            terminated = pod.status.container_statuses[0].state.terminated
            exit_code = terminated.exit_code
            if terminated.finished_at is not None and terminated.started_at is not None:
                duration = (terminated.finished_at - terminated.started_at).total_seconds()
                if self.duration is not None:
                    self.duration += duration
            else:
                log.warning(f'job {self.id} has pod {pod.metadata.name} which is '
                            f'terminated but has no timing information. {pod}')
                self.duration = None
            try:
                pod_log = v1.read_namespaced_pod_log(
                    pod.metadata.name,
                    HAIL_POD_NAMESPACE,
                    _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
            except kube.client.rest.ApiException as exc:
                log.exception(f'could not get logs for {pod.metadata.name} due to {exc}')
                if exc.status == 400:
                    await self.mark_unscheduled()
                    return
                raise

        await self._mark_job_task_complete(task_name, pod_log, exit_code)

        if exit_code == 0:
            if self._has_next_task():
                await self._create_pod()
                return
            await self._delete_pvc()
        else:
            await self._delete_pvc()

        await self.set_state('Complete')

        log.info('job {} complete, exit_codes {}'.format(self.id, self.exit_codes))

        if self.callback:
            def handler(id, callback, json):
                try:
                    requests.post(callback, json=json, timeout=120)
                except requests.exceptions.RequestException as exc:
                    log.warning(
                        f'callback for job {id} failed due to an error, I will not retry. '
                        f'Error: {exc}')

            threading.Thread(target=handler, args=(self.id, self.callback, self.to_dict())).start()

        if self.batch_id:
            batch = await Batch.from_db(self.batch_id, self.user)
            await batch.mark_job_complete(self)

    def to_dict(self):
        result = {
            'id': self.id,
            'state': self._state
        }
        if self._state == 'Complete':
            result['exit_code'] = {t.name: ec for ec, t in zip(self.exit_codes, self._tasks)}
            result['duration'] = self.duration

        if self.attributes:
            result['attributes'] = self.attributes
        return result


@routes.post('/jobs/create')
@authenticated_users_only
async def create_job(request, userdata):  # pylint: disable=R0912
    parameters = await request.json()
    user = userdata['username']

    schema = {
        # will be validated when creating pod
        'spec': schemas.pod_spec,
        'batch_id': {'required': True, 'type': 'integer'},
        'parent_ids': {'type': 'list', 'schema': {'type': 'integer'}},
        'input_files': {
            'type': 'list',
            'schema': {'type': 'list', 'items': 2 * ({'type': 'string'},)}},
        'output_files': {
            'type': 'list',
            'schema': {'type': 'list', 'items': 2 * ({'type': 'string'},)}},
        'always_run': {'type': 'boolean'},
        'attributes': {
            'type': 'dict',
            'keyschema': {'type': 'string'},
            'valueschema': {'type': 'string'}
        },
        'callback': {'type': 'string'}
    }
    validator = cerberus.Validator(schema)
    if not validator.validate(parameters):
        abort(400, 'invalid request: {}'.format(validator.errors))

    pod_spec = v1.api_client._ApiClient__deserialize(
        parameters['spec'], kube.client.V1PodSpec)

    batch_id = parameters.get('batch_id')
    batch = await Batch.from_db(batch_id, user)
    if batch is None:
        abort(404, f'invalid request: batch_id {batch_id} not found')
    if not batch.is_open:
        abort(400, f'invalid request: batch_id {batch_id} is closed')

    parent_ids = parameters.get('parent_ids', [])
    parents = {job.id: job for job in await Job.from_db_multiple(parent_ids, user)}
    for parent_id in parent_ids:
        parent_job = parents.get(parent_id)
        if parent_job is None:
            abort(400, f'invalid parent_id: no job with id {parent_id}')
        if parent_job.batch_id != batch_id or parent_job.batch_id is None or batch_id is None:
            abort(400,
                  f'invalid parent batch: {parent_id} is in batch '
                  f'{parent_job.batch_id} but child is in {batch_id}')

    input_files = parameters.get('input_files')
    output_files = parameters.get('output_files')
    always_run = parameters.get('always_run', False)

    if len(pod_spec.containers) != 1:
        abort(400, f'only one container allowed in pod_spec {pod_spec}')

    if pod_spec.containers[0].name != 'main':
        abort(400, f'container name must be "main" was {pod_spec.containers[0].name}')

    if not pod_spec.containers[0].resources:
        pod_spec.containers[0].resources = kube.client.V1ResourceRequirements()
    if not pod_spec.containers[0].resources.requests:
        pod_spec.containers[0].resources.requests = {}
    if 'cpu' not in pod_spec.containers[0].resources.requests:
        pod_spec.containers[0].resources.requests['cpu'] = '100m'
    if 'memory' not in pod_spec.containers[0].resources.requests:
        pod_spec.containers[0].resources.requests['memory'] = '500M'

    job = await Job.create_job(
        pod_spec=pod_spec,
        batch_id=batch_id,
        attributes=parameters.get('attributes'),
        callback=parameters.get('callback'),
        parent_ids=parent_ids,
        input_files=input_files,
        output_files=output_files,
        userdata=userdata,
        always_run=always_run)
    return jsonify(job.to_dict())


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return jsonify({})


@routes.get('/jobs/{job_id}')
@authenticated_users_only
async def get_job(request, userdata):
    job_id = int(request.match_info['job_id'])
    user = userdata['username']

    job = await Job.from_db(job_id, user)
    if not job:
        abort(404)
    return jsonify(job.to_dict())


@routes.get('/jobs/{job_id}/log')
@authenticated_users_only
async def get_job_log(request, userdata):  # pylint: disable=R1710
    job_id = int(request.match_info['job_id'])
    user = userdata['username']

    job = await Job.from_db(job_id, user)
    if not job:
        abort(404)

    job_log = await job._read_logs()
    if job_log:
        return jsonify(job_log)
    abort(404)


class Batch:
    MAX_TTL = 30 * 60

    @staticmethod
    def from_record(record, deleted=False):
        if record is not None:
            if not deleted:
                assert not record['deleted']
            attributes = json.loads(record['attributes'])
            userdata = json.loads(record['userdata'])

            if record['n_failed'] > 0:
                state = 'failure'
            elif record['n_cancelled'] > 0:
                state = 'cancelled'
            elif not record['is_open'] and (record['n_succeeded'] == record['n_jobs']):
                state = 'success'
            else:
                state = 'running'

            complete = not record['is_open'] and (record['n_completed'] == record['n_jobs'])

            return Batch(id=record['id'],
                         attributes=attributes,
                         callback=record['callback'],
                         ttl=record['ttl'],
                         is_open=record['is_open'],
                         userdata=userdata,
                         user=record['user'],
                         state=state,
                         complete=complete,
                         deleted=record['deleted'])
        return None

    @staticmethod
    async def from_db(ids, user):
        batches = await Batch.from_db_multiple(ids, user)
        if len(batches) == 1:
            return batches[0]
        return None

    @staticmethod
    async def from_db_multiple(ids, user):
        records = await db.batch.get_undeleted_records(ids, user)
        batches = [Batch.from_record(record) for record in records]
        return batches

    @staticmethod
    async def create_batch(attributes, callback, ttl, userdata):
        is_open = True
        user = userdata['username']

        if ttl is None or ttl > Batch.MAX_TTL:
            ttl = Batch.MAX_TTL

        id = await db.batch.new_record(attributes=json.dumps(attributes),
                                       callback=callback,
                                       ttl=ttl,
                                       is_open=is_open,
                                       userdata=json.dumps(userdata),
                                       user=user,
                                       deleted=False)

        batch = Batch(id=id, attributes=attributes, callback=callback,
                      ttl=ttl, is_open=is_open, userdata=userdata, user=user,
                      state='running', complete=False, deleted=False)
        batch.close_ttl()
        return batch

    def __init__(self, id, attributes, callback, ttl, is_open, userdata, user,
                 state, complete, deleted):
        self.id = id
        self.attributes = attributes
        self.callback = callback
        self.ttl = ttl
        self.is_open = is_open
        self.userdata = userdata
        self.user = user
        self.state = state
        self.complete = complete
        self.deleted = deleted

    async def get_jobs(self):
        return [Job.from_record(record) for record in await db.jobs.get_records_by_batch(self.id)]

    async def cancel(self):
        await self.close()
        jobs = await self.get_jobs()
        for j in jobs:
            await j.cancel()
        log.info(f'batch {self.id} cancelled')

    async def mark_deleted(self):
        await self.cancel()
        await db.batch.update_record(self.id,
                                     deleted=True)
        self.deleted = True
        log.info(f'batch {self.id} marked for deletion')

    async def delete(self):
        for j in await self.get_jobs():
            # Job deleted from database when batch is deleted with delete cascade
            await j._delete_logs()
        await db.batch.delete_record(self.id)
        log.info(f'batch {self.id} deleted')

    async def mark_job_complete(self, job):
        if self.callback:
            def handler(id, job_id, callback, json):
                try:
                    requests.post(callback, json=json, timeout=120)
                except requests.exceptions.RequestException as exc:
                    log.warning(
                        f'callback for batch {id}, job {job_id} failed due to an error, I will not retry. '
                        f'Error: {exc}')

            threading.Thread(
                target=handler,
                args=(self.id, job.id, self.callback, job.to_dict())
            ).start()

    def close_ttl(self):
        async def _close():
            await asyncio.sleep(self.ttl)
            await self.close()
        return asyncio.ensure_future(_close())

    async def close(self):
        if self.is_open:
            log.info(f'closing batch {self.id}, ttl was {self.ttl}')
            await db.batch.update_record(self.id, is_open=False)
            self.is_open = False
        else:
            log.info(f're-closing batch {self.id}, ttl was {self.ttl}')

    def is_complete(self):
        return self.complete

    def is_successful(self):
        return self.state == 'success'

    async def to_dict(self, include_jobs=False):
        result = {
            'id': self.id,
            'is_open': self.is_open,
            'state': self.state,
            'complete': self.complete
        }
        if self.attributes:
            result['attributes'] = self.attributes
        if include_jobs:
            jobs = await self.get_jobs()
            result['jobs'] = sorted([j.to_dict() for j in jobs], key=lambda j: j['id'])
        return result


@routes.get('/batches')
@authenticated_users_only
async def get_batches_list(request, userdata):
    params = request.query
    user = userdata['username']

    batches = [Batch.from_record(record)
               for record in await db.batch.get_records_where({'user': user, 'deleted': False})]

    for name, value in params.items():
        if name == 'complete':
            if value not in ('0', '1'):
                abort(400, f'invalid complete value, expected 0 or 1, got {value}')
            c = value == '1'
            batches = [batch for batch in batches if batch.is_complete() == c]
        elif name == 'success':
            if value not in ('0', '1'):
                abort(400, f'invalid success value, expected 0 or 1, got {value}')
            s = value == '1'
            batches = [batch for batch in batches if batch.is_successful() == s]
        else:
            if not name.startswith('a:'):
                abort(400, f'unknown query parameter {name}')
            k = name[2:]
            batches = [batch for batch in batches
                       if batch.attributes and k in batch.attributes and batch.attributes[k] == value]

    return jsonify([await batch.to_dict(include_jobs=False) for batch in batches])


@routes.post('/batches/create')
@authenticated_users_only
async def create_batch(request, userdata):
    parameters = await request.json()

    schema = {
        'attributes': {
            'type': 'dict',
            'keyschema': {'type': 'string'},
            'valueschema': {'type': 'string'}
        },
        'callback': {'type': 'string'},
        'ttl': {'type': 'number'}
    }
    validator = cerberus.Validator(schema)
    if not validator.validate(parameters):
        abort(400, 'invalid request: {}'.format(validator.errors))

    batch = await Batch.create_batch(attributes=parameters.get('attributes'),
                                     callback=parameters.get('callback'),
                                     ttl=parameters.get('ttl'),
                                     userdata=userdata)
    return jsonify(await batch.to_dict(include_jobs=False))


@routes.get('/batches/{batch_id}')
@authenticated_users_only
async def get_batch(request, userdata):
    batch_id = int(request.match_info['batch_id'])
    user = userdata['username']

    batch = await Batch.from_db(batch_id, user)
    if not batch:
        abort(404)
    return jsonify(await batch.to_dict(include_jobs=True))


@routes.patch('/batches/{batch_id}/cancel')
@authenticated_users_only
async def cancel_batch(request, userdata):
    batch_id = int(request.match_info['batch_id'])
    user = userdata['username']

    batch = await Batch.from_db(batch_id, user)
    if not batch:
        abort(404)
    await batch.cancel()
    return jsonify({})


@routes.delete('/batches/{batch_id}')
@authenticated_users_only
async def delete_batch(request, userdata):
    batch_id = int(request.match_info['batch_id'])
    user = userdata['username']

    batch = await Batch.from_db(batch_id, user)
    if not batch:
        abort(404)
    await batch.mark_deleted()
    return jsonify({})


@routes.patch('/batches/{batch_id}/close')
@authenticated_users_only
async def close_batch(request, userdata):
    batch_id = int(request.match_info['batch_id'])
    user = userdata['username']

    batch = await Batch.from_db(batch_id, user)
    if not batch:
        abort(404)
    await batch.close()
    return jsonify({})


async def update_job_with_pod(job, pod):
    log.info(f'update job {job.id} with pod {pod.metadata.name if pod else "None"}')
    if not pod or (pod.status and pod.status.reason == 'Evicted'):
        log.info(f'job {job.id} mark unscheduled')
        await job.mark_unscheduled()
    elif (pod
          and pod.status
          and pod.status.container_statuses):
        assert len(pod.status.container_statuses) == 1
        container_status = pod.status.container_statuses[0]
        assert container_status.name in ['input', 'main', 'output']

        if container_status.state:
            if container_status.state.terminated:
                log.info(f'job {job.id} mark complete')
                await job.mark_complete(pod)
            elif (container_status.state.waiting
                  and container_status.state.waiting.reason == 'ImagePullBackOff'):
                log.info(f'job {job.id} mark failed: ImagePullBackOff')
                await job.mark_complete(pod, failed=True)


class DeblockedIterator:
    def __init__(self, it):
        self.it = it

    def __aiter__(self):
        return self

    def __anext__(self):
        return blocking_to_async(app['blocking_pool'], self.it.__next__)


async def pod_changed(pod):
    job = Job.from_record(await db.jobs.get_record_by_pod(pod.metadata.name))

    if job and not job.is_complete():
        await update_job_with_pod(job, pod)


async def kube_event_loop():
    while True:
        try:
            stream = kube.watch.Watch().stream(
                v1.list_namespaced_pod,
                HAIL_POD_NAMESPACE,
                label_selector=f'app=batch-job,hail.is/batch-instance={INSTANCE_ID}')
            async for event in DeblockedIterator(stream):
                await pod_changed(event['object'])
        except Exception as exc:  # pylint: disable=W0703
            log.exception(f'k8s event stream failed due to: {exc}')
        await asyncio.sleep(5)


async def refresh_k8s_pods():
    # if we do this after we get pods, we will pick up jobs created
    # while listing pods and unnecessarily restart them
    pod_jobs = [Job.from_record(record) for record in await db.jobs.get_records_where({'pod_name': 'NOT NULL'})]

    pods = await blocking_to_async(
        app['blocking_pool'],
        v1.list_namespaced_pod,
        HAIL_POD_NAMESPACE,
        label_selector=f'app=batch-job,hail.is/batch-instance={INSTANCE_ID}',
        _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)

    log.info(f'k8s had {len(pods.items)} pods')

    seen_pods = set()
    for pod in pods.items:
        pod_name = pod.metadata.name
        seen_pods.add(pod_name)

        job = Job.from_record(await db.jobs.get_record_by_pod(pod_name))
        if job and not job.is_complete():
            await update_job_with_pod(job, pod)

    log.info('starting pods not seen in k8s')

    for job in pod_jobs:
        pod_name = job._pod_name
        if pod_name not in seen_pods:
            log.info(f'restarting job {job.id}')
            await update_job_with_pod(job, None)


async def refresh_k8s_pvc():
    pvcs = await blocking_to_async(
        app['blocking_pool'],
        v1.list_namespaced_persistent_volume_claim,
        HAIL_POD_NAMESPACE,
        label_selector=f'app=batch-job,hail.is/batch-instance={INSTANCE_ID}',
        _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)

    log.info(f'k8s had {len(pvcs.items)} pvcs')

    seen_pvcs = set()
    for record in await db.jobs.get_records_where({'pvc_name': 'NOT NULL'}):
        job = Job.from_record(record)
        assert job._pvc_name
        seen_pvcs.add(job._pvc_name)

    for pvc in pvcs.items:
        if pvc.metadata.name not in seen_pvcs:
            log.info(f'deleting orphaned pvc {pvc.metadata.name}')
            try:
                v1.delete_namespaced_persistent_volume_claim(
                    pvc.metadata.name,
                    HAIL_POD_NAMESPACE,
                    _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
            except kube.client.rest.ApiException as e:
                if e.status == 404:
                    return
                log.exception(f'Delete pvc {pvc.metadata.name} failed due to exception: {e}')
            except concurrent.futures.CancelledError:
                raise
            except Exception as e:  # pylint: disable=broad-except
                log.exception(f'Delete pvc {pvc.metadata.name} failed due to exception: {e}')


async def create_pods_if_ready():
    await asyncio.sleep(30)
    while True:
        for record in await db.jobs.get_records_where({'state': 'Ready',
                                                       'pod_name': None}):
            job = Job.from_record(record)
            try:
                await job._create_pod()
            except Exception as exc:  # pylint: disable=W0703
                log.exception(f'Could not create pod for job {job.id} due to exception: {exc}')
        await asyncio.sleep(30)


async def refresh_k8s_state():  # pylint: disable=W0613
    log.info('started k8s state refresh')
    await refresh_k8s_pods()
    await refresh_k8s_pvc()
    log.info('k8s state refresh complete')


async def polling_event_loop():
    await asyncio.sleep(1)
    while True:
        try:
            await refresh_k8s_state()
        except Exception as exc:  # pylint: disable=W0703
            log.exception(f'Could not poll due to exception: {exc}')
        await asyncio.sleep(REFRESH_INTERVAL_IN_SECONDS)


async def db_cleanup_event_loop():
    await asyncio.sleep(60)
    while True:
        try:
            for record in await db.batch.get_finished_deleted_records():
                batch = Batch.from_record(record, deleted=True)
                await batch.delete()
        except Exception as exc:  # pylint: disable=W0703
            log.exception(f'Could not delete batches due to exception: {exc}')
        await asyncio.sleep(60)


def serve(port=5000):
    app.add_routes(routes)
    with concurrent.futures.ThreadPoolExecutor() as pool:
        app['blocking_pool'] = pool
        asyncio.ensure_future(polling_event_loop())
        asyncio.ensure_future(kube_event_loop())
        asyncio.ensure_future(db_cleanup_event_loop())
        asyncio.ensure_future(create_pods_if_ready())
        web.run_app(app, host='0.0.0.0', port=port)
