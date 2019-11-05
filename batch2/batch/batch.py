import json
import logging
import asyncio
import aiohttp
from hailtop.utils import sleep_and_backoff, is_transient_error

from .globals import complete_states, tasks
from .database import check_call_procedure
from .batch_configuration import KUBERNETES_TIMEOUT_IN_SECONDS, BATCH_NAMESPACE

log = logging.getLogger('batch')


async def batch_record_to_dict(db, record, include_jobs=False):
    if record['n_failed'] > 0:
        state = 'failure'
    elif record['n_cancelled'] > 0:
        state = 'cancelled'
    elif record['closed'] and record['n_succeeded'] == record['n_jobs']:
        state = 'success'
    else:
        state = 'running'

    complete = record['closed'] and record['n_completed'] == record['n_jobs']

    d = {
        'id': record['id'],
        'state': state,
        'complete': complete,
        'closed': record['closed']
    }

    attributes = json.loads(record['attributes'])
    if attributes:
        d['attributes'] = attributes

    if include_jobs:
        jobs = [
            job_record_to_dict(record)
            async for record
            in db.execute_and_fetchall(
                'SELECT * FROM jobs where batch_id = %s',
                (record['id'],))
        ]
        d['jobs'] = sorted(jobs, key=lambda j: j['job_id'])

    return d


async def notify_batch_job_complete(db, batch_id):
    record = await db.execute_and_fetchone(
        '''
SELECT *
FROM batches
WHERE id = %s AND NOT deleted AND callback IS NOT NULL AND
   closed AND n_completed = n_jobs;
''',
        (batch_id,))

    if not record:
        return
    callback = record['callback']

    log.info(f'making callback for batch {batch_id}: {callback}')

    try:
        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
            await session.post(callback, json=await batch_record_to_dict(db, record))
            log.info(f'callback for batch {batch_id} successful')
    except Exception:
        log.exception(f'callback for batch {batch_id} failed, will not retry.')


async def mark_job_complete(app, batch_id, job_id, new_state, status):
    scheduler_state_changed = app['scheduler_state_changed']
    db = app['db']
    inst_pool = app['inst_pool']

    id = (batch_id, job_id)

    rv = await check_call_procedure(
        db,
        'CALL mark_job_complete(%s, %s, %s, %s);',
        (batch_id, job_id, new_state,
         json.dumps(status) if status is not None else None))

    log.info(f'mark_job_complete returned {rv} for job {id}')

    old_state = rv['old_state']
    if old_state in complete_states:
        # already complete, do nothing
        return

    log.info(f'job {id} changed state: {rv["old_state"]} => {new_state}')

    instance_name = rv['instance_name']
    if instance_name:
        instance = inst_pool.name_instance.get(instance_name)
        if instance:
            log.info(f'updating {instance}')

            instance.adjust_free_cores_in_memory(rv['cores_mcpu'])
            scheduler_state_changed.set()
        else:
            log.warning(f'mark_complete for job {id} from unknown {instance}')

    await notify_batch_job_complete(db, batch_id)


def job_record_to_dict(record):
    def getopt(obj, attr):
        if obj is None:
            return None
        return obj.get(attr)

    result = {
        'batch_id': record['batch_id'],
        'job_id': record['job_id'],
        'state': record['state']
    }
    # FIXME can't change this yet, batch and batch2 share client
    if record['status']:
        status = json.loads(record['status'])

        if 'error' in status:
            result['error'] = status['error']
        result['exit_code'] = {
            k: getopt(getopt(status['container_statuses'][k], 'container_status'), 'exit_code') for
            k in tasks
        }
        result['duration'] = {k: getopt(status['container_statuses'][k]['timing'], 'runtime') for k in tasks}
        result['message'] = {
            # the execution of the job container might have
            # failed, or the docker container might have completed
            # (wait returned) but had status error
            k: (getopt(status['container_statuses'][k], 'error') or
                getopt(getopt(status['container_statuses'][k], 'container_status'), 'error'))
            for k in tasks
        }

    spec = json.loads(record['spec'])
    attributes = spec.get('attributes')
    if attributes:
        result['attributes'] = attributes
    return result


async def unschedule_job(app, record):
    scheduler_state_changed = app['scheduler_state_changed']
    db = app['db']
    inst_pool = app['inst_pool']

    batch_id = record['batch_id']
    job_id = record['job_id']
    id = (batch_id, job_id)

    instance_name = record['instance_name']
    assert instance_name is not None

    log.info(f'unscheduling job {id} from instance {instance_name}')

    instance = inst_pool.name_instance.get(instance_name)
    if not instance:
        log.warning(f'unschedule job {id}: unknown instance {instance_name}')
        return

    await check_call_procedure(
        db,
        'CALL unschedule_job(%s, %s, %s);',
        (batch_id, job_id, instance_name))

    log.info(f'unschedule job {id}: updated database')

    instance.adjust_free_cores_in_memory(record['cores_mcpu'])
    scheduler_state_changed.set()

    log.info(f'unschedule job {id}: updated {instance} free cores')

    url = (f'http://{instance.ip_address}:5000'
           f'/api/v1alpha/batches/{batch_id}/jobs/{job_id}/delete')
    delay = 0.1
    while True:
        if instance.state in ('inactive', 'deleted'):
            break
        try:
            async with aiohttp.ClientSession(
                    raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
                await session.delete(url)
                await instance.mark_healthy()
                break
        except Exception as e:
            if (isinstance(e, aiohttp.ClientResponseError) and
                    e.status == 404):  # pylint: disable=no-member
                await instance.mark_healthy()
                break
            else:
                await instance.incr_failed_request_count()
                if is_transient_error(e):
                    pass
                else:
                    raise
        delay = await sleep_and_backoff(delay)

    log.info(f'unschedule job {id}: called delete job')


async def job_config(app, record):
    k8s_client = app['k8s_client']

    job_spec = json.loads(record['spec'])
    userdata = json.loads(record['userdata'])

    secrets = job_spec['secrets']
    k8s_secrets = await asyncio.gather(*[
        k8s_client.read_namespaced_secret(
            secret['name'], BATCH_NAMESPACE,
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
        for secret in secrets
    ])

    gsa_key = None
    for secret, k8s_secret in zip(secrets, k8s_secrets):
        if secret['name'] == userdata['gsa_key_secret_name']:
            gsa_key = k8s_secret.data
        secret['data'] = k8s_secret.data

    assert gsa_key

    return {
        'batch_id': record['batch_id'],
        'user': record['user'],
        'gsa_key': gsa_key,
        'job_spec': job_spec
    }


async def schedule_job(app, record, instance):
    assert instance.state == 'active'

    db = app['db']

    batch_id = record['batch_id']
    job_id = record['job_id']
    id = (batch_id, job_id)

    try:
        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
            url = (f'http://{instance.ip_address}:5000'
                   f'/api/v1alpha/batches/jobs/create')
            body = await job_config(app, record)
            await session.post(url, json=body)
            await instance.mark_healthy()
    except Exception:
        await instance.incr_failed_request_count()
        raise

    log.info(f'schedule job {id} on {instance}: called create job')

    await check_call_procedure(
        db,
        'CALL schedule_job(%s, %s, %s);',
        (batch_id, job_id, instance.name))

    log.info(f'schedule job {id} on {instance}: updated database')

    instance.adjust_free_cores_in_memory(-record['cores_mcpu'])

    log.info(f'schedule job {id} on {instance}: adjusted instance pool')
