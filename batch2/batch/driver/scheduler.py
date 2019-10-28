import asyncio
import logging
import aiohttp
from hailtop.utils import request_retry_transient_errors

from ..batch import mark_job_complete
from ..database import check_call_procedure

log = logging.getLogger('driver')


class Scheduler:
    def __init__(self, scheduler_state_changed, cancel_state_changed, db, inst_pool):
        self.scheduler_state_changed = scheduler_state_changed
        self.cancel_state_changed = cancel_state_changed
        self.db = db
        self.inst_pool = inst_pool

    async def async_init(self):
        asyncio.ensure_future(self.loop('schedule_loop', self.scheduler_state_changed, self.schedule_1))
        asyncio.ensure_future(self.loop('cancel_loop', self.cancel_state_changed, self.cancel_1))
        asyncio.ensure_future(self.bump_loop())

    async def bump_loop(self):
        while True:
            self.scheduler_state_changed.set()
            self.cancel_state_changed.set()
            await asyncio.sleep(60)

    async def loop(self, name, changed, body):
        changed.clear()
        while True:
            should_wait = False
            try:
                should_wait = await body()
            except Exception:
                # FIXME back off?
                log.exception(f'in {name}')
            if should_wait:
                await changed.wait()
                changed.clear()

    # FIXME move to InstancePool.unschedule_job?
    async def unschedule_job(self, record):
        batch_id = record['batch_id']
        job_id = record['job_id']
        id = (batch_id, job_id)

        instance_id = record['instance_id']
        assert instance_id is not None

        log.info(f'unscheduling job {id} on instance {instance_id}')

        instance = self.inst_pool.id_instance.get(instance_id)
        # FIXME what to do if instance missing?
        if not instance:
            log.warning(f'unschedule job {id}: unknown instance {instance_id}')
            return

        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
            url = (f'http://{instance.ip_address}:5000'
                   f'/api/v1alpha/batches/{batch_id}/jobs/{job_id}/delete')
            try:
                await request_retry_transient_errors(session, 'DELETE', url)
            except aiohttp.ClientResponseError as e:
                if e.status == 404:
                    pass
                else:
                    raise

        log.info(f'unschedule job {id}: called delete job')

        await check_call_procedure(
            self.db,
            'CALL unschedule_job(%s, %s, %s);',
            (batch_id, job_id, instance_id))

        log.info(f'unschedule job {id}: updated database')

        self.inst_pool.adjust_for_remove_instance(instance)
        instance.free_cores_mcpu += record['cores_mcpu']
        self.inst_pool.adjust_for_add_instance(instance)

        log.info(f'unschedule job {id}: updated instance pool')

        self.scheduler_state_changed.set()

    async def cancel_1(self):
        records = self.db.execute_and_fetchall(
            '''
SELECT job_id, batch_id, cores_mcpu, instance_id
FROM jobs
INNER JOIN batch ON batch.id = jobs.batch_id
WHERE jobs.state = 'Running' AND (NOT jobs.always_run) AND batch.closed AND batch.cancelled
LIMIT 50;
''')

        should_wait = True
        async for record in records:
            should_wait = False
            await self.unschedule_job(record)

        return should_wait

    async def schedule_1(self):
        records = self.db.execute_and_fetchall(
            '''
SELECT job_id, batch_id, directory, spec, cores_mcpu,
  always_run,
  (cancel OR batch.cancelled) as cancel,
  batch.user as user
FROM jobs
INNER JOIN batch ON batch.id = jobs.batch_id
WHERE jobs.state = 'Ready' AND batch.closed
LIMIT 50;
''')

        should_wait = True
        async for record in records:
            batch_id = record['batch_id']
            job_id = record['job_id']
            id = (batch_id, job_id)

            log.info(f'scheduling job {id}')

            if record['cancel'] and not record['always_run']:
                log.info(f'cancelling job {id}')
                await mark_job_complete(
                    self.db, self.scheduler_state_changed, self.inst_pool,
                    batch_id, job_id, 'Cancelled', None)
                should_wait = False
                continue

            i = self.inst_pool.active_instances_by_free_cores.bisect_key_left(record['cores_mcpu'])
            if i < len(self.inst_pool.active_instances_by_free_cores):
                instance = self.inst_pool.active_instances_by_free_cores[i]
                assert record['cores_mcpu'] <= instance.free_cores_mcpu
                log.info(f'scheduling job {id} on {instance}')
                await self.inst_pool.schedule_job(record, instance)
                should_wait = False

        return should_wait
