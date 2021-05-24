import json
import logging

from hailtop.utils import time_msecs_str, humanize_timedelta_msecs, time_msecs
from gear import transaction

from .batch_format_version import BatchFormatVersion
from .exceptions import NonExistentBatchError

log = logging.getLogger('batch')


def batch_record_to_dict(record):
    format_version = BatchFormatVersion(record['format_version'])

    if record['state'] == 'open':  # backwards compatibility
        state = 'open'
    elif record['cancelled']:
        state = 'cancelled'
    elif record['n_failed'] > 0:
        state = 'failure'
    elif record['n_cancelled'] > 0:
        state = 'cancelled'
    elif record['state'] == 'complete':
        assert record['n_succeeded'] == record['n_jobs']
        state = 'success'
    elif record['state'] == 'created':
        state = 'created'
    else:
        state = 'running'

    def _time_msecs_str(t):
        if t:
            return time_msecs_str(t)
        return None

    time_created = _time_msecs_str(record['time_created'])
    time_closed = _time_msecs_str(record['time_closed'])
    time_completed = _time_msecs_str(record['time_completed'])

    if format_version.format_version < 7:
        closed = record['state'] != 'open'
        start_time = record['time_closed']
    else:
        closed = bool(record['closed'])
        start_time = record['time_created']

    if start_time and record['time_completed']:
        duration = humanize_timedelta_msecs(record['time_completed'] - start_time)
    else:
        duration = None

    d = {
        'id': record['id'],
        'user': record['user'],
        'billing_project': record['billing_project'],
        'token': record['token'],
        'state': state,
        'complete': record['state'] == 'complete',
        'closed': closed,
        'n_jobs': record['n_jobs'],
        'n_completed': record['n_completed'],
        'n_succeeded': record['n_succeeded'],
        'n_failed': record['n_failed'],
        'n_cancelled': record['n_cancelled'],
        'time_created': time_created,
        'time_closed': time_closed,
        'time_completed': time_completed,
        'duration': duration
    }

    attributes = json.loads(record['attributes'])
    if attributes:
        d['attributes'] = attributes

    msec_mcpu = record['msec_mcpu']
    d['msec_mcpu'] = msec_mcpu

    if closed and record['cost'] is None:
        record['cost'] = 0

    cost = format_version.cost(record['msec_mcpu'], record['cost'])
    d['cost'] = cost

    return d


def job_record_to_dict(record, name):
    format_version = BatchFormatVersion(record['format_version'])

    db_status = record['status']
    if db_status:
        db_status = json.loads(db_status)
        exit_code, duration = format_version.get_status_exit_code_duration(db_status)
    else:
        exit_code = None
        duration = None

    result = {
        'batch_id': record['batch_id'],
        'job_id': record['job_id'],
        'name': name,
        'user': record['user'],
        'billing_project': record['billing_project'],
        'state': record['state'],
        'exit_code': exit_code,
        'duration': duration,
    }

    msec_mcpu = record['msec_mcpu']
    result['msec_mcpu'] = msec_mcpu

    cost = format_version.cost(record['msec_mcpu'], record['cost'])
    result['cost'] = cost

    return result


async def cancel_batch_in_db(db, batch_id):
    @transaction(db)
    async def cancel(tx):
        record = await tx.execute_and_fetchone(
            '''
SELECT `state` FROM batches
WHERE id = %s AND NOT deleted
FOR UPDATE;
''',
            (batch_id,),
        )
        if not record:
            raise NonExistentBatchError(batch_id)

        now = time_msecs()

        await tx.just_execute('CALL cancel_batch(%s, %s);', (batch_id, now))

    await cancel()  # pylint: disable=no-value-for-parameter
