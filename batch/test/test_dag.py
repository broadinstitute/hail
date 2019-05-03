import os
import time
import pytest
import re
import requests
from flask import Response

import hailjwt as hj

from batch.client import BatchClient, Job

from .serverthread import ServerThread


@pytest.fixture
def client():
    return BatchClient(url=os.environ.get('BATCH_URL'))


def test_user():
    fname = os.environ.get("HAIL_TOKEN_FILE")
    with open(fname) as f:
        return hj.JWTClient.unsafe_decode(f.read())


def batch_status_job_counter(batch_status, job_state):
    return len([j for j in batch_status['jobs'] if j['state'] == job_state])


def batch_status_exit_codes(batch_status):
    return [j['exit_code'] for j in batch_status['jobs']]


def test_simple(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[head])
    batch.run()

    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 2
    assert batch_status_exit_codes(status) == [{'main': 0}, {'main': 0}]


def test_missing_parent(client):
    try:
        batch = client.create_batch()
        fake_job = Job(client, batch, 100000)
        batch.create_job('alpine:3.8', command=['echo', 'head'], parent_ids=[fake_job])
        batch.run()
    except ValueError as err:
        assert re.search('Found the following invalid parent ids:', str(err))
        return
    assert False


def test_dag(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    left = batch.create_job('alpine:3.8', command=['echo', 'left'], parent_ids=[head])
    right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head])
    tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[left, right])
    batch.run()

    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 4
    for node in [head, left, right, tail]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code']['main'] == 0


def test_cancel_tail(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    left = batch.create_job('alpine:3.8', command=['echo', 'left'], parent_ids=[head])
    right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head])
    tail = batch.create_job(
        'alpine:3.8',
        command=['/bin/sh', '-c', 'while true; do sleep 86000; done'],
        parent_ids=[left.id, right.id])
    batch.run()

    left.wait()
    right.wait()
    batch.cancel()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 3
    for node in [head, left, right]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code']['main'] == 0
    assert tail.status()['state'] == 'Cancelled'


def test_cancel_left_after_tail(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    left = batch.create_job(
        'alpine:3.8',
        command=['/bin/sh', '-c', 'while true; do sleep 86000; done'],
        parent_ids=[head.id])
    right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head])
    tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[left, right])
    batch.run()

    right.wait()
    batch.cancel()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 2
    for node in [head, right]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code']['main'] == 0
    for node in [left, tail]:
        assert node.status()['state'] == 'Cancelled'


def test_callback(client):
    from flask import Flask, request
    app = Flask('test-client')
    output = []

    @app.route('/test', methods=['POST'])
    def test():
        d = request.get_json()
        output.append(d)
        return Response(status=200)

    try:
        server = ServerThread(app)
        server.start()
        batch = client.create_batch(callback=server.url_for('/test'))
        head = batch.create_job('alpine:3.8', command=['echo', 'head'])
        left = batch.create_job('alpine:3.8', command=['echo', 'left'], parent_ids=[head])
        right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head])
        tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[left, right])
        batch.run()

        batch.wait()
        i = 0
        while len(output) != 4:
            time.sleep(0.100 * (3/2) ** i)
            i += 1
            if i > 14:
                break
        assert len(output) == 4
        assert all([job_result['state'] == 'Complete' and job_result['exit_code']['main'] == 0
                    for job_result in output])
        assert output[0]['job_id'] == head.id
        middle_ids = (output[1]['job_id'], output[2]['job_id'])
        assert middle_ids in ((left.id, right.id), (right.id, left.id))
        assert output[3]['job_id'] == tail.id
    finally:
        if server:
            server.shutdown()
            server.join()


def test_no_parents_allowed_in_other_batches(client):
    b1 = client.create_batch()
    b2 = client.create_batch()
    head = b1.create_job('alpine:3.8', command=['echo', 'head'])
    b1.run()
    try:
        b2.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[head])
        b2.run()
    except ValueError as err:
        assert re.search("Found the following invalid parent ids", str(err))
        return
    assert False


def test_input_dependency(client):
    user = test_user()
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8',
                            command=['/bin/sh', '-c', 'echo head1 > /io/data1 ; echo head2 > /io/data2'],
                            output_files=[('/io/data*', f'gs://{user["bucket_name"]}')])
    tail = batch.create_job('alpine:3.8',
                            command=['/bin/sh', '-c', 'cat /io/data1 ; cat /io/data2'],
                            input_files=[(f'gs://{user["bucket_name"]}/data*', '/io/')],
                            parent_ids=[head.id])
    batch.run()
    tail.wait()
    assert head.status()['exit_code']['main'] == 0, head.cached_status()
    assert tail.log()['main'] == 'head1\nhead2\n'


def test_input_dependency_directory(client):
    user = test_user()
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8',
                            command=['/bin/sh', '-c', 'mkdir -p /io/test/; echo head1 > /io/test/data1 ; echo head2 > /io/test/data2'],
                            output_files=[('/io/test/', f'gs://{user["bucket_name"]}')])
    tail = batch.create_job('alpine:3.8',
                            command=['/bin/sh', '-c', 'cat /io/test/data1 ; cat /io/test/data2'],
                            input_files=[(f'gs://{user["bucket_name"]}/test', '/io/')],
                            parent_ids=[head])
    batch.run()
    tail.wait()
    assert head.status()['exit_code']['main'] == 0, head.cached_status()
    assert tail.log()['main'] == 'head1\nhead2\n', tail.log()


def test_always_run_cancel(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    left = batch.create_job(
        'alpine:3.8',
        command=['/bin/sh', '-c', 'while true; do sleep 86000; done'],
        parent_ids=[head])
    right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head])
    tail = batch.create_job('alpine:3.8',
                            command=['echo', 'tail'],
                            parent_ids=[left, right],
                            always_run=True)
    batch.run()
    right.wait()
    batch.cancel()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 3
    for node in [head, right, tail]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code']['main'] == 0


def test_always_run_error(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['/bin/sh', '-c', 'exit 1'])
    tail = batch.create_job('alpine:3.8',
                            command=['echo', 'tail'],
                            parent_ids=[head],
                            always_run=True)
    batch.run()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 2

    for job, ec in [(head, 1), (tail, 0)]:
        status = job.status()
        assert status['state'] == 'Complete'
        assert status['exit_code']['main'] == ec
