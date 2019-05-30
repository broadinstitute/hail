
import collections
import batch
import json
import os
import pkg_resources
import re
import secrets
import time
import unittest
import aiohttp
from flask import Flask, Response, request
import requests

import hailjwt as hj

from .serverthread import ServerThread


class Test(unittest.TestCase):
    def setUp(self):
        session = aiohttp.ClientSession(
            raise_for_status=True,
            timeout=aiohttp.ClientTimeout(total=60))
        self.client = batch.client.BatchClient(session, url=os.environ.get('BATCH_URL'))

    def tearDown(self):
        self.client.close()

    def test_job(self):
        b = self.client.create_batch()
        j = b.create_job('alpine', ['echo', 'test'])
        b.close()
        status = j.wait()
        self.assertTrue('attributes' not in status)
        self.assertEqual(status['state'], 'Complete')
        self.assertEqual(status['exit_code']['main'], 0)

        self.assertEqual(j.log(), {'main': 'test\n'})

        self.assertTrue(j.is_complete())

    def test_create_fails_for_closed_batch(self):
        b = self.client.create_batch()
        b.close()
        try:
            b.create_job('alpine', ['echo', 'test'])
        except aiohttp.ClientResponseError as err:
            assert err.status == 400
            assert re.search('.*invalid request: batch_id [0-9]+ is closed', err.message)
            return
        assert False

    def test_batch_ttl(self):
        b = self.client.create_batch(ttl=1)
        t = 1
        while b.status()['is_open']:
            if t > 64:
                assert False, "took more than 128 seconds to close a batch with ttl 1"
            time.sleep(t)
            t = t * 2

    def test_attributes(self):
        a = {
            'name': 'test_attributes',
            'foo': 'bar'
        }
        b = self.client.create_batch()
        j = b.create_job('alpine', ['true'], attributes=a)
        status = j.status()
        assert(status['attributes'] == a)

    def test_list_batches(self):
        tag = secrets.token_urlsafe(64)
        b1 = self.client.create_batch(attributes={'tag': tag, 'name': 'b1'})
        b1.create_job('alpine', ['sleep', '30'])
        b1.close()

        b2 = self.client.create_batch(attributes={'tag': tag, 'name': 'b2'})
        b2.create_job('alpine', ['echo', 'test'])
        b2.close()

        def assert_batch_ids(expected, complete=None, success=None, attributes=None):
            batches = self.client.list_batches(complete=complete, success=success, attributes=attributes)
            # list_batches returns all batches for all prev run tests
            actual = set([batch.id for batch in batches]).intersection({b1.id, b2.id})
            self.assertEqual(actual, expected)

        assert_batch_ids({b1.id, b2.id}, attributes={'tag': tag})

        b2.wait()

        assert_batch_ids({b1.id}, complete=False, attributes={'tag': tag})
        assert_batch_ids({b2.id}, complete=True, attributes={'tag': tag})

        assert_batch_ids({b1.id}, success=False, attributes={'tag': tag})
        assert_batch_ids({b2.id}, success=True, attributes={'tag': tag})

        b1.cancel()
        b1.wait()

        assert_batch_ids({b1.id}, success=False, attributes={'tag': tag})
        assert_batch_ids({b2.id}, success=True, attributes={'tag': tag})

        assert_batch_ids(set(), complete=False, attributes={'tag': tag})
        assert_batch_ids({b1.id, b2.id}, complete=True, attributes={'tag': tag})

        assert_batch_ids({b2.id}, attributes={'tag': tag, 'name': 'b2'})

    def test_fail(self):
        b = self.client.create_batch()
        j = b.create_job('alpine', ['false'])
        b.close()
        status = j.wait()
        self.assertEqual(status['exit_code']['main'], 1)

    def test_deleted_job_log(self):
        b = self.client.create_batch()
        j = b.create_job('alpine', ['echo', 'test'])
        b.close()
        j.wait()
        b.delete()

        try:
            j.log()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                pass
            else:
                self.assertTrue(False, f"batch should have deleted log {e}")

    def test_delete_batch(self):
        b = self.client.create_batch()
        j = b.create_job('alpine', ['sleep', '30'])
        b.close()
        id = j.id
        print(b.id)
        b.delete()

        # verify doesn't exist
        try:
            self.client.get_job(id)
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                pass
            else:
                raise

    def test_cancel_batch(self):
        b = self.client.create_batch()
        j = b.create_job('alpine', ['sleep', '30'])
        b.close()

        status = j.status()
        self.assertTrue(status['state'], 'Ready')

        b.cancel()

        status = j.status()
        self.assertTrue(status['state'], 'Cancelled')
        self.assertTrue('log' not in status)

        # cancelled job has no log
        try:
            j.log()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                pass
            else:
                raise

    def test_get_nonexistent_job(self):
        try:
            self.client.get_job(666)
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                pass
            else:
                raise

    def test_get_job(self):
        b = self.client.create_batch()
        j = b.create_job('alpine', ['true'])
        b.close()

        j2 = self.client.get_job(j.id)
        status2 = j2.status()
        assert(status2['id'] == j.id)

    def test_batch(self):
        b = self.client.create_batch()
        j1 = b.create_job('alpine', ['false'])
        j2 = b.create_job('alpine', ['sleep', '1'])
        j3 = b.create_job('alpine', ['sleep', '30'])
        b.close()

        j1.wait()
        j2.wait()
        b.cancel()
        bstatus = b.wait()

        assert(len(bstatus['jobs']) == 3)
        state_count = collections.Counter([j['state'] for j in bstatus['jobs']])
        n_cancelled = state_count['Cancelled']
        n_complete = state_count['Complete']
        self.assertTrue(n_cancelled <= 1)
        self.assertTrue(n_cancelled + n_complete == 3)

        n_failed = sum([j['exit_code']['main'] > 0 for j in bstatus['jobs'] if j['state'] == 'Complete'])
        self.assertTrue(n_failed == 1)

    def test_batch_status(self):
        b1 = self.client.create_batch()
        b1.create_job('alpine', ['true'])
        b1.close()
        b1.wait()
        b1s = b1.status()
        assert b1s['complete'] and b1s['state'] == 'success', b1s

        b2 = self.client.create_batch()
        b2.create_job('alpine', ['false'])
        b2.create_job('alpine', ['true'])
        b2.close()
        b2.wait()
        b2s = b2.status()
        assert b2s['complete'] and b2s['state'] == 'failure', b2s

        b3 = self.client.create_batch()
        b3.create_job('alpine', ['sleep', '30'])
        b3.close()
        b3s = b3.status()
        assert not b3s['complete'] and b3s['state'] == 'running', b3s

        b4 = self.client.create_batch()
        b4.create_job('alpine', ['sleep', '30'])
        b4.close()
        b4.cancel()
        b4.wait()
        b4s = b4.status()
        assert b4s['complete'] and b4s['state'] == 'cancelled', b4s

    def test_callback(self):
        app = Flask('test-client')

        d = {}

        @app.route('/test', methods=['POST'])
        def test():
            d['status'] = request.get_json()
            return Response(status=200)

        server = ServerThread(app)
        try:
            server.start()
            b = self.client.create_batch()
            j = b.create_job(
                'alpine',
                ['echo', 'test'],
                attributes={'foo': 'bar'},
                callback=server.url_for('/test'))
            b.close()
            j.wait()

            batch.poll_until(lambda: 'status' in d)
            status = d['status']
            self.assertEqual(status['state'], 'Complete')
            self.assertEqual(status['attributes'], {'foo': 'bar'})
        finally:
            server.shutdown()
            server.join()

    def test_log_after_failing_job(self):
        b = self.client.create_batch()
        j = b.create_job('alpine', ['/bin/sh', '-c', 'echo test; exit 127'])
        b.close()
        status = j.wait()
        self.assertTrue('attributes' not in status)
        self.assertEqual(status['state'], 'Complete')
        self.assertEqual(status['exit_code']['main'], 127)

        self.assertEqual(j.log(), {'main': 'test\n'})

        self.assertTrue(j.is_complete())

    def test_authorized_users_only(self):
        endpoints = [
            (requests.post, '/jobs/create'),
            (requests.get, '/jobs/0'),
            (requests.get, '/jobs/0/log'),
            (requests.get, '/batches'),
            (requests.post, '/batches/create'),
            (requests.get, '/batches/0'),
            (requests.delete, '/batches/0'),
            (requests.patch, '/batches/0/close'),
            (requests.get, '/ui/batches'),
            (requests.get, '/ui/batches/0'),
            (requests.get, '/ui/jobs/0/log')]
        for f, url in endpoints:
            r = f(os.environ.get('BATCH_URL')+url)
            assert r.status_code == 401, r

    def test_bad_jwt_key(self):
        fname = pkg_resources.resource_filename(
            __name__,
            'jwt-test-user.json')
        with open(fname) as f:
            userdata = json.loads(f.read())
        token = hj.JWTClient(hj.JWTClient.generate_key()).encode(userdata)
        session = aiohttp.ClientSession(
            raise_for_status=True,
            timeout=aiohttp.ClientTimeout(total=60))
        bc = batch.client.BatchClient(session, url=os.environ.get('BATCH_URL'), token=token)
        try:
            b = bc.create_batch()
            j = b.create_job('alpine', ['false'])
            b.close()
            assert False, j
        except aiohttp.ClientResponseError as e:
            if e.status == 401:
                pass
            else:
                assert False, e
        finally:
            bc.close()

    def test_ui_batches(self):
        with open(os.environ['HAIL_TOKEN_FILE']) as f:
            token = f.read()
        # just check successful response
        r = requests.get(f'{os.environ.get("BATCH_URL")}/ui/batches',
                         cookies={'user': token})
        assert (r.status_code >= 200) and (r.status_code < 300)

    def test_ui_batch_and_job_log(self):
        b = self.batch.create_batch()
        j = b.create_job('alpine', ['true'])
        b.close()
        status = j.wait()

        with open(os.environ['HAIL_TOKEN_FILE']) as f:
            token = f.read()

        # just check successful response
        r = requests.get(f'{os.environ.get("BATCH_URL")}/ui/batches/{b.id}',
                         cookies={'user': token})
        assert (r.status_code >= 200) and (r.status_code < 300)

        # just check successful response
        r = requests.get(f'{os.environ.get("BATCH_URL")}/ui/jobs/{j.id}/log',
                         cookies={'user': token})
        assert (r.status_code >= 200) and (r.status_code < 300)
