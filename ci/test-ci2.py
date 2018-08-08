import subprocess
from subprocess import call, run
import os
import time
import random
import requests
import tempfile
import unittest

CI_URL='http://localhost:5000'
GITHUB_URL = 'https://api.github.com/'

class NoOAuthToken(Exception):
    pass
class BadStatus(Exception):
    def __init__(self, data, status_code):
        Exception.__init__(self, str(data))
        self.data = data
        self.status_code = status_code

def read_oauth_token_or_fail(path):
    try:
        with open(path, 'r') as f:
            return f.read()
    except FileNotFoundError as e:
        raise NoOAuthToken(
            f"working directory must contain `{path}' "
            "containing a valid GitHub oauth token"
        ) from e

oauth_tokens = {}
oauth_tokens['user1'] = read_oauth_token_or_fail('github-tokens/user1')
oauth_tokens['user2'] = read_oauth_token_or_fail('github-tokens/user2')

def ci_post(endpoint, json=None, status_code=None, json_response=True):
    r = requests.post(CI_URL + endpoint, json = json)
    if status_code and r.status_code != status_code:
        raise ValueError(
            'bad status_code from pull_request: {status_code}\n{message}'.format(
                endpoint=endpoint, status_code=r.status_code, message=r.text))
    if json_response:
        return r.json()
    else:
        return r.text

def ci_get(endpoint, status_code=None, json_response=True):
    r = requests.get(CI_URL + endpoint)
    if status_code and r.status_code != status_code:
        raise ValueError(
            'bad status_code from {endpoint}: {status_code}\n{message}'.format(
                endpoint=endpoint, status_code=r.status_code, message=r.text))
    if json_response:
        return r.json()
    else:
        return r.text

def post_repo(repo,
              url,
              headers=None,
              json=None,
              data=None,
              status_code=None,
              user='user1',
              json_response=True):
    return modify_repo(
        'post',
        repo,
        url,
        headers,
        json,
        data,
        status_code,
        user=user,
        json_response=json_response)

def patch_repo(repo,
               url,
               headers=None,
               json=None,
               data=None,
               status_code=None,
               user='user1',
               json_response=True):
    return modify_repo(
        'patch',
        repo,
        url,
        headers,
        json,
        data,
        status_code,
        user=user,
        json_response=json_response)

def modify_repo(verb,
                repo,
                url,
                headers=None,
                json=None,
                data=None,
                status_code=None,
                user='user1',
                json_response=True):
    if headers is None:
        headers = {}
    if 'Authorization' in headers:
        raise ValueError(
            'Header already has Authorization? ' + str(headers))
    headers['Authorization'] = 'token ' + oauth_tokens[user]
    if verb == 'post':
        r = requests.post(
            f'{GITHUB_URL}repos/{repo}/{url}',
            headers=headers,
            json=json,
            data=data
        )
    else:
        assert verb == 'patch', verb
        r = requests.patch(
            f'{GITHUB_URL}repos/{repo}/{url}',
            headers=headers,
            json=json,
            data=data
        )
    if status_code and r.status_code != status_code:
        raise BadStatus({
            'method': verb,
            'endpoint' : f'{GITHUB_URL}repos/{repo}/{url}',
            'status_code' : r.status_code,
            'data': data,
            'json': json,
            'message': 'github error',
            'github_json': r.json()
        }, r.status_code)
    elif json_response:
        return r.json()
    else:
        return r.text

def get_repo(repo,
             url,
             headers=None,
             status_code=None,
             user='user1',
             json_response=True):
    return get_github(
        f'repos/{repo}/{url}',
        headers,
        status_code,
        user=user,
        json_response=json_response)

def get_github(url,
               headers=None,
               status_code=None,
               user='user1',
               json_response=True):
    if headers is None:
        headers = {}
    if 'Authorization' in headers:
        raise ValueError(
            'Header already has Authorization? ' + str(headers))
    headers['Authorization'] = 'token ' + oauth_tokens[user]
    r = requests.get(
        f'{GITHUB_URL}{url}',
        headers=headers
    )
    if status_code and r.status_code != status_code:
        raise BadStatus({
            'method': 'get',
            'endpoint' : f'{GITHUB_URL}{url}',
            'status_code' : r.status_code,
            'message': 'github error',
            'github_json': r.json()
        }, r.status_code)
    elif json_response:
        return r.json()
    else:
        return r.text

###############################################################################

class Match(object):
    def __init__(self, pred, msg):
        self.pred = pred
        self.msg = msg

    def matches(self, x):
        return self.pred(x)

    def __str__(self):
        return self.msg

    def any(*args):
        s = set(args)
        return Match(lambda x: x in s, f'any{args}')

def assertDictHasKVs(actual, kvs):
    d = dictKVMismatches(actual, kvs)
    assert len(d) == 0, d

def dictKVMismatches(actual, kvs):
    assert isinstance(actual, dict), actual
    errors = {}
    for k, v in kvs.items():
        if k not in actual:
            errors[k] = f'had nothing, should have had {v}'
        elif isinstance(v, dict):
            sub_errors = dictKVMismatches(actual[k], v)
            if len(sub_errors) != 0:
                errors[k] = sub_errors
        else:
            actual_v = actual[k]
            if isinstance(v, Match):
                if not v.matches(actual_v):
                    errors[k] = f'{actual_v} does not match {v}'
            elif actual_v != v:
                errors[k] = f'{actual_v} != {v}'
    return errors

###############################################################################

class TestCI(unittest.TestCase):

    def get_pr(self, source_ref):
        status = ci_get('/status', status_code=200)
        assert 'prs' in status
        assert 'watched_repos' in status
        prs = status['prs']
        prs = [pr for pr in prs if pr['source_ref'] == source_ref]
        assert len(prs) == 1
        return prs[0]

    DELAY_IN_SECONDS=20
    MAX_POLLS=15

    def poll_until_finished_pr(self, source_ref, delay_in_seconds=DELAY_IN_SECONDS, max_polls=MAX_POLLS):
        return self.poll_pr(
            source_ref,
            lambda pr: pr['status']['state'] == 'running' or pr['status']['state'] == 'pending',
            delay_in_seconds=delay_in_seconds,
            max_polls=max_polls
        )

    def poll_until_running_pr(self, source_ref, delay_in_seconds=DELAY_IN_SECONDS, max_polls=MAX_POLLS):
        return self.poll_pr(
            source_ref,
            lambda pr: pr['status']['state'] == 'pending',
            delay_in_seconds=delay_in_seconds,
            max_polls=max_polls
        )

    def poll_until_merged_pr(self, source_ref, delay_in_seconds=DELAY_IN_SECONDS, max_polls=MAX_POLLS):
        return self.poll_pr(
            source_ref,
            lambda pr: pr['status']['state'] != 'merged',
            delay_in_seconds=delay_in_seconds,
            max_polls=max_polls
        )

    def poll_pr(self, source_ref, poll_until_false, delay_in_seconds=DELAY_IN_SECONDS, max_polls=MAX_POLLS):
        pr = self.get_pr(source_ref)
        polls = 0
        while poll_until_false(pr):
            assert polls < max_polls
            time.sleep(delay_in_seconds)
            pr = self.get_pr(source_ref)
            polls = polls + 1
        return pr

    def poll_until_pr_exists(self, source_ref, delay_in_seconds=DELAY_IN_SECONDS, max_polls=MAX_POLLS):
        return self.poll_until_pr_exists(source_ref, lambda x: True, delay_in_seconds, max_polls)

    def poll_until_pr_exists_and(self, source_ref, poll_until_true, delay_in_seconds=DELAY_IN_SECONDS, max_polls=MAX_POLLS):
        prs = []
        while len(prs) == 0 or not poll_until_true(prs[0]):
            status = ci_get('/status', status_code=200)
            assert 'prs' in status
            assert 'watched_repos' in status
            prs = status['prs']
            prs = [pr for pr in prs if pr['source_ref'] == source_ref]
            assert len(prs) <= 1
        assert len(prs) == 1
        return prs[0]

    def test_pull_request_trigger(self):
        BRANCH_NAME='test_pull_request_trigger'
        call(['git', 'push', 'origin', ':'+BRANCH_NAME])
        with tempfile.TemporaryDirectory() as d:
            pr_number = None
            try:
                status = ci_get('/status', status_code=200)
                self.assertIn('watched_repos', status)
                self.assertEqual(status['watched_repos'], ['hail-is/ci-test'])
                os.chdir(d)
                call(['git', 'clone', 'git@github.com:hail-is/ci-test.git'])
                os.chdir('ci-test')
                call(['git', 'remote', '-v'])

                call(['git', 'checkout', '-b', BRANCH_NAME])
                call(['git', 'commit', '--allow-empty', '-m', 'foo'])
                call(['git', 'push', 'origin', BRANCH_NAME])
                source_sha = run(['git', 'rev-parse', BRANCH_NAME], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()
                target_sha = run(['git', 'rev-parse', 'master'], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()
                data = post_repo(
                    'hail-is/ci-test',
                    'pulls',
                    json={ "title" : "foo", "head": BRANCH_NAME, "base": "master" },
                    status_code=201
                )
                pr_number = data['number']
                time.sleep(7)
                pr = self.poll_until_finished_pr(BRANCH_NAME)
                assertDictHasKVs(pr, {
                    'source_url': 'https://github.com/hail-is/ci-test.git',
                    'target_url': 'https://github.com/hail-is/ci-test.git',
                    'target_ref': 'master',
                    'status': {
                        'state': 'success',
                        'review_state': 'pending',
                        'source_sha': source_sha,
                        'target_sha': target_sha,
                        'pr_number': str(pr_number),
                        'docker_image': 'google/cloud-sdk:alpine'
                    }
                })
                assert pr['status']['job_id'] is not None
            finally:
                call(['git', 'push', 'origin', ':' + BRANCH_NAME])
                if pr_number is not None:
                    patch_repo(
                        'hail-is/ci-test',
                        f'pulls/{pr_number}',
                        json={ "state" : "closed" },
                        status_code=200
                    )

    def test_push_while_building(self):
        BRANCH_NAME='test_push_while_building'
        SLOW_BRANCH_NAME='test_push_while_building2'
        call(['git', 'push', 'origin', ':'+BRANCH_NAME])
        with tempfile.TemporaryDirectory() as d:
            pr_number = {}
            source_sha = {}
            first_target_sha = {}
            gh_pr = {}
            pr = {}
            try:
                status = ci_get('/status', status_code=200)
                assert 'watched_repos' in status
                assert status['watched_repos'] == ['hail-is/ci-test']
                os.chdir(d)
                call(['git', 'clone', 'git@github.com:hail-is/ci-test.git'])
                os.chdir('ci-test')
                call(['git', 'remote', '-v'])

                # start slow branch
                call(['git', 'checkout', '-b', SLOW_BRANCH_NAME])
                with open('hail-ci-build.sh', 'w') as f:
                    f.write('sleep 30')
                call(['git', 'add', 'hail-ci-build.sh'])
                call(['git', 'commit', '-m', 'foo'])
                call(['git', 'push', 'origin', SLOW_BRANCH_NAME])
                source_sha[SLOW_BRANCH_NAME] = run(['git', 'rev-parse', SLOW_BRANCH_NAME], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()
                first_target_sha[SLOW_BRANCH_NAME] = run(['git', 'rev-parse', 'master'], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()
                gh_pr[SLOW_BRANCH_NAME] = post_repo(
                    'hail-is/ci-test',
                    'pulls',
                    json={ "title" : "foo", "head": SLOW_BRANCH_NAME, "base": "master" },
                    status_code=201
                )
                pr_number[SLOW_BRANCH_NAME] = gh_pr[SLOW_BRANCH_NAME]['number']
                time.sleep(7)
                post_repo(
                    'hail-is/ci-test',
                    f'pulls/{pr_number[SLOW_BRANCH_NAME]}/reviews',
                    json={ "commit_id": source_sha, "event": "APPROVE" },
                    status_code=200,
                    user='user2'
                )

                # get details on first job of slow branch
                pr[SLOW_BRANCH_NAME] = self.poll_until_pr_exists_and(
                    SLOW_BRANCH_NAME,
                    lambda x: x['status']['state'] == 'running'
                )
                assertDictHasKVs(pr[SLOW_BRANCH_NAME], {
                    'source_url': 'https://github.com/hail-is/ci-test.git',
                    'target_url': 'https://github.com/hail-is/ci-test.git',
                    'target_ref': 'master',
                    'status': {
                        'state': 'running',
                        'review_state': 'pending',
                        'source_sha': source_sha[SLOW_BRANCH_NAME],
                        'target_sha': first_target_sha[SLOW_BRANCH_NAME],
                        'pr_number': str(pr_number[SLOW_BRANCH_NAME]),
                        'docker_image': 'google/cloud-sdk:alpine'
                    }
                })
                first_slow_job_id = pr[SLOW_BRANCH_NAME]['status']['job_id']
                assert first_slow_job_id is not None

                # start fast branch
                call(['git', 'checkout', '-b', BRANCH_NAME])
                call(['git', 'commit', '--allow-empty', '-m' 'foo'])
                call(['git', 'push', 'origin', BRANCH_NAME])
                source_sha[BRANCH_NAME] = run(['git', 'rev-parse', BRANCH_NAME], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()
                target_sha[BRANCH_NAME] = run(['git', 'rev-parse', 'master'], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()
                gh_pr[BRANCH_NAME] = post_repo(
                    'hail-is/ci-test',
                    'pulls',
                    json={ "title" : "foo", "head": SLOW_BRANCH_NAME, "base": "master" },
                    status_code=201
                )
                pr_number[BRANCH_NAME] = gh_pr[BRANCH_NAME]['number']

                # wait for fast branch to finish and merge
                pr[BRANCH_NAME] = self.poll_until_merged_pr(BRANCH_NAME)
                assertDictHasKVs(pr[BRANCH_NAME], {
                    'source_url': 'https://github.com/hail-is/ci-test.git',
                    'target_url': 'https://github.com/hail-is/ci-test.git',
                    'target_ref': 'master',
                    'status': {
                        'state': 'merged',
                        'review_state': 'approved',
                        'source_sha': source_sha[BRANCH_NAME],
                        'target_sha': first_target_sha[BRANCH_NAME],
                        'pr_number': str(pr_number[BRANCH_NAME]),
                        'docker_image': 'google/cloud-sdk:alpine'
                    }
                })

                pr[SLOW_BRANCH_NAME] = self.get_pr(SLOW_BRANCH_NAME)
                assertDictHasKVs(pr, {
                    'source_url': 'https://github.com/hail-is/ci-test.git',
                    'target_url': 'https://github.com/hail-is/ci-test.git',
                    'target_ref': 'master',
                    'status': {
                        'state': 'running',
                        'review_state': 'pending',
                        'source_sha': source_sha,
                        'target_sha': second_target_sha,
                        'pr_number': str(pr_number),
                        'docker_image': 'google/cloud-sdk:alpine'
                    }
                })
                second_slow_job_id = pr[SLOW_BRANCH_NAME]['status']['job_id']
                assert second_slow_job_id != first_slow_job_id
                pr[SLOW_BRANCH_NAME] = self.poll_until_finished_pr(SLOW_BRANCH_NAME)
                assertDictHasKVs(pr, {
                    'source_url': 'https://github.com/hail-is/ci-test.git',
                    'target_url': 'https://github.com/hail-is/ci-test.git',
                    'target_ref': 'master',
                    'status': {
                        'state': 'success',
                        'review_state': 'pending',
                        'source_sha': source_sha,
                        'target_sha': second_target_sha,
                        'pr_number': str(pr_number),
                        'docker_image': 'google/cloud-sdk:alpine'
                    }
                })
                assert pr[SLOW_BRANCH_NAME]['status']['job_id'] == second_slow_job_id
            finally:
                call(['git', 'push', 'origin', ':'+SLOW_BRANCH_NAME])
                call(['git', 'push', 'origin', ':'+BRANCH_NAME])
                for pr_number in pr_number.values():
                    patch_repo(
                        'hail-is/ci-test',
                        f'pulls/{b1_pr_number}',
                        json={ "state" : "closed" },
                        status_code=200
                    )

    def test_merges_approved_pr(self):
        BRANCH_NAME='test_merges_approved_pr'
        call(['git', 'push', 'origin', ':'+BRANCH_NAME])
        with tempfile.TemporaryDirectory() as d:
            pr_number = None
            try:
                status = ci_get('/status', status_code=200)
                self.assertIn('watched_repos', status)
                self.assertEqual(status['watched_repos'], ['hail-is/ci-test'])
                os.chdir(d)
                call(['git', 'clone', 'git@github.com:hail-is/ci-test.git'])
                os.chdir('ci-test')
                call(['git', 'remote', '-v'])

                call(['git', 'checkout', '-b', BRANCH_NAME])
                call(['git', 'commit', '--allow-empty', '-m', 'foo'])
                call(['git', 'push', 'origin', BRANCH_NAME])
                source_sha = run(['git', 'rev-parse', BRANCH_NAME], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()
                target_sha = run(['git', 'rev-parse', 'master'], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()
                gh_pr = post_repo(
                    'hail-is/ci-test',
                    'pulls',
                    json={ "title" : "foo", "head": BRANCH_NAME, "base": "master" },
                    status_code=201
                )
                pr_number = str(gh_pr['number'])
                post_repo(
                    'hail-is/ci-test',
                    f'pulls/{pr_number}/reviews',
                    json={ "commit_id": source_sha, "event": "APPROVE" },
                    status_code=200,
                    user='user2'
                )
                r = get_repo(
                    'hail-is/ci-test',
                    f'pulls/{pr_number}/reviews',
                    status_code=200
                )
                time.sleep(7)
                pr = self.poll_until_finished_pr(BRANCH_NAME)
                assertDictHasKVs(pr, {
                    'source_url': 'https://github.com/hail-is/ci-test.git',
                    'target_url': 'https://github.com/hail-is/ci-test.git',
                    'target_ref': 'master',
                    'status': {
                        'state': Match.any('success', 'merged'),
                        'review_state': 'approved',
                        'source_sha': source_sha,
                        'target_sha': target_sha,
                        'pr_number': pr_number
                    }
                })
                get_repo(
                    'hail-is/ci-test',
                    f'pulls/{pr_number}/merge',
                    status_code=204, # 204 NO CONTENT means merged, 404 means not merged
                    json_response=False
                )
            finally:
                call(['git', 'push', 'origin', ':'+BRANCH_NAME])
                if pr_number is not None:
                    patch_repo(
                        'hail-is/ci-test',
                        f'pulls/{pr_number}',
                        json={ "state" : "closed" },
                        status_code=200
                    )
