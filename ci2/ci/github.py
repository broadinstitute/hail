from constants import GITHUB_CLONE_URL, SHA_LENGTH
import json


class Repo(object):
    def __init__(self, owner, name):
        assert isinstance(owner, str)
        assert isinstance(name, str)
        self.owner = owner
        self.name = name
        self.url = f'{GITHUB_CLONE_URL}{owner}/{name}.git'
        self.qname = self.short_str()

    def __eq__(self, other):
        return self.owner == other.owner and self.name == other.name

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.owner, self.name))

    def __str__(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_short_str(s):
        pieces = s.split("/")
        assert len(pieces) == 2, f'{pieces} {s}'
        return Repo(pieces[0], pieces[1])

    def short_str(self):
        return f'{self.owner}/{self.name}'

    @staticmethod
    def from_json(d):
        assert isinstance(d, dict), f'{type(d)} {d}'
        assert 'owner' in d, d
        assert 'name' in d, d
        return Repo(d['owner'], d['name'])

    def to_dict(self):
        return {'owner': self.owner, 'name': self.name}

    @staticmethod
    def from_gh_json(d):
        assert isinstance(d, dict), f'{type(d)} {d}'
        assert 'owner' in d, d
        assert 'login' in d['owner'], d
        assert 'name' in d, d
        return Repo(d['owner']['login'], d['name'])


class FQBranch(object):
    def __init__(self, repo, name):
        assert isinstance(repo, Repo)
        assert isinstance(name, str)
        self.repo = repo
        self.name = name

    def __eq__(self, other):
        return self.repo == other.repo and self.name == other.name

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.repo, self.name))

    def __str__(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_short_str(s):
        pieces = s.split(":")
        assert len(pieces) == 2, f'{pieces} {s}'
        return FQBranch(Repo.from_short_str(pieces[0]), pieces[1])

    def short_str(self):
        return f'{self.repo.short_str()}:{self.name}'

    @staticmethod
    def from_json(d):
        assert isinstance(d, dict), f'{type(d)} {d}'
        assert 'repo' in d, d
        assert 'name' in d, d
        return FQBranch(Repo.from_json(d['repo']), d['name'])

    def to_dict(self):
        return {'repo': self.repo.to_dict(), 'name': self.name}


class PR(object):
    def __init__(self, number, title, source_sha, target_branch):
        self.number = number
        self.title = title
        self.source_sha = source_sha
        self.target_branch = target_branch

        # one of pending, changes_requested, approve
        self.state = None

        self.job = None
        self.passing = None

    def update_from_gh_json(self, gh_json):
        assert self.number == gh_json['number']
        self.title = gh_json['title']

        new_source_sha = gh_json['head']['sha']
        if self.source_sha != new_source_sha:
            self.source_sha = new_source_sha
            self.job = None
            self.passing = None

    @staticmethod
    def from_gh_json(gh_json, target_branch):
        return PR(gh_json['number'], gh_json['title'], gh_json['head']['sha'], target_branch)

    async def refresh(self, gh):
        latest_state_by_login = {}
        async for review in gh.getiter(f'/repos/{self.target_branch.branch.repo.short_str()}/pulls/{self.number}/reviews'):
            login = review['user']['login']
            state = review['state']
            # reviews is chronological, so later ones are newer statuses
            if state == 'APPROVED' or state == 'CHANGES_REQUESTED':
                latest_state_by_login[login] = state

        total_state = 'pending'
        for login, state in latest_state_by_login.items():
            if (state == 'CHANGES_REQUESTED'):
                total_state = 'changes_requested'
                break
            elif (state == 'DISMISSED'):
                total_state = 'pending'
                break
            elif (state == 'APPROVED'):
                total_state = 'approved'

        self.state = total_state

    async def heal(self, batch, run, seen_job_ids):
        if self.job is None:
            jobs = await batch.list_jobs(
                complete=False,
                attributes={
                    'source_sha': self.source_sha,
                    'target_sha': self.target_branch.sha
                })
            # we might be returning to a commit that was only partially tested
            jobs = [j for j in jobs if j._status['state'] != 'Cancelled']

            # should be at most one job
            if len(jobs) > 0:
                self.job = jobs[0]
            elif run:
                self.job = await batch.create_job(
                    # FIXME build
                    'alpine', ['echo', 'foo'],
                    attributes={
                        'target_branch': self.target_branch.branch.short_str(),
                        'pr': str(self.number),
                        'source_sha': self.source_sha,
                        'target_sha': self.target_branch.sha
                    })

        if self.job:
            seen_job_ids.add(self.job.id)

            if self.passing is None:
                status = await self.job.status()
                state = status['state']
                if state == 'Complete':
                    self.passing = status['exit_code'] == 0
                else:
                    assert state == 'Created'


class WatchedBranch(object):
    def __init__(self, branch):
        self.branch = branch
        self.prs = None
        self.sha = None

    async def refresh(self, gh):
        repo_ss = self.branch.repo.short_str()

        branch_gh_json = await gh.getitem(f'/repos/{repo_ss}/git/refs/heads/{self.branch.name}')
        new_sha = branch_gh_json['object']['sha']
        if new_sha != self.sha:
            self.sha = new_sha
            if self.prs:
                for pr in self.prs:
                    pr.job = None
                    pr.passing = None

        new_prs = {}
        async for gh_json_pr in gh.getiter(f'/repos/{repo_ss}/pulls?state=open&base={self.branch.name}'):
            number = gh_json_pr['number']
            if self.prs is not None and number in self.prs:
                pr = self.prs[number]
                pr.update_from_gh_json(gh_json_pr)
            else:
                pr = PR.from_gh_json(gh_json_pr, self)
            new_prs[number] = pr
        self.prs = new_prs

        for pr in self.prs.values():
            await pr.refresh(gh)

    async def heal(self, batch):
        # FIXME merge
        merge_candidate = None
        for pr in self.prs.values():
            if pr.state == 'approved' and pr.passing is None:
                merge_candidate = pr.number
                break

        running_jobs = await batch.list_jobs(
            complete=False,
            attributes={
                'target_branch': self.branch.short_str()
            })

        seen_job_ids = set()
        for number, pr in self.prs.items():
            await pr.heal(batch, (merge_candidate is None or pr.number == merge_candidate), seen_job_ids)

        for job in running_jobs:
            if job.id not in seen_job_ids:
                await job.cancel()
