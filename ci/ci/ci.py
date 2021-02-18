import traceback
import json
import logging
import aiohttp
from aiohttp import web
import uvloop  # type: ignore
from gidgethub import aiohttp as gh_aiohttp
from hailtop.utils import collect_agen, humanize_timedelta_msecs
from hailtop.batch_client.aioclient import BatchClient
from hailtop.config import get_deploy_config
from hailtop.tls import internal_server_ssl_context
from hailtop.hail_logging import AccessLogger
from gear import (
    setup_aiohttp_session,
    rest_authenticated_developers_only,
    web_authenticated_developers_only,
)
from web_common import setup_aiohttp_jinja2, setup_common_static_routes, render_template

from .github import FQBranch, UnwatchedBranch

log = logging.getLogger('ci')

uvloop.install()

deploy_config = get_deploy_config()

routes = web.RouteTableDef()


@routes.get('/batches')
@web_authenticated_developers_only()
async def get_batches(request, userdata):
    batch_client = request.app['batch_client']
    batches = [b async for b in batch_client.list_batches()]
    statuses = [await b.last_known_status() for b in batches]
    page_context = {'batches': statuses}
    return await render_template('ci', request, userdata, 'batches.html', page_context)


@routes.get('/batches/{batch_id}')
@web_authenticated_developers_only()
async def get_batch(request, userdata):
    batch_id = int(request.match_info['batch_id'])
    batch_client = request.app['batch_client']
    b = await batch_client.get_batch(batch_id)
    status = await b.last_known_status()
    jobs = await collect_agen(b.jobs())
    for j in jobs:
        j['duration'] = humanize_timedelta_msecs(j['duration'])
    page_context = {'batch': status, 'jobs': jobs}
    return await render_template('ci', request, userdata, 'batch.html', page_context)


@routes.get('/batches/{batch_id}/jobs/{job_id}')
@web_authenticated_developers_only()
async def get_job(request, userdata):
    batch_id = int(request.match_info['batch_id'])
    job_id = int(request.match_info['job_id'])
    batch_client = request.app['batch_client']
    job = await batch_client.get_job(batch_id, job_id)
    page_context = {
        'batch_id': batch_id,
        'job_id': job_id,
        'job_log': await job.log(),
        'job_status': json.dumps(await job.status(), indent=2),
        'attempts': await job.attempts(),
    }
    return await render_template('ci', request, userdata, 'job.html', page_context)


@routes.get('/healthcheck')
async def healthcheck(request):  # pylint: disable=unused-argument
    return web.Response(status=200)


@routes.post('/api/v1alpha/dev_deploy_branch')
@rest_authenticated_developers_only
async def dev_deploy_branch(request, userdata):
    app = request.app
    try:
        params = await request.json()
    except Exception as e:
        message = 'could not read body as JSON'
        log.info('dev deploy failed: ' + message, exc_info=True)
        raise web.HTTPBadRequest(text=message) from e

    try:
        branch = FQBranch.from_short_str(params['branch'])
        steps = params['steps']
    except Exception as e:
        message = (
            f'parameters are wrong; check the branch and steps syntax.\n\n{params}'
        )
        log.info('dev deploy failed: ' + message, exc_info=True)
        raise web.HTTPBadRequest(text=message) from e

    gh = app['github_client']
    request_string = (
        f'/repos/{branch.repo.owner}/{branch.repo.name}/git/refs/heads/{branch.name}'
    )

    try:
        branch_gh_json = await gh.getitem(request_string)
        sha = branch_gh_json['object']['sha']
    except Exception as e:
        message = f'error finding {branch} at GitHub'
        log.info('dev deploy failed: ' + message, exc_info=True)
        raise web.HTTPBadRequest(text=message) from e

    unwatched_branch = UnwatchedBranch(branch, sha, userdata)

    batch_client = app['batch_client']

    try:
        batch_id = await unwatched_branch.deploy(batch_client, steps)
    except Exception as e:  # pylint: disable=broad-except
        message = traceback.format_exc()
        log.info('dev deploy failed: ' + message, exc_info=True)
        raise web.HTTPBadRequest(
            text=f'starting the deploy failed due to\n{message}'
        ) from e
    return web.json_response({'sha': sha, 'batch_id': batch_id})


async def on_startup(app):
    app['gh_client_session'] = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=5)
    )
    app['github_client'] = gh_aiohttp.GitHubAPI(app['gh_client_session'], 'ci')
    app['batch_client'] = BatchClient('ci')


async def on_cleanup(app):
    await app['gh_client_session'].close()
    await app['batch_client'].close()


def run():
    app = web.Application()
    setup_aiohttp_jinja2(app, 'ci')
    setup_aiohttp_session(app)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    setup_common_static_routes(routes)
    app.add_routes(routes)

    web.run_app(
        deploy_config.prefix_application(app, 'ci'),
        host='0.0.0.0',
        port=5000,
        access_log_class=AccessLogger,
        ssl_context=internal_server_ssl_context(),
    )
