from typing import Any, Dict
import aiohttp_jinja2
from aiohttp import web
import logging
from gear import setup_aiohttp_session, web_authenticated_developers_only
from hailtop.config import get_deploy_config
from hailtop.tls import get_in_cluster_server_ssl_context
from hailtop.hail_logging import AccessLogger, configure_logging
from web_common import setup_aiohttp_jinja2, setup_common_static_routes
import json
import re

configure_logging()
router = web.RouteTableDef()
logging.basicConfig(level=logging.DEBUG)
deploy_config = get_deploy_config()
log = logging.getLogger('benchmark')


file_path = '/Users/dabuhijl/hail/0.2.45-ac6815ee857c-master.json'
with open(file_path) as f:
    pre_data = json.load(f)
print(json.dumps(pre_data, indent=4, sort_keys=True))

x = re.findall('.*/+(.*)-(.*)-(.*)?\.json', file_path)
sha = x[0][1]

data = list()
prod_of_means = 1
for d in pre_data['benchmarks']:
    stats = dict()
    stats['name'] = d['name']
    stats['failed'] = d['failed']
    if not (d['failed']):
        prod_of_means *= d['mean']
        stats['f-stat'] = d['f-stat']
        stats['mean'] = d['mean']
        stats['median'] = d['median']
        stats['p-value'] = d['p-value']
        stats['stdev'] = d['stdev']
    data.append(stats)
geometric_mean = prod_of_means ** (1.0 / len(pre_data['benchmarks']))

benchmarks = dict()
benchmarks['sha'] = sha
benchmarks['geometric-mean'] = geometric_mean
benchmarks['data'] = data


@router.get('/healthcheck')
async def healthcheck(request: web.Request) -> web.Response:  # pylint: disable=unused-argument
    return web.Response()


@router.get('/{username}')
async def greet_user(request: web.Request) -> web.Response:

    context = {
        'username': request.match_info.get('username', ''),
        'current_date': 'July 10, 2020'
    }
    response = aiohttp_jinja2.render_template('user.html', request,
                                              context=context)
    return response


@router.get('/')
@router.get('')
@web_authenticated_developers_only(redirect=False)
async def index(request: web.Request, userdata) -> Dict[str, Any]:  # pylint: disable=unused-argument
    context = {
        'current_date': 'July 10, 2020'
    }
    response = aiohttp_jinja2.render_template('index.html', request,
                                              context=benchmarks)
    return response


def init_app() -> web.Application:
    app = web.Application()
    setup_aiohttp_jinja2(app, 'benchmark')
    setup_aiohttp_session(app)

    setup_common_static_routes(router)
    app.add_routes(router)

    web.run_app(deploy_config.prefix_application(init_app(), 'benchmark'),
                host='0.0.0.0',
                port=5000,
                access_log_class=AccessLogger,
                ssl_context=get_in_cluster_server_ssl_context())
