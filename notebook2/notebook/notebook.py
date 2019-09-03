import logging
import os
import uuid
import jinja2
import aiohttp
from aiohttp import web
import aiohttp_session
import aiohttp_session.cookie_storage
import aiohttp_jinja2
import sass
from kubernetes_asyncio import client, config
import kubernetes_asyncio as kube

from hailtop.gear import get_deploy_config
from hailtop.gear.auth import web_authenticated_users_only, web_maybe_authenticated_user

log = logging.getLogger('notebook2')

NOTEBOOK_NAMESPACE = os.environ['HAIL_NOTEBOOK_NAMESPACE']

deploy_config = get_deploy_config()

routes = web.RouteTableDef()

notebook_root = os.path.dirname(os.path.abspath(__file__))

scss_path = f'{notebook_root}/static/styles'
css_path = f'{notebook_root}/static/css'
os.makedirs(css_path, exist_ok=True)

sass.compile(dirname=(scss_path, css_path), output_style='compressed')

# Must be int for Kubernetes V1 api timeout_seconds property
KUBERNETES_TIMEOUT_IN_SECONDS = float(os.environ.get('KUBERNETES_TIMEOUT_IN_SECONDS', 5))

INSTANCE_ID = uuid.uuid4().hex

POD_PORT = 8888

WORKER_IMAGE = os.environ['HAIL_NOTEBOOK2_WORKER_IMAGE']

log.info(f'KUBERNETES_TIMEOUT_IN_SECONDS {KUBERNETES_TIMEOUT_IN_SECONDS}')
log.info(f'INSTANCE_ID {INSTANCE_ID}')


async def start_pod(k8s, userdata):
    jupyter_token = uuid.uuid4().hex
    pod_id = uuid.uuid4().hex

    user_id = userdata['id']
    ksa_name = userdata['ksa_name']
    bucket = userdata['bucket_name']
    gsa_key_secret_name = userdata['gsa_key_secret_name']
    jwt_secret_name = userdata['jwt_secret_name']

    pod_spec = kube.client.V1PodSpec(
        service_account_name=ksa_name,
        containers=[
            kube.client.V1Container(
                command=[
                    'jupyter',
                    'notebook',
                    f'--NotebookApp.token={jupyter_token}',
                    f'--NotebookApp.base_url=/instance/{pod_id}/',
                    f'--GoogleStorageContentManager.default_path="{bucket}"',
                    "--ip", "0.0.0.0", "--no-browser", "--allow-root"
                ],
                name='default',
                image=WORKER_IMAGE,
                env=[kube.client.V1EnvVar(name='HAIL_TOKEN_FILE',
                                          value='/user-jwt/jwt')],
                ports=[kube.client.V1ContainerPort(container_port=POD_PORT)],
                resources=kube.client.V1ResourceRequirements(
                    requests={'cpu': '1.601', 'memory': '1.601G'}),
                readiness_probe=kube.client.V1Probe(
                    period_seconds=5,
                    http_get=kube.client.V1HTTPGetAction(
                        path=f'/instance/{pod_id}/login',
                        port=POD_PORT)),
                volume_mounts=[
                    kube.client.V1VolumeMount(
                        mount_path='/gsa-key',
                        name='gsa-key',
                        read_only=True
                    ),
                    kube.client.V1VolumeMount(
                        mount_path='/user-jwt',
                        name='user-jwt',
                        read_only=True
                    )
                ]
            )
        ],
        volumes=[
            kube.client.V1Volume(
                name='gsa-key',
                secret=kube.client.V1SecretVolumeSource(
                    secret_name=gsa_key_secret_name)),
            kube.client.V1Volume(
                name='user-jwt',
                secret=kube.client.V1SecretVolumeSource(
                    secret_name=jwt_secret_name))
        ])
    pod_template = kube.client.V1Pod(
        metadata=kube.client.V1ObjectMeta(
            generate_name='notebook2-worker-',
            labels={
                'app': 'notebook2-worker',
                'hail.is/notebook2-instance': INSTANCE_ID,
                'uuid': pod_id,
                'jupyter-token': jupyter_token,
                'user_id': str(user_id)
            }),
        spec=pod_spec)
    pod = await k8s.create_namespaced_pod(
        NOTEBOOK_NAMESPACE,
        pod_template,
        _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)

    return pod


def container_status_for_ui(container_statuses):
    """
        Summarize the container status based on its most recent state

        Parameters
        ----------
        container_statuses : list[V1ContainerStatus]
            https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ContainerStatus.md
    """
    if container_statuses is None:
        return None

    assert len(container_statuses) == 1

    state = container_statuses[0].state

    if state.running:
        return {"running": {"started_at": state.running.started_at}}

    if state.waiting:
        return {"waiting": {"reason": state.waiting.reason}}

    if state.terminated:
        return {"terminated": {
            "exit_code": state.terminated.exit_code,
            "finished_at": state.terminated.finished_at,
            "started_at": state.terminated.started_at,
            "reason": state.terminated.reason
        }}

    # FIXME
    return None


def pod_condition_for_ui(conds):
    """
        Return the most recent status=="True" V1PodCondition or None
        Parameters
        ----------
        conds : list[V1PodCondition]
            https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1PodCondition.md
    """
    if conds is None:
        return None

    maxCond = max(conds, key=lambda c: (c.last_transition_time, c.status == 'True'))

    return {"status": maxCond.status, "type": maxCond.type}


def pod_to_ui_dict(pod):
    notebook = {
        'name': 'a_notebook',
        'pod_name': pod.metadata.name,
        'pod_status': pod.status.phase,
        'pod_uuid': pod.metadata.labels['uuid'],
        'pod_ip': pod.status.pod_ip,
        'creation_date': pod.metadata.creation_timestamp.strftime('%D'),
        'jupyter_token': pod.metadata.labels['jupyter-token'],
        'container_status': container_status_for_ui(pod.status.container_statuses),
        'condition': pod_condition_for_ui(pod.status.conditions),
        'deletion_timestamp': pod.metadata.deletion_timestamp
    }

    notebook['url'] = f"/instance/{notebook['pod_uuid']}/?token={notebook['jupyter_token']}"

    return notebook


async def get_notebook(k8s, session, userdata):
    if 'notebook' in session:
        return session['notebook']

    user_id = userdata['id']
    pods = await k8s.list_namespaced_pod(
        namespace=NOTEBOOK_NAMESPACE,
        label_selector=f"user_id={user_id}",
        _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)

    for pod in pods.items:
        if pod.metadata.deletion_timestamp is None:
            notebook = pod_to_ui_dict(pod)
            session['notebook'] = notebook
            return notebook


async def delete_worker_pod(k8s, pod_name):
    try:
        await k8s.delete_namespaced_pod(
            pod_name,
            NOTEBOOK_NAMESPACE,
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
    except kube.client.rest.ApiException as e:
        log.info(f'pod {pod_name} already deleted {e}')


@routes.get('/healthcheck')
async def healthcheck(request):  # pylint: disable=unused-argument
    return web.Response()


def base_context(userdata):
    return {
        'base_path': deploy_config.base_path('notebook2'),
        'auth_base_url': deploy_config.external_url('auth', ''),
        'userdata': userdata
    }


@routes.get('/')
@aiohttp_jinja2.template('index.html')
@web_maybe_authenticated_user
async def index(request, userdata):  # pylint: disable=unused-argument
    context = base_context(userdata)
    return context


@routes.get('/notebook')
@aiohttp_jinja2.template('notebook.html')
@web_authenticated_users_only
async def notebook_page(request, userdata):
    k8s = request.app['k8s_client']
    session = await aiohttp_session.get_session(request)
    context = base_context(userdata)
    context['notebook'] = await get_notebook(k8s, session, userdata)
    return context


@routes.post('/notebook/delete')
@web_authenticated_users_only
async def notebook_delete(request, userdata):
    k8s = request.app['k8s_client']
    session = await aiohttp_session.get_session(request)
    notebook = await get_notebook(k8s, session, userdata)
    if notebook:
        await delete_worker_pod(k8s, notebook['pod_name'])
        del session['notebook']

    return web.HTTPFound(location=deploy_config.external_url('notebook2', '/notebook'))


@routes.post('/notebook')
@web_authenticated_users_only
async def notebook_post(request, userdata):
    k8s = request.app['k8s_client']
    session = aiohttp_session.get_session(request)
    pod = await start_pod(k8s, userdata)
    session['notebook'] = pod_to_ui_dict(pod)
    return web.HTTPFound(location=deploy_config.external_url('notebook2', '/notebook'))


@routes.get('/auth/{requested_pod_uuid}')
@web_authenticated_users_only
async def auth(request, userdata):
    request_pod_uuid = request.match_info['requested_pod_uuid']
    k8s = request.app['k8s_client']
    session = aiohttp_session.get_session(request)
    notebook = await get_notebook(k8s, session, userdata)
    if notebook and notebook['pod_uuid'] == request_pod_uuid:
        return web.Response(headers={
            'pod_ip': f"{notebook['pod_ip']}:{POD_PORT}"
        })

    return web.HTTPNotFound()


@routes.get('/worker-image')
async def worker_image():
    return web.Response(text=WORKER_IMAGE)


@routes.get('/wait')
@web_authenticated_users_only
async def wait_websocket(request, userdata):
    k8s = request.app['k8s_client']
    session = aiohttp_session.get_session(request)
    notebook = await get_notebook(k8s, session, userdata)

    if not notebook:
        return web.HTTPNotFound()

    notebook = session['notebook']

    pod_uuid = notebook['pod_uuid']
    url = deploy_config.external_url('notebook2', '/instance-ready/{pod_uuid}/')

    ws = web.WebSocketResponse()
    await ws.prepare(request)

    attempts = 0
    while attempts < 10:
        try:
            async with aiohttp.ClientSession(raise_for_status=True,
                                             timeout=aiohttp.ClientTimeout(total=60)) as session:
                async with session.head(url, cookies=request.cookies) as resp:
                    if resp.status == 405:
                        log.info(f'HEAD on jupyter succeeded for pod_uuid: {pod_uuid} : response: {resp}')
                        break
                    else:
                        log.info(f'HEAD on jupyter failed for {pod_uuid}: {resp}')
        except aiohttp.ServerTimeoutError:
            log.info(f'HEAD on jupyter timed out for pod_uuid : {pod_uuid}')

        attempts += 1

    await ws.send_str("1")

    return ws


@routes.get('/error')
@aiohttp_jinja2.template('error.html')
@web_maybe_authenticated_user
async def error_page(request, userdata):  # pylint: disable=unused-argument
    context = base_context(userdata)
    context['error'] = request.args.get('err')
    return context


@routes.get('/user')
@aiohttp_jinja2.template('user.html')
@web_authenticated_users_only
async def user_page(request, userdata):  # pylint: disable=unused-argument
    context = base_context(userdata)
    return context


async def on_startup(app):
    if 'BATCH_USE_KUBE_CONFIG' in os.environ:
        await config.load_kube_config()
    else:
        config.load_incluster_config()
    app['k8s_client'] = client.CoreV1Api()


def run():
    routes.static('/static', f'{notebook_root}/static')
    app = web.Application()

    with open('/session-secret-keys/aiohttp-session-secret-key', 'rb') as f:
        aiohttp_session.setup(app, aiohttp_session.cookie_storage.EncryptedCookieStorage(
            f.read(),
            cookie_name=deploy_config.auth_session_cookie_name(),
            domain=os.environ['HAIL_DOMAIN'],
            # 2592000s = 30d
            max_age=2592000))

    app.add_routes(routes)
    app.on_startup.append(on_startup)
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(os.path.join(notebook_root, 'templates')))
    web.run_app(deploy_config.prefix_application(app, 'notebook2'), host='0.0.0.0', port=5000)
