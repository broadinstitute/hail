import os
import base64
import json
import kubernetes_asyncio as kube
from hailtop import aiogoogle
from hailtop.utils import async_to_blocking
from gear import Database

from auth.driver.driver import create_user

SCOPE = os.environ['HAIL_SCOPE']
PROJECT = os.environ['HAIL_PROJECT']
DEFAULT_NAMESPACE = os.environ['HAIL_DEFAULT_NAMESPACE']

async def insert_user_if_not_exists(app, username, email, is_developer, is_service_account):
    db = app['db']
    k8s_client = app['k8s_client']

    row = await db.execute_and_fetchone('SELECT id, state FROM users where username = %s;', (username,))
    if row:
        if row['state'] == 'active':
            return None
        return row['id']

    gsa_key_secret_name = f'{username}-gsa-key'

    secret = await k8s_client.read_namespaced_secret(gsa_key_secret_name, DEFAULT_NAMESPACE)
    key_json = base64.b64decode(secret.data['key.json']).decode()
    key = json.loads(key_json)
    gsa_email = key['client_email']

    if is_developer and SCOPE != 'deploy':
        namespace_name = DEFAULT_NAMESPACE
    else:
        namespace_name = None

    return await db.execute_insertone(
        '''
INSERT INTO users (state, username, email, is_developer, is_service_account, gsa_email, gsa_key_secret_name, namespace_name)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
''',
        ('creating', username, email, is_developer, is_service_account, gsa_email, gsa_key_secret_name, namespace_name))


async def main():
    users = [
        # username, email, is_developer, is_service_account
        ('auth', None, 0, 1),
        ('benchmark', None, 0, 1),
        ('ci', None, 0, 1),
        ('test', None, 0, 0),
        ('test-dev', None, 1, 0)
    ]

    app = {}

    db = Database()
    await db.async_init(maxsize=50)
    app['db'] = db

    db_instance = Database()
    await db_instance.async_init(maxsize=50, config_file='/database-server-config/sql-config.json')
    app['db_instance'] = db_instance

    # kube.config.load_incluster_config()
    await kube.config.load_kube_config()
    k8s_client = kube.client.CoreV1Api()
    app['k8s_client'] = k8s_client

    app['iam_client'] = aiogoogle.IAmClient(
        PROJECT, credentials=aiogoogle.Credentials.from_file('/gsa-key/key.json'))

    for username, email, is_developer, is_service_account in users:
        user_id = await insert_user_if_not_exists(app, username, email, is_developer, is_service_account)

        if user_id is not None:
            db_user = await db.execute_and_fetchone('SELECT * FROM users where id = %s;', (user_id,))
            await create_user(app, db_user, skip_trial_bp=True)


async_to_blocking(main())
