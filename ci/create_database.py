import os
import sys
import string
import json
import secrets
import asyncio
from shlex import quote as shq
from hailtop.utils import check_shell, check_shell_output
from gear import Database


def generate_token(size=12):
    assert size > 0
    alpha = string.ascii_lowercase
    alnum = string.ascii_lowercase + string.digits
    return secrets.choice(alpha) + ''.join([secrets.choice(alnum) for _ in range(size - 1)])


async def write_user_config(namespace, database_name, user, config):
    with open(f'sql-config.json', 'w') as f:
        f.write(json.dumps(config))

    with open(f'sql-config.cnf', 'w') as f:
        f.write(f'''
host={config["host"]}
user={config["user"]}
password="{config["password"]}"
database={config["db"]}
''')

    secret_name = f'sql-{database_name}-{user}-config'
    print(f'creating secret {secret_name}')
    await check_shell(
        f'''
kubectl -n {shq(namespace)} delete --ignore-not-found=true secret {shq(secret_name)}
kubectl -n {shq(namespace)} create secret generic {shq(secret_name)} --from-file=sql-config.json --from-file=sql-config.cnf
''')


async def create_database(create_database_config):
    with open('/sql-config/sql-config.json', 'r') as f:
        sql_config = json.loads(f.read())

    namespace = create_database_config['namespace']
    scope = create_database_config['scope']
    code_short_str = create_database_config['code_short_str']
    database_name = create_database_config['database_name']
    cant_create_database = create_database_config['cant_create_database']

    if cant_create_database:
        assert sql_config.get('db') is not None

        await write_user_config(namespace, database_name, 'admin', sql_config)
        await write_user_config(namespace, database_name, 'user', sql_config)
        return

    db = Database()
    await db.async_init()

    if scope == 'deploy':
        # create if not exists
        rows = db.execute_and_fetchall(
            "SHOW DATABASES LIKE '{database_name}';")
        rows = [row async for row in rows]
        if len(rows) > 0:
            assert len(rows) == 1
            return

        _name = database_name
        admin_username = f'{database_name}-admin'
        user_username = f'{database_name}-user'
    else:
        _name = f'{code_short_str}-{database_name}-{generate_token()}'
        admin_username = generate_token()
        user_username = generate_token()

    admin_password = secrets.token_urlsafe(16)
    user_password = secrets.token_urlsafe(16)

    await db.just_execute(f'''
CREATE DATABASE `{_name}`;

CREATE USER '{admin_username}'@'%' IDENTIFIED BY '{admin_password}';
GRANT ALL ON `{_name}`.* TO '{admin_username}'@'%';

CREATE USER '{user_username}'@'%' IDENTIFIED BY '{user_password}';
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON `{_name}`.* TO '{user_username}'@'%';
''')

    await write_user_config(namespace, database_name, 'admin', {
        'host': sql_config['host'],
        'port': sql_config['port'],
        'instance': sql_config['instance'],
        'connection_name': sql_config['connection_name'],
        'user': admin_username,
        'password': admin_password,
        'db': _name
    })

    await write_user_config(namespace, database_name, 'user', {
        'host': sql_config['host'],
        'port': sql_config['port'],
        'instance': sql_config['instance'],
        'connection_name': sql_config['connection_name'],
        'user': user_username,
        'password': user_password,
        'db': _name
    })


async def migrate(database_name, db, i, migration):
    print(f'applying migration {i} {migration}')

    # version to migrate to
    # the 0th migration migrates from 1 to 2
    to_version = i + 2

    name = migration['name']
    script = migration['script']

    out, _ = await check_shell_output(f'sha1sum {script} | cut -d " " -f1')
    script_sha1 = out.decode('utf-8').strip()
    print(f'script_sha1 {script_sha1}')

    row = await db.execute_and_fetchone(
        f'SELECT version FROM {database_name}_migration_version;')
    current_version = row['version']

    if current_version + 1 == to_version:
        # migrate
        if script.endswith('.py'):
            await check_shell(f'python3 {script}')
        else:
            await check_shell(f'''
mysql --defaults-extra-file=$HAIL_DATABASE_CONFIG_FILE <{script}
''')

        await db.just_execute(
            f'''
UPDATE {database_name}_migration_version
SET version = %s;

INSERT INTO {database_name}_migrations (version, name, script_sha1)
VALUES (%s, %s, %s);
''',
            (to_version, to_version, name, script_sha1))
    else:
        assert current_version >= to_version

        # verify checksum
        row = await db.execute_and_fetchone(
            f'SELECT * FROM {database_name}_migrations WHERE version = %s;', (to_version,))
        assert row is not None
        assert name == row['name']
        assert script_sha1 == row['script_sha1']


async def async_main():
    assert len(sys.argv) == 2
    create_database_config = json.loads(sys.argv[1])

    await create_database(create_database_config)

    namespace = create_database_config['namespace']
    database_name = create_database_config['database_name']

    admin_secret_name = f'sql-{database_name}-admin-config'
    out, _ = await check_shell_output(
        f'''
kubectl -n {namespace} get -o jsonpath={{.data.sql-config\\\\.json}} secret {shq(admin_secret_name)} \\
  | base64 -d
''')

    with open('sql-config.json', 'w') as f:
        f.write(out.decode('utf-8'))

    config_file = f'{os.getcwd()}/sql-config.json'
    print(f'database config file {config_file}')
    os.environ['HAIL_DATABASE_CONFIG_FILE'] = config_file

    db = Database()
    await db.async_init()

    rows = db.execute_and_fetchall(
        f"SHOW TABLES LIKE '{database_name}_migration_version';")
    rows = [row async for row in rows]
    if len(rows) == 0:
        await db.just_execute(f'''
CREATE TABLE `{database_name}_migration_version` (
  `version` BIGINT NOT NULL
) ENGINE = InnoDB;
INSERT INTO `{database_name}_migration_version` (`version`) VALUES (1);

CREATE TABLE {database_name}_migrations (
  `version` BIGINT NOT NULL,
  `name` VARCHAR(100),
  `script_sha1` VARCHAR(40),
  PRIMARY KEY (`version`)
) ENGINE = InnoDB;
''')

    migrations = create_database_config['migrations']
    for i, m in enumerate(migrations):
        await migrate(database_name, db, i, m)


loop = asyncio.get_event_loop()
loop.run_until_complete(async_main())
