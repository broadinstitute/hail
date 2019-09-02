import asyncio
import aiohttp

from hailtop.gear import get_deploy_config
from hailtop.gear.auth import get_tokens, auth_headers


def init_parser(parser):  # pylint: disable=unused-argument
    pass


async def async_main():
    deploy_config = get_deploy_config()

    headers = auth_headers('auth')
    async with aiohttp.ClientSession(
            raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60), headers=headers) as session:
        async with session.post(deploy_config.url('auth', '/api/v1alpha/logout')):
            pass
    tokens = get_tokens()
    auth_ns = deploy_config.service_ns('auth')
    del tokens[auth_ns]
    tokens.write()

    print('Logged out.')


def main(args):  # pylint: disable=unused-argument
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main())
