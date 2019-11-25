import pytest
import aiohttp
import asyncio

from hailtop.config import get_deploy_config
import hailtop.utils as utils

pytestmark = pytest.mark.asyncio


async def test_deploy():
    deploy_config = get_deploy_config()
    ci_deploy_status_url = deploy_config.url('ci', '/api/v1alpha/deploy_status')
    async with aiohttp.ClientSession(
            raise_for_status=True,
            timeout=aiohttp.ClientTimeout(total=60)) as session:
        deploy_state = None
        while deploy_state is None:
            resp = await utils.request_retry_transient_errors(
                session, 'POST', f'{ci_deploy_status_url}')
            deploy_statuses = resp.json()
            assert len(deploy_statuses) == 1, deploy_statuses
            deploy_status = deploy_statuses[0]
            deploy_state = deploy_status['deploy_state']
            await asyncio.sleep(5)
        assert deploy_state == 'success', deploy_state
