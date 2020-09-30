import logging
import asyncio
import pytest
import aiohttp

from hailtop.config import get_deploy_config
from hailtop.auth import service_auth_headers
from hailtop import httpx
import hailtop.utils as utils

pytestmark = pytest.mark.asyncio

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def test_billing_monitoring():
    deploy_config = get_deploy_config()
    monitoring_deploy_config_url = deploy_config.url('monitoring', '/api/v1alpha/billing')
    headers = service_auth_headers(deploy_config, 'monitoring')
    async with httpx.client_session(
            timeout=aiohttp.ClientTimeout(total=60)) as session:

        async def wait_forever():
            data = None
            while data is None:
                async with session.get(f'{monitoring_deploy_config_url}', headers=headers) as resp:
                    data = await resp.json()
                await asyncio.sleep(5)
            return data

        data = await asyncio.wait_for(wait_forever(), timeout=30 * 60)
        assert data['cost_by_service'] is not None, data
