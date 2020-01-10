import time
import sortedcontainers


class K8sCache:
    def __init__(self, client, refresh_time, max_size=100):
        self.client = client
        self.refresh_time = refresh_time
        self.max_size = max_size

        self.secrets = {}
        self.secret_ids = sortedcontainers.SortedSet(
            key=lambda id: self.secrets[id][1])

        self.service_accounts = {}
        self.service_account_ids = sortedcontainers.SortedSet(
            key=lambda id: self.service_accounts[id][1])

    async def read_secret(self, name, namespace, timeout):
        id = (name, namespace)

        secret, time_updated = self.secrets.get(id, (None, None))
        if time_updated and time.time() < time_updated + self.refresh_time:
            return secret

        if len(self.secrets) == self.max_size:
            head_id = self.secret_ids.pop(0)
            del self.secrets[head_id]

        secret = await self.client.read_namespaced_secret(
            name, namespace, _request_timeout=timeout)
        self.secrets[id] = (secret, time.time())
        self.secret_ids.add(id)

        return secret

    async def read_service_account(self, name, namespace, timeout):
        id = (name, namespace)

        sa, time_updated = self.service_accounts.get(id, (None, None))
        if time_updated and time.time() < time_updated + self.refresh_time:
            return sa

        if len(self.service_accounts) == self.max_size:
            head_id = self.service_account_ids.pop(0)
            del self.service_accounts[head_id]

        sa = await self.client.read_namespaced_service_account(
            name, namespace, _request_timeout=timeout)
        self.service_accounts[id] = (sa, time.time())
        self.service_account_ids.add(id)

        return sa
