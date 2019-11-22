import asyncio
import aiohttp
import collections
import struct

import hailtop.utils as utils
from hailtop.config import get_deploy_config


class DBufClient:
    async def __aenter__(self):
        self.aiosession = aiohttp.ClientSession(raise_for_status=True,
                                                timeout=aiohttp.ClientTimeout(total=60))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aiosession.close()

    def __init__(self,
                 name,
                 id=None,
                 max_bufsize=10*1024*1024 - 1,
                 deploy_config=None):
        if not deploy_config:
            deploy_config = get_deploy_config()
        self.deploy_config = deploy_config
        self.root_url = deploy_config.base_url(name)
        self.session_url = None if id is None else f'{self.root_url}/s/{id}'
        self.aiosession = None
        self.id = id
        self.offs = []
        self.sizes = []
        self.cursor = 0
        self.max_bufsize = max_bufsize

    async def create(self):
        async with utils.request_retry_transient_errors(
                self.aiosession,
                'POST',
                f'{self.root_url}/s') as resp:
            assert resp.status == 200
            self.id = int(await resp.text())
        self.session_url = f'{self.root_url}/s/{self.id}'
        return self.id

    async def start_write(self):
        return DBufAppender(self.max_bufsize, self.aiosession, self.session_url)

    async def get(self, key):
        server = key[0]
        server_url = self.deploy_config.base_url(server)

        async with utils.request_retry_transient_errors(
                self.aiosession,
                'POST',
                f'{server_url}/s/{self.id}/get',
                json=key) as resp:
            assert resp.status == 200
            return await resp.read()

    def decode(self, byte_array):
        off = 0
        result = []
        while off < len(byte_array):
            n2 = struct.unpack_from("i", byte_array, off)[0]
            off += 4
            result.append(byte_array[off:(off+n2)])
            off += n2
        return result

    async def getmany(self, keys, retry_delay=1):
        servers = collections.defaultdict(list)
        results = [None for _ in keys]
        for i, key in enumerate(keys):
            servers[key[0]].append((key, i))

        def get_from_server(server, keys):
            server_url = self.deploy_config.base_url(server)
            i = 0
            while i < len(keys):
                batch = []
                size = 0
                while i < len(keys):
                    assert keys[i][0][3] < self.max_bufsize
                    if size + keys[i][0][3] < self.max_bufsize:
                        batch.append(keys[i])
                        size += keys[i][0][3]
                        i += 1
                    else:
                        break

                async with utils.request_retry_transient_errors(
                        self.aiosession,
                        'POST',
                        f'{server_url}/s/{self.id}/getmany',
                        json=[x[0] for x in batch]) as resp:
                    assert resp.status == 200
                    data = await resp.read()
                    for v, j in zip(self.decode(data), (x[1] for x in batch)):
                        results[j] = v
        await asyncio.gather(*[get_from_server(server, keys)
                               for server, keys in servers.items()])
        return results

    async def delete(self):
        async with self.aiosession.delete(self.session_url) as resp:
            assert resp.status == 200

    async def get_workers(self):
        async with self.aiosession.get(f'{self.root_url}/w') as resp:
            assert resp.status == 200
            return await resp.json()


class DBufAppender:
    def __init__(self, max_bufsize, aiosession, session_url):
        self.aiosession = aiosession
        self.session_url = session_url
        self.buf = bytearray(max_bufsize)
        self.offs = []
        self.sizes = []
        self.cursor = 0
        self.keys = []

    async def write(self, data):
        n = len(data)
        assert n < len(self.buf)
        if self.cursor + n > len(self.buf):
            await self.flush()
        self.buf[self.cursor:(self.cursor+n)] = data
        self.offs.append(self.cursor)
        self.sizes.append(n)
        self.cursor += n

    async def flush(self):
        if self.cursor == 0:
            return
        buf = self.buf
        offs = self.offs
        sizes = self.sizes
        cursor = self.cursor
        self.buf = bytearray(len(self.buf))
        self.offs = []
        self.sizes = []
        self.cursor = 0

        async with utils.request_retry_transient_errors(
                self.aiosession,
                'POST',
                self.session_url,
                data=buf[0:cursor]) as resp:
            assert resp.status == 200
            server, file_id, pos, _ = await resp.json()
            self.keys.extend([(server, file_id, pos + off, size)
                              for off, size in zip(offs, sizes)])

    async def finish(self):
        await self.flush()
        self.buf = None
        return self.keys
