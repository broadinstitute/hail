import asyncio
import unittest
import os
import uuid
from memory.client import MemoryClient

from hailtop.aiogoogle.client.storage_client import GoogleStorageAsyncFS
from hailtop.config import get_user_config
from hailtop.utils import async_to_blocking


class BlockingMemoryClient:
    def __init__(self, gcs_project=None, fs=None):
        self._client = MemoryClient(gcs_project, fs)

    def read_file(self, filename):
        return async_to_blocking(self._client.read_file(filename))

    def write_file(self, filename, data):
        return async_to_blocking(self._client.write_file(filename, data))

    def close(self):
        return async_to_blocking(self._client.close())


class Tests(unittest.TestCase):
    def setUp(self):
        bucket_name = get_user_config().get('batch', 'bucket')
        token = uuid.uuid4()
        self.test_path = f'gs://{bucket_name}/memory-tests/{token}'

        self.fs = GoogleStorageAsyncFS(project=os.environ['PROJECT'])
        self.sem = asyncio.Semaphore(50)
        self.client = BlockingMemoryClient(fs=self.fs)
        self.temp_files = set()

    def tearDown(self):
        async def delete_files(url):
            async with self.sem:
                await self.fs.rmtree(self.sem, url)

        async_to_blocking(delete_files(self.test_path))
        self.client.close()

    async def add_temp_file_from_string(self, name: str, str_value: bytes):
        handle = f'{self.test_path}/{name}'
        await self.client.write_file(handle, str_value)
        return handle

    def test_non_existent(self):
        for _ in range(3):
            self.assertIsNone(self.client.read_file(f'{self.test_path}/nonexistent'))

    def test_small_write_around(self):
        async def read(url):
            async with await self.fs.open(url) as f:
                return await f.read()

        cases = [('empty_file', b''), ('null', b'\0'), ('small', b'hello world')]
        for file, data in cases:
            handle = async_to_blocking(self.add_temp_file_from_string(file, data))
            expected = async_to_blocking(read(handle))
            self.assertEqual(expected, data)
            i = 0
            cached = self.client.read_file(handle)
            while cached is None and i < 10:
                cached = self.client.read_file(handle)
                i += 1
            self.assertEqual(cached, expected)

    def test_small_write_through(self):
        cases = [('empty_file2', b''), ('null2', b'\0'), ('small2', b'hello world')]
        for file, data in cases:
            filename = f'{self.test_path}/{file}'
            self.client.write_file(filename, data)
            cached = self.client.read_file(filename)
            self.assertEqual(cached, data)
