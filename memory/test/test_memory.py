import concurrent
import os
import unittest
import uuid
from memory.client import MemoryClient

from hailtop.config import get_user_config
from hailtop.google_storage import GCS
from hailtop.utils import async_to_blocking


class BlockingMemoryClient:
    def __init__(self, gcs_project=None, fs=None, deploy_config=None, session=None,
                 headers=None, _token=None):
        self._client = async_to_blocking(MemoryClient(gcs_project, fs, deploy_config, session, headers, _token))

    def _get_file_if_exists(self, filename):
        return async_to_blocking(self._client._get_file_if_exists(filename))

    def read_file(self, filename):
        return async_to_blocking(self._client.read_file(filename))

    def close(self):
        return async_to_blocking(self._client.close())

class Tests(unittest.TestCase):
    def setUp(self):
        bucket_name = get_user_config().get('batch', 'bucket')
        token = uuid.uuid4()
        self.test_path = f'gs://{bucket_name}/memory-tests/{token}'

        self.fs = GCS(concurrent.futures.ThreadPoolExecutor(), project=os.environ['PROJECT'])
        self.client = BlockingMemoryClient(fs=self.fs)
        self.temp_files = set()

    def tearDown(self):
        async_to_blocking(self.fs.delete_gs_files(self.test_path))
        self.client.close()

    def add_temp_file_from_string(self, name: str, str_value: str):
        handle = f'{self.test_path}/{name}'
        self.fs._write_gs_file_from_string(handle, str_value)
        return handle

    def test_non_existent(self):
        for _ in range(3):
            self.assertIsNone(self.client._get_file_if_exists(f'{self.test_path}/nonexistent'))

    def test_small(self):
        cases = [('empty_file', b''), ('null', b'\0'), ('small', b'hello world')]
        for file, data in cases:
            handle = self.add_temp_file_from_string(file, data)
            i = 0
            cached = self.client._get_file_if_exists(handle)
            while cached is None and i < 10:
                cached = self.client._get_file_if_exists(handle)
                i += 1
            self.assertEqual(cached, data)
