import logging
import asyncio

from .google_storage import GCS
from .spec_writer import SpecWriter

log = logging.getLogger('logstore')


class LogStore:
    def __init__(self, bucket_name, instance_id, blocking_pool, *, project=None, credentials=None):
        self.bucket_name = bucket_name
        self.instance_id = instance_id
        self.log_root = f'gs://{bucket_name}/batch/logs/{instance_id}'
        self.gcs = GCS(blocking_pool, project=project, credentials=credentials)

    def worker_log_path(self, machine_name, log_file):
        # this has to match worker startup-script
        return f'{self.log_root}/worker/{machine_name}/{log_file}'

    def batch_log_dir(self, batch_id):
        return f'{self.log_root}/batch/{batch_id}'

    def log_path(self, format_version, batch_id, job_id, attempt_id, task):
        if format_version == 1:
            return f'{self.batch_log_dir(batch_id)}/{job_id}/{task}/log'

        return f'{self.batch_log_dir(batch_id)}/{job_id}/{attempt_id}/{task}/log'

    async def read_log_file(self, format_version, batch_id, job_id, attempt_id, task):
        path = self.log_path(format_version, batch_id, job_id, attempt_id, task)
        return await self.gcs.read_gs_file(path)

    async def write_log_file(self, format_version, batch_id, job_id, attempt_id, task, data):
        path = self.log_path(format_version, batch_id, job_id, attempt_id, task)
        return await self.gcs.write_gs_file(path, data)

    async def delete_batch_logs(self, batch_id):
        await self.gcs.delete_gs_files(
            self.batch_log_dir(batch_id))

    def status_path(self, batch_id, job_id, attempt_id):
        return f'{self.batch_log_dir(batch_id)}/{job_id}/{attempt_id}/status.json'

    async def read_status_file(self, batch_id, job_id, attempt_id):
        path = self.status_path(batch_id, job_id, attempt_id)
        return await self.gcs.read_gs_file(path)

    async def write_status_file(self, batch_id, job_id, attempt_id, status):
        path = self.status_path(batch_id, job_id, attempt_id)
        return await self.gcs.write_gs_file(path, status)

    async def delete_status_file(self, batch_id, job_id, attempt_id):
        path = self.status_path(batch_id, job_id, attempt_id)
        return await self.gcs.delete_gs_file(path)

    def specs_dir(self, batch_id, token):
        return f'{self.log_root}/batch/{batch_id}/bunch/{token}'

    def specs_path(self, batch_id, token):
        return f'{self.specs_dir(batch_id, token)}/specs.json'

    def specs_index_path(self, batch_id, token):
        return f'{self.specs_dir(batch_id, token)}/specs.idx'

    async def read_spec_file(self, batch_id, token, start_job_id, job_id):
        idx_path = self.specs_index_path(batch_id, token)
        idx_start, idx_end = SpecWriter.get_index_file_offsets(job_id, start_job_id)
        offsets = await self.gcs.read_binary_gs_file(idx_path, start=idx_start, end=idx_end)

        spec_path = self.specs_path(batch_id, token)
        spec_start, spec_end = SpecWriter.read_spec_file_offsets(offsets)
        return await self.gcs.read_gs_file(spec_path, start=spec_start, end=spec_end)

    async def write_spec_file(self, batch_id, token, data_bytes, offsets_bytes):
        idx_path = self.specs_index_path(batch_id, token)
        write1 = self.gcs.write_gs_file(idx_path, offsets_bytes,
                                        content_type='application/octet-stream')

        specs_path = self.specs_path(batch_id, token)
        write2 = self.gcs.write_gs_file(specs_path, data_bytes)

        await asyncio.gather(write1, write2)

    async def delete_spec_file(self, batch_id, token):
        await self.gcs.delete_gs_files(self.specs_dir(batch_id, token))
