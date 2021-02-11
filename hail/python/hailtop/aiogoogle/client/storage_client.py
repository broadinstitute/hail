import os
from typing import Tuple, Any, Set, Optional, Mapping, Dict, AsyncIterator, cast, Type, Iterator
from types import TracebackType
import collections
from multidict import CIMultiDictProxy  # pylint: disable=unused-import
import sys
import logging
import asyncio
import urllib.parse
import aiohttp
from hailtop.utils import TransientError, retry_transient_errors
from hailtop.aiotools import (
    FileStatus, FileListEntry, ReadableStream, WritableStream, AsyncFS,
    FeedableAsyncIterable, FileAndDirectoryError, MultiPartCreate)

from hailtop.aiogoogle.auth import Session
from .base_client import BaseClient

log = logging.getLogger(__name__)


class PageIterator:
    def __init__(self, client: 'BaseClient', path: str, request_kwargs: Mapping[str, Any]):
        if 'params' in request_kwargs:
            request_params = request_kwargs['params']
            del request_kwargs['params']
        else:
            request_params = {}
        self._client = client
        self._path = path
        self._request_params = request_params
        self._request_kwargs = request_kwargs
        self._page = None

    def __aiter__(self) -> 'PageIterator':
        return self

    async def __anext__(self):
        if self._page is None:
            assert 'pageToken' not in self._request_params
            self._page = await self._client.get(self._path, params=self._request_params, **self._request_kwargs)
            return self._page

        next_page_token = self._page.get('nextPageToken')
        if next_page_token is not None:
            self._request_params['pageToken'] = next_page_token
            self._page = await self._client.get(self._path, params=self._request_params, **self._request_kwargs)
            return self._page

        raise StopAsyncIteration


class InsertObjectStream(WritableStream):
    def __init__(self, it, request_task):
        super().__init__()
        self._it = it
        self._request_task = request_task
        self._value = None

    async def write(self, b):
        assert not self.closed
        await self._it.feed(b)
        return len(b)

    async def _wait_closed(self):
        await self._it.stop()
        async with await self._request_task as resp:
            self._value = await resp.json()


class _WriteBuffer:
    def __init__(self):
        """Long writes that might fail need to be broken into smaller chunks
        that can be retried.  _WriteBuffer stores data at the end of
        the write stream that has not been committed and may be needed
        to retry the failed write of a chunk."""
        self._buffers = collections.deque()
        self._offset = 0
        self._size = 0
        self._iterating = False

    def append(self, b: bytes):
        """`b` can be any length"""
        self._buffers.append(b)
        self._size += len(b)

    def size(self) -> int:
        """Return the total number of bytes stored in the write buffer.  This
        is the sum of the length of the bytes in `_buffers`."""
        return self._size

    def offset(self) -> int:
        """Return the offset in the write stream of the first byte in the
        write buffer."""
        return self._offset

    def advance_offset(self, new_offset: int):
        """Inform the write buffer that bytes before `new_offset` have been
        committed and can be discarded.  After calling advance_offset,
        `self.offset() == new_offset`."""
        assert not self._iterating
        assert new_offset <= self._offset + self._size
        while self._buffers and new_offset >= self._offset + len(self._buffers[0]):
            b = self._buffers.popleft()
            n = len(b)
            self._offset += n
            self._size -= n
        if new_offset > self._offset:
            n = new_offset - self._offset
            b = self._buffers[0]
            assert n < len(b)
            b = b[n:]
            self._buffers[0] = b
            self._offset += n
            self._size -= n
        assert self._offset == new_offset

    def chunks(self, chunk_size: int) -> Iterator[bytes]:
        """Return an iterator that yields bytes whose total size is
        `chunk_size` from the beginning of the write buffer."""
        assert not self._iterating
        self._iterating = True
        remaining = chunk_size
        i = 0
        while remaining > 0:
            b = self._buffers[i]
            n = len(b)
            if n <= remaining:
                yield b
                remaining -= n
                i += 1
            else:
                yield b[:remaining]
                break
        self._iterating = False


class _TaskManager:
    def __init__(self, coro):
        self._coro = coro
        self._task = None

    async def __aenter__(self) -> '_TaskManager':
        self._task = asyncio.create_task(self._coro)
        return self._task

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_val: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> None:
        if not self._task.done():
            if exc_val:
                self._task.cancel()
                try:
                    await self._task
                except:
                    _, exc, _ = sys.exc_info()
                    if exc is not exc_val:
                        log.warning('dropping preempted task exception', exc_info=True)
            else:
                await self._task


class ResumableInsertObjectStream(WritableStream):
    def __init__(self, session: Session, session_url: str, chunk_size: int):
        super().__init__()
        self._session = session
        self._session_url = session_url
        self._write_buffer = _WriteBuffer()
        self._broken = False
        self._done = False
        self._chunk_size = chunk_size

    @staticmethod
    def _range_upper(range):
        range = range.split('/', 1)[0]
        split_range = range.split('-')
        assert len(split_range) == 2
        return int(split_range[1])

    async def _write_chunk_1(self):
        if self._closed:
            total_size = self._write_buffer.offset() + self._write_buffer.size()
            total_size_str = str(total_size)
        else:
            total_size = None
            total_size_str = '*'

        if self._broken:
            # If the last request was unsuccessful, we are out of sync
            # with the server and we don't know what byte to send
            # next.  Perform a status check to find out.  See:
            # https://cloud.google.com/storage/docs/performing-resumable-uploads#status-check

            # note: this retries
            resp = await self._session.put(self._session_url,
                                           headers={
                                               'Content-Length': '0',
                                               'Content-Range': f'bytes */{total_size_str}'
                                           },
                                           raise_for_status=False)
            if resp.status >= 200 and resp.status < 300:
                assert self._closed
                assert total_size is not None
                self._write_buffer.advance_offset(total_size)
                assert self._write_buffer.size() == 0
                self._done = True
                return
            if resp.status == 308:
                range = resp.headers.get('Range')
                if range is not None:
                    new_offset = self._range_upper(range) + 1
                else:
                    new_offset = 0
                self._write_buffer.advance_offset(new_offset)
                self._broken = False
            else:
                assert resp.status >= 400
                resp.raise_for_status()
                assert False

        assert not self._broken
        self._broken = True

        offset = self._write_buffer.offset()
        if self._closed:
            n = self._write_buffer.size()
        else:
            assert self._write_buffer.size() >= self._chunk_size
            n = self._chunk_size
        if n > 0:
            range = f'bytes {offset}-{offset + n - 1}/{total_size_str}'
        else:
            range = f'bytes */{total_size_str}'

        # Upload a single chunk.  See:
        # https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-upload
        it: FeedableAsyncIterable[bytes] = FeedableAsyncIterable()
        async with _TaskManager(
                self._session.put(self._session_url,
                                  data=aiohttp.AsyncIterablePayload(it),
                                  headers={
                                      'Content-Length': f'{n}',
                                      'Content-Range': range
                                  },
                                  raise_for_status=False,
                                  retry=False)) as put_task:
            for chunk in self._write_buffer.chunks(n):
                async with _TaskManager(it.feed(chunk)) as feed_task:
                    done, _ = await asyncio.wait([put_task, feed_task], return_when=asyncio.FIRST_COMPLETED)
                    if feed_task not in done:
                        msg = 'resumable upload chunk PUT request finished before writing data'
                        log.warning(msg)
                        raise TransientError(msg)

            await it.stop()

            resp = await put_task
            if resp.status >= 200 and resp.status < 300:
                assert self._closed
                assert total_size is not None
                self._write_buffer.advance_offset(total_size)
                assert self._write_buffer.size() == 0
                self._done = True
                return

            if resp.status == 308:
                range = resp.headers['Range']
                self._write_buffer.advance_offset(self._range_upper(range) + 1)
                self._broken = False
                return

            assert resp.status >= 400
            resp.raise_for_status()
            assert False

    async def _write_chunk(self):
        await retry_transient_errors(self._write_chunk_1)

    async def write(self, b):
        assert not self._closed
        assert self._write_buffer.size() < self._chunk_size
        self._write_buffer.append(b)
        while self._write_buffer.size() >= self._chunk_size:
            await self._write_chunk()
        assert self._write_buffer.size() < self._chunk_size

    async def _wait_closed(self):
        assert self._closed
        assert self._write_buffer.size() < self._chunk_size
        while not self._done:
            await self._write_chunk()
        assert self._done and self._write_buffer.size() == 0


class GetObjectStream(ReadableStream):
    def __init__(self, resp):
        super().__init__()
        self._resp = resp
        self._content = resp.content

    async def read(self, n: int = -1) -> bytes:
        assert not self._closed
        return await self._content.read(n)

    def headers(self) -> 'CIMultiDictProxy[str]':
        return self._resp.headers

    async def _wait_closed(self) -> None:
        self._content = None
        self._resp.release()
        self._resp = None


class StorageClient(BaseClient):
    def __init__(self, **kwargs):
        super().__init__('https://storage.googleapis.com/storage/v1', **kwargs)

    # docs:
    # https://cloud.google.com/storage/docs/json_api/v1

    async def insert_object(self, bucket: str, name: str, **kwargs) -> InsertObjectStream:
        # Insert an object.  See:
        # https://cloud.google.com/storage/docs/json_api/v1/objects/insert
        if 'params' in kwargs:
            params = kwargs['params']
        else:
            params = {}
            kwargs['params'] = params
        assert 'name' not in params
        params['name'] = name

        if 'data' in params:
            return await self._session.post(
                f'https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o',
                **kwargs)

        upload_type = params.get('uploadType')
        if not upload_type:
            upload_type = 'resumable'
            params['uploadType'] = upload_type

        if upload_type == 'media':
            it: FeedableAsyncIterable[bytes] = FeedableAsyncIterable()
            kwargs['data'] = aiohttp.AsyncIterablePayload(it)
            request_task = asyncio.ensure_future(self._session.post(
                f'https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o',
                retry=False,
                **kwargs))
            return InsertObjectStream(it, request_task)

        # Write using resumable uploads.  See:
        # https://cloud.google.com/storage/docs/performing-resumable-uploads
        assert upload_type == 'resumable'
        chunk_size = kwargs.get('bufsize', 256 * 1024)
        resp = await self._session.post(
            f'https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o',
            **kwargs)
        session_url = resp.headers['Location']
        return ResumableInsertObjectStream(self._session, session_url, chunk_size)

    async def get_object(self, bucket: str, name: str, **kwargs) -> GetObjectStream:
        if 'params' in kwargs:
            params = kwargs['params']
        else:
            params = {}
            kwargs['params'] = params
        assert 'alt' not in params
        params['alt'] = 'media'

        resp = await self._session.get(
            f'https://storage.googleapis.com/storage/v1/b/{bucket}/o/{urllib.parse.quote(name, safe="")}', **kwargs)
        return GetObjectStream(resp)

    async def get_object_metadata(self, bucket: str, name: str, **kwargs) -> Dict[str, str]:
        params = kwargs.get('params')
        assert not params or 'alt' not in params
        return cast(Dict[str, str], await self.get(f'/b/{bucket}/o/{urllib.parse.quote(name, safe="")}', **kwargs))

    async def delete_object(self, bucket: str, name: str, **kwargs) -> None:
        await self.delete(f'/b/{bucket}/o/{urllib.parse.quote(name, safe="")}', **kwargs)

    async def list_objects(self, bucket: str, **kwargs) -> PageIterator:
        return PageIterator(self, f'/b/{bucket}/o', kwargs)


class GetObjectFileStatus(FileStatus):
    def __init__(self, items: Dict[str, str]):
        self._items = items

    async def size(self) -> int:
        return int(self._items['size'])

    async def __getitem__(self, key: str) -> str:
        return self._items[key]


class GoogleStorageFileListEntry(FileListEntry):
    def __init__(self, url: str, items: Optional[Dict[str, Any]]):
        assert url.endswith('/') == (items is None), f'{url} {items}'
        self._url = url
        self._items = items
        self._status = None

    def name(self) -> str:
        parsed = urllib.parse.urlparse(self._url)
        return os.path.basename(parsed.path)

    async def url(self) -> str:
        return self._url

    def url_maybe_trailing_slash(self) -> str:
        return self._url

    async def is_file(self) -> bool:
        return self._items is not None

    async def is_dir(self) -> bool:
        return self._items is None

    async def status(self) -> FileStatus:
        if self._status is None:
            if self._items is None:
                raise ValueError("directory has no file status")
            self._status = GetObjectFileStatus(self._items)
        return self._status


class GoogleStorageMultiPartCreate(MultiPartCreate):
    def __init__(self, fs, url, num_parts):
        self._fs = fs
        self._url = url
        self._num_parts = num_parts

    async def create_part(self, number: int, start: int):
        raise NotImplementedError

    async def __aenter__(self) -> 'GoogleStorageMultiPartCreate':
        raise NotImplementedError

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_val: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> None:
        raise NotImplementedError


class GoogleStorageAsyncFS(AsyncFS):
    def __init__(self, storage_client: Optional[StorageClient] = None):
        if not storage_client:
            storage_client = StorageClient()
        self._storage_client = storage_client

    def schemes(self) -> Set[str]:
        return {'gs'}

    @staticmethod
    def _get_bucket_name(url: str) -> Tuple[str, str]:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme != 'gs':
            raise ValueError(f"invalid scheme, expected gs: {parsed.scheme}")

        name = parsed.path
        if name:
            assert name[0] == '/'
            name = name[1:]

        return (parsed.netloc, name)

    async def open(self, url: str) -> ReadableStream:
        bucket, name = self._get_bucket_name(url)
        return await self._storage_client.get_object(bucket, name)

    async def create(self, url: str) -> WritableStream:
        bucket, name = self._get_bucket_name(url)
        return await self._storage_client.insert_object(bucket, name)

    async def multi_part_create(self, url: str, num_parts: int) -> GoogleStorageMultiPartCreate:
        return GoogleStorageMultiPartCreate(self, url, num_parts)

    async def staturl(self, url: str) -> str:
        assert not url.endswith('/')

        async def with_exception(f, *args, **kwargs):
            try:
                return (await f(*args, **kwargs)), None
            except Exception as e:
                return None, e

        [(is_file, isfile_exc), (is_dir, isdir_exc)] = await asyncio.gather(
            with_exception(self.isfile, url), with_exception(self.isdir, url + '/'))
        # raise exception deterministically
        if isfile_exc:
            raise isfile_exc
        if isdir_exc:
            raise isdir_exc

        if is_file:
            if is_dir:
                raise FileAndDirectoryError(url)
            return AsyncFS.FILE

        if is_dir:
            return AsyncFS.DIR

        raise FileNotFoundError(url)

    async def mkdir(self, url: str) -> None:
        pass

    async def makedirs(self, url: str, exist_ok: bool = False) -> None:
        pass

    async def statfile(self, url: str) -> GetObjectFileStatus:
        try:
            bucket, name = self._get_bucket_name(url)
            return GetObjectFileStatus(await self._storage_client.get_object_metadata(bucket, name))
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                raise FileNotFoundError(url) from e
            raise

    async def _listfiles_recursive(self, bucket: str, name: str) -> AsyncIterator[FileListEntry]:
        assert name.endswith('/')
        params = {
            'prefix': name
        }
        async for page in await self._storage_client.list_objects(bucket, params=params):
            prefixes = page.get('prefixes')
            assert not prefixes

            items = page.get('items')
            if items is not None:
                for item in page['items']:
                    yield GoogleStorageFileListEntry(f'gs://{bucket}/{item["name"]}', item)

    async def _listfiles_flat(self, bucket: str, name: str) -> AsyncIterator[FileListEntry]:
        assert name.endswith('/')
        params = {
            'prefix': name,
            'delimiter': '/',
            'includeTrailingDelimiter': 'true'
        }
        async for page in await self._storage_client.list_objects(bucket, params=params):
            prefixes = page.get('prefixes')
            if prefixes:
                for prefix in prefixes:
                    assert prefix.endswith('/')
                    url = f'gs://{bucket}/{prefix}'
                    yield GoogleStorageFileListEntry(url, None)

            items = page.get('items')
            if items:
                for item in page['items']:
                    yield GoogleStorageFileListEntry(f'gs://{bucket}/{item["name"]}', item)

    async def listfiles(self, url: str, recursive: bool = False) -> AsyncIterator[FileListEntry]:
        bucket, name = self._get_bucket_name(url)
        if not name.endswith('/'):
            name = f'{name}/'

        if recursive:
            it = self._listfiles_recursive(bucket, name)
        else:
            it = self._listfiles_flat(bucket, name)

        it = it.__aiter__()
        try:
            first_entry = await it.__anext__()
        except StopAsyncIteration:
            raise FileNotFoundError(url)  # pylint: disable=raise-missing-from

        async def cons(first_entry, it):
            yield first_entry
            try:
                while True:
                    yield await it.__anext__()
            except StopAsyncIteration:
                pass

        return cons(first_entry, it)

    async def isfile(self, url: str) -> bool:
        try:
            bucket, name = self._get_bucket_name(url)
            await self._storage_client.get_object_metadata(bucket, name)
            return True
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return False
            raise

    async def isdir(self, url: str) -> bool:
        bucket, name = self._get_bucket_name(url)
        assert name.endswith('/')
        params = {
            'prefix': name,
            'delimiter': '/',
            'includeTrailingDelimiter': 'true',
            'maxResults': 1
        }
        async for page in await self._storage_client.list_objects(bucket, params=params):
            prefixes = page.get('prefixes')
            items = page.get('items')
            return prefixes or items
        assert False  # unreachable

    async def remove(self, url: str) -> None:
        bucket, name = self._get_bucket_name(url)
        await self._storage_client.delete_object(bucket, name)

    async def rmtree(self, url: str) -> None:
        try:
            async for entry in await self.listfiles(url, recursive=True):
                await self.remove(await entry.url())
        except FileNotFoundError:
            pass

    async def close(self) -> None:
        await self._storage_client.close()
        self._storage_client = None
