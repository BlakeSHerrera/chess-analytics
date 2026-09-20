import contextlib
import functools
import hashlib
import io
import os
import pathlib
import shutil
import tempfile
import time

from loguru import logger
import requests
from tqdm import tqdm
import zstandard

from extract_lichess import RecordItem


BUFFER_SIZE_BYTES = int(os.environ['BUFFER_SIZE_KB']) * 2 ** 10


@functools.wraps(requests.request)
def request(*args, **kwargs) -> requests.Response:
    r = requests.request(*args, **kwargs)
    t = 1
    while r.status_code == 429:
        # Too many requests
        time.sleep(r.headers.get('Retry-After', t))
        r = requests.request(*args, **kwargs)
        t *= 2
        if t >= 256:
            break
    r.raise_for_status()
    return r


def stream(record: RecordItem):
    dctx = zstandard.ZstdDecompressor()
    with contextlib.ExitStack() as stack:

        cached = os.path.exists(record.local_path)
        if cached:
            byte_stream = stack.enter_context(
                open(record.local_path, 'rb', buffering = BUFFER_SIZE_BYTES))
            total = os.path.getsize(record.local_path)
        else:
            response = request('GET', record.url, stream = True)
            byte_stream = stack.enter_context(
                StreamHashCache(
                    response.raw, 
                    record.checksum, 
                    record.local_path))
            total = int(response.headers.get('Content-Length', '0'))

        progress_wrapper = stack.enter_context(
            tqdm.wrapattr(
                byte_stream,
                'read',
                total = total,
                desc = f'{"Cached" if cached else "Download"} {record.file_name}',
                unit = 'B',
                unit_scale = True,
                unit_divisor = 2 ** 10,
                leave = False))
        decompressor = stack.enter_context(
            dctx.stream_reader(progress_wrapper))
        text_stream = stack.enter_context(
            io.TextIOWrapper(decompressor, encoding = 'utf-8'))
        return stack.pop_all(), text_stream
        

class StreamHashCache:

    def __init__(self, stream: io.IOBase, expected_checksum: str, file_path: pathlib.Path):
        self.stream = stream
        self.expected_checksum = expected_checksum
        self.hasher = hashlib.sha256()
        self.file_path = file_path
    
    def __enter__(self):
        self.temp_file = tempfile.NamedTemporaryFile(
            mode = 'w+b', 
            dir = './data/cache', 
            delete = False, 
            buffering = BUFFER_SIZE_BYTES
        ).__enter__()
        return self

    def __exit__(self, *args):
        self.temp_file.__exit__(None, None, None)
        checksum = self.hasher.hexdigest()
        if checksum != self.expected_checksum:
            msg = f'Invalid checksum after download for {self.temp_file} - expected {self.expected_checksum} got {checksum}'
            logger.error(msg)
            raise Exception(msg)
        shutil.move(self.temp_file.name, self.file_path)
        return False

    def read(self, size = -1):
        chunk = self.stream.read(size)
        self.hasher.update(chunk)
        self.temp_file.write(chunk)
        return chunk
