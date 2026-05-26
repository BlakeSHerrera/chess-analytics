import contextlib
import functools
import hashlib
import os
import pathlib
import shutil
import tempfile
import time
from typing import Callable

from loguru import logger
import requests
from tqdm import tqdm
import wget

from extract import RecordItem


BUFFER_SIZE_BYTES = int(os.environ['BUFFER_SIZE_KB']) * 2 ** 10


@functools.wraps(requests.request)
def request(*args, **kwargs):
    r = requests.request(*args, **kwargs)
    t = 1
    while r.status_code == 429:
        time.sleep(r.headers.get('Retry-After', t))
        r = requests.request(*args, **kwargs)
        t *= 2
        if t >= 256:
            break
    r.raise_for_status()
    return r


def get_checksum(
    path: pathlib.Path, 
    pbar_position: int,
    algorithm: Callable = hashlib.sha256,
) -> str:
    logger.info(f'Checking {algorithm.__name__} on {path}')
    with contextlib.ExitStack() as stack:
        fp = stack.enter_context(
            open(path, 'rb', buffering = BUFFER_SIZE_BYTES))
        fp_progress = stack.enter_context(
            tqdm.wrapattr(
                fp,
                'read',
                total = os.path.getsize(path),
                desc = f'{algorithm.__name__} {path.stem}',
                unit = 'B',
                unit_scale = True,
                unit_divisor = 2 ** 10,
                position = pbar_position,
                leave = False))
        return hashlib.file_digest(fp_progress, algorithm).hexdigest()


def download_item(record: RecordItem, pbar_position: int):
    logger.info(f'Downloading {record.url} to {record.local_path}')
    with contextlib.ExitStack() as stack:
        temp_file = stack.enter_context(
            tempfile.NamedTemporaryFile(mode = 'w+b', dir = './data/cache', delete = False, buffering = BUFFER_SIZE_BYTES))
        response = stack.enter_context(
            request('GET', record.url, stream = True))
        pbar = stack.enter_context(
            tqdm(
                total = int(response.headers.get('Content-Length', '0')),
                desc = f'Download {record.local_path}',
                unit = 'B',
                unit_scale = True,
                unit_divisor = 2 ** 10,
                position = pbar_position,
                leave = False))
        
        for chunk in response.iter_content(BUFFER_SIZE_BYTES):
            temp_file.write(chunk)
            pbar.update(len(chunk))
        pbar.close()

        temp_file.close()
        checksum = get_checksum(pathlib.Path(temp_file.name), pbar_position)
        if checksum != record.checksum:
            msg = f'Invalid checksum after download for {temp_file} - expected {record.checksum} got {checksum}'
            logger.error(msg)
            raise Exception(msg)
        
        shutil.move(temp_file.name, record.local_path)
