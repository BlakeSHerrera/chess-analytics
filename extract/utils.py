import contextlib
import functools
import hashlib
import logging
import os
import pathlib
import shutil
import tempfile
from typing import Callable

import requests
from tqdm import tqdm
import wget

from extract import RecordItem


@functools.wraps(requests.request)
def request(*args, **kwargs):
    r = requests.request(*args, **kwargs)
    r.raise_for_status()
    return r

def wget_progress_bar(current, total, width = 80):
    return wget.bar_adaptive(round(current / 2 ** 20, 1), round(total / 2 ** 20, 1), width) + ' MB'


def get_checksum(
    path: pathlib.Path, 
    pbar_position: int,
    algorithm: Callable = hashlib.sha256,
) -> str:
    logging.info(f'Checking {algorithm.__name__} on {path}')
    with contextlib.ExitStack() as stack:
        fp = stack.enter_context(
            open(path, 'rb'))
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
    logging.info(f'Downloading {record.url} to {record.local_path}')
    with contextlib.ExitStack() as stack:
        temp_file = stack.enter_context(
            tempfile.NamedTemporaryFile(mode = 'w+b', delete = False))
        response = stack.enter_context(
            requests.get(record.url, stream = True))
        pbar = stack.enter_context(
            tqdm(
                total = int(response.headers.get('Content-Length', '0')),
                desc = f'Download {record.local_path}',
                unit = 'B',
                unit_scale = True,
                unit_divisor = 2 ** 10,
                position = pbar_position,
                leave = False))
        
        response.raise_for_status()
        for chunk in response.iter_content(2 ** 20):
            temp_file.write(chunk)
            pbar.update(len(chunk))
        pbar.close()

        checksum = get_checksum(pathlib.Path(temp_file.name), pbar_position)
        if checksum != record.checksum:
            msg = f'Invalid checksum after download for {temp_file} - expected {record.checksum} got {checksum}'
            logging.error(msg)
            raise Exception(msg)
        temp_file.close()
        shutil.move(temp_file.name, record.local_path)
