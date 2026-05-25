import functools
import hashlib
import logging
import os
import pathlib
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
    algorithm: Callable = hashlib.sha256
) -> str:
    logging.info(f'Checking {algorithm.__name__} on {path}')
    with open(path, 'rb') as fp:
        with tqdm.wrapattr(
            fp,
            'read',
            total = os.path.getsize(path),
            desc = f'{algorithm.__name__} {path.stem}',
            unit = 'B',
            unit_scale = True,
            unit_divisor = 2 ** 10
        ) as fp_progress:
            return hashlib.file_digest(fp_progress, algorithm).hexdigest()


def download_item(record: RecordItem):
    logging.info(f'Downloading {record.url} to {record.local_path}')
    temp_file = record.local_path.with_suffix(record.local_path.suffix + '.tmp')
    wget.download(record.url, str(temp_file), bar = wget_progress_bar)
    checksum = get_checksum(temp_file)
    if checksum != record.checksum:
        msg = f'Invalid checksum after download for {temp_file} - expected {record.checksum} got {checksum}'
        logging.error(msg)
        raise Exception(msg)
    os.rename(temp_file, record.local_path)
