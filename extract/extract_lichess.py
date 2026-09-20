import dataclasses
from datetime import datetime
import functools
import io
import itertools
import os
import pathlib
import posixpath
import shutil
import sqlite3
import time
from typing import Callable, Iterable, Mapping
import urllib.parse

import dotenv
from loguru import logger
import polars as pl
from tqdm import tqdm

import parse_pgn
import utils


dotenv.load_dotenv(override = True)

PROJECT_FOLDER = pathlib.Path('./data')
CACHE = PROJECT_FOLDER / 'cache'
LOGS = PROJECT_FOLDER / 'logs'
for i in (PROJECT_FOLDER, CACHE, LOGS, PROJECT_FOLDER / 'tmp'):
    os.makedirs(i, exist_ok = True)

@functools.cache
def sql(file: str):
    return pathlib.Path(f'extract/lichess/{file}.sql').read_text()

TARGET_FILE_SIZE_BYTES = int(os.environ['TARGET_FILE_SIZE_MB']) * 2 ** 20
ROW_BUFFER_SIZE = int(os.environ['ROW_BUFFER_SIZE'])

logger.remove()
logger.add(
    LOGS / 'extract.log', 
    level = os.environ['LOG_LEVEL'], 
    rotation = os.environ['LOG_ROTATION'], 
    enqueue = True)


@dataclasses.dataclass
class RecordItem:

    url: str
    file_name: str
    status: str
    checksum: str
    local_path: str | pathlib.Path

    def as_tuple(self) -> tuple[str]:
        return (self.url, self.file_name, self.status, self.checksum, str(self.local_path))


def main():
    conn = sqlite3.connect(PROJECT_FOLDER / 'raw_metadata.sqlite', autocommit = True)
    conn.execute(sql('create_table'))
    
    checksums_file = utils.request('GET', 'https://database.lichess.org/standard/sha256sums.txt').text.strip().split('\n')
    checksums = {file: checksum for checksum, file in map(str.split, checksums_file)}
    file_list = utils.request('GET', 'https://database.lichess.org/standard/list.txt').text.strip().split('\n')
    
    def should_download(file_name: str) -> bool:
        if os.environ['FILE_FILTER'] not in file_name:
            return False
        db_records = conn.execute(sql('query_file'), [file_name]).fetchall()
        if len(db_records) > 1:
            msg = 'Primary key violation in metadata table.'
            logger.exception(msg)
            raise Exception(msg)
        if not db_records:
            return True
        file = RecordItem(*db_records[0])
        if file.status != 'complete':
            # When is this condition ever false?
            return True
        if checksums[file_name] != file.checksum:
            return True
        return False


    clean_cache()

    todo = list()
    for url in file_list:
        file_name = posixpath.basename(urllib.parse.urlparse(url).path)
        if should_download(file_name):
            record = RecordItem(url, file_name, 'incomplete', checksums[file_name], CACHE / file_name)
            todo.append(record)

    for record in tqdm(todo):
        write_files(record, PROJECT_FOLDER / 'lichess_standard_rated_headers', parse_pgn.parse_headers)
        record.status = 'complete'
        conn.execute(sql('upsert_file'), record.as_tuple())
    
    if not todo:
        logger.info('Nothing to do!')

def clean_cache():
    expiry = float(os.environ['CACHE_EXPIRY_SECONDS'])
    for i in CACHE.rglob('*.parquet'):
        i.unlink()
    for file in filter(pathlib.Path.is_file, CACHE.iterdir()):
        modification_time = file.stat().st_mtime
        age_seconds = time.time() - modification_time
        if age_seconds > expiry:
            logger.info(f'Removing cached file {file}; age is {age_seconds}s, expiry is {expiry}s')
            file.unlink()


def write_files(
    record: RecordItem, 
    path: pathlib.Path, 
    parser: Callable[[io.TextIOBase], Iterable[Mapping]]
):
    j = record.file_name.index('-')
    year, month = record.file_name[j - 4:j + 3].split('-')
    suffix = f'year={year}/month={month}'
    path = path / suffix
    os.makedirs(path, exist_ok = True)
    temp = CACHE
    parquet_kwargs = {
        'compression': 'zstd',
        'mkdir': True
    }

    def compact(df: pl.LazyFrame, suffix: str):
        df = df.with_columns(ingest_timestamp = datetime.now())
        df.sink_parquet(temp / suffix, **parquet_kwargs)
        shutil.move(temp / suffix, path / suffix)

    cleanup_stack, stream = utils.stream(record)
    batches = itertools.batched(parser(stream), n = ROW_BUFFER_SIZE)
    with cleanup_stack:
        for i in itertools.count():
            df = pl.LazyFrame()
            size = 0

            for j, batch in enumerate(batches):
                file = temp / f'{j:04d}.parquet'
                pl.DataFrame(batch).write_parquet(file, **parquet_kwargs)
                df = pl.concat(
                    [df, pl.scan_parquet(file)], 
                    how = 'diagonal_relaxed')
                logger.info(f'Wrote {len(batch)} to {path / f"{j:04d}.parquet"}')
                size += file.stat().st_size
                if size >= TARGET_FILE_SIZE_BYTES:
                    break
            else:
                compact(df, f'{i:03d}.parquet')
                break
            compact(df, f'{i:03d}.parquet')


if __name__ == '__main__':
    start = datetime.now()
    logger.info('Process initialized.')
    main()
    logger.info(f'Done in {datetime.now() - start}')
