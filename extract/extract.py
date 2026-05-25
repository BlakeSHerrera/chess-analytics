import concurrent.futures
import contextlib
import dataclasses
from datetime import datetime
import io
import itertools
import multiprocessing, multiprocessing.managers
import os
import pathlib
import posixpath
import queue
import sqlite3
import time
from typing import Callable, Iterable, Mapping
import urllib.parse

import dotenv
from loguru import logger
import pandas as pd
from tqdm import tqdm
import zstandard

import parse_pgn
import utils


dotenv.load_dotenv(override = True)

PROJECT_FOLDER = pathlib.Path('./data')
CACHE = PROJECT_FOLDER / 'cache'
LOGS = PROJECT_FOLDER / 'logs'
for i in (PROJECT_FOLDER, CACHE, LOGS):
    os.makedirs(i, exist_ok = True)
NUM_WORKERS = int(os.environ['NUM_WORKERS'])

CREATE_TABLE, QUERY_FILE, UPSERT_FILE = (
    pathlib.Path(f'extract/{i}.sql').read_text() for i in 
        ['create_table', 'query_file', 'upsert_file'])

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

    def stream(self, pbar_position: int):
        with contextlib.ExitStack() as stack:
            fp = stack.enter_context(
                open(self.local_path, 'rb', buffering = 2 ** 20))
            fp_progress = stack.enter_context(
                tqdm.wrapattr(
                    fp,
                    'read',
                    total = os.path.getsize(self.local_path),
                    desc = f'{self.file_name}',
                    unit = 'B',
                    unit_scale = True,
                    unit_divisor = utils.BUFFER_SIZE_BYTES,
                    position = pbar_position,
                    leave = False))
            decompressor = stack.enter_context(
                zstandard.ZstdDecompressor().stream_reader(fp_progress))
            text_stream = stack.enter_context(
                io.TextIOWrapper(decompressor, encoding = 'utf-8'))
            return stack.pop_all(), text_stream
        
    def as_tuple(self) -> tuple[str]:
        return (self.url, self.file_name, self.status, self.checksum, str(self.local_path))

    def mark_complete(self, conn: sqlite3.Connection):
        self.status = 'complete'
        conn.execute(UPSERT_FILE, self.as_tuple())
        conn.commit()


class QueueManager(multiprocessing.managers.SyncManager):
    pass
QueueManager.register('PriorityQueue', queue.PriorityQueue)


def main():
    conn = sqlite3.connect(PROJECT_FOLDER / 'raw_metadata.sqlite')
    conn.execute(CREATE_TABLE)
    checksums_file = utils.request('GET', 'https://database.lichess.org/standard/sha256sums.txt').text.strip().split('\n')
    checksums = {file: checksum for checksum, file in map(str.split, checksums_file)}
    file_list = utils.request('GET', 'https://database.lichess.org/standard/list.txt').text.strip().split('\n')
    
    def should_download(file_name: str) -> bool:
        if os.environ['FILE_FILTER'] not in file_name:
            return False
        db_records = conn.execute(QUERY_FILE, [file_name]).fetchall()
        if len(db_records) > 1:
            raise Exception('Primary key violation in metadata table.')
        if not db_records:
            return True
        file = RecordItem(*db_records[0])
        if file.status != 'complete':
            # When is this condition ever false?
            return True
        if checksums[file_name] != file.checksum:
            return True
        return False

    with QueueManager() as manager, \
         concurrent.futures.ProcessPoolExecutor(NUM_WORKERS) as executor:
        futures: dict[concurrent.futures.Future, RecordItem] = dict()
        pbar_queue: queue.PriorityQueue = manager.PriorityQueue()
        for i in range(NUM_WORKERS):
            pbar_queue.put(i + 1)

        for url in file_list:
            file_name = posixpath.basename(urllib.parse.urlparse(url).path)
            if should_download(file_name):
                record = RecordItem(url, file_name, 'incomplete', checksums[file_name], CACHE / file_name)
                futures[executor.submit(worker_main, record, pbar_queue)] = record

        if not futures:
            logger.info('Nothing to do!')
        for future in tqdm(
                concurrent.futures.as_completed(futures), 
                total = len(futures),
                desc = 'Lichess DB', 
                unit = 'files', 
                position = 0, 
                leave = True):
            record: RecordItem = futures[future]
            _ = future.result()
            record.mark_complete(conn)

    clean_cache()


def clean_cache():
    expiry = float(os.environ['CACHE_EXPIRY_SECONDS'])
    for file in filter(pathlib.Path.is_file, CACHE.iterdir()):
        modification_time = file.stat().st_mtime
        age_seconds = time.time() - modification_time
        if age_seconds > expiry:
            logger.info(f'Removing cached file {file}; age is {age_seconds}s, expiry is {expiry}s')
            os.remove(file)


def worker_main(record: RecordItem, pbar_queue: queue.PriorityQueue[int]):
    pbar_position: int = pbar_queue.get()
    try:
        if not os.path.exists(record.local_path):
            utils.download_item(record, pbar_position)
        write_files(record, PROJECT_FOLDER / 'lichess_standard_rated_headers', parse_pgn.parse_headers, pbar_position)
        # Parsing moves with python-chess is painfully slow, ~90 kb/s
        # write_files(record, DATALAKE / 'lichess_standard_rated_moves', parse_pgn.parse_moves)
    finally:
        pbar_queue.put(pbar_position)


def write_files(
        record: RecordItem, 
        path: pathlib.Path, 
        parser: Callable[[io.TextIOBase], Iterable[Mapping]],
        pbar_position: int, 
        n: int = int(os.environ['ROWS_PER_PARQUET'])):
    i = record.file_name.index('-')
    year, month = record.file_name[i - 4:i + 3].split('-')
    path = path / f'year={year}/month={month}'
    os.makedirs(path, exist_ok = True)

    cleanup_stack, stream = record.stream(pbar_position)
    with cleanup_stack:
        for i, batch in enumerate(itertools.batched(parser(stream), n = n)):
            df = pd.DataFrame(batch)
            df['ingest_timestamp'] = datetime.now()
            df.to_parquet(
                path = path / f'{i:04d}.parquet',
                compression = 'zstd')
            logger.info(f'Wrote {path / f"{i:04d}.parquet"}')


if __name__ == '__main__':
    start = datetime.now()
    logger.info('Process initialized.')
    main()
    logger.info(f'Done in {datetime.now() - start}')
