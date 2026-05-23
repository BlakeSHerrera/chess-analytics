import dataclasses
from datetime import datetime
import io
import itertools
import logging
import os
import pathlib
import posixpath
import re
import sqlite3
import time
from typing import Iterable
import urllib.parse

import dotenv
import pandas as pd
from tqdm import tqdm
import zstandard

import utils


EPOCH = datetime.now()
DATALAKE = pathlib.Path(os.environ['DATALAKE_PATH'])
CACHE = DATALAKE / 'cache'
os.makedirs(CACHE, exist_ok = True)
TAG_PATTERN = re.compile(r'\[(\w+) "(.*)"\]')

CREATE_TABLE, QUERY_FILE, UPSERT_FILE = (
    pathlib.Path(f'src/{i}.sql').read_text() for i in 
        ['create_table', 'query_file', 'upsert_file'])


@dataclasses.dataclass
class RecordItem:
    url: str
    file_name: str
    status: str
    checksum: str
    local_path: str | pathlib.Path


def main():
    conn = sqlite3.connect(DATALAKE / 'db.sqlite')
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

    todo: list[RecordItem] = []
    for url in tqdm(file_list, desc = 'Lichess DB', unit = 'files'):
        file_name = posixpath.basename(urllib.parse.urlparse(url).path)
        if should_download(file_name):
            todo.append(RecordItem(url, file_name, 'incomplete', checksums[file_name], CACHE / file_name))
    
    # TODO add parallelism via map-reduce paradigm
    for i in todo:
        worker_main(i)
        conn.execute(UPSERT_FILE, [i.url, i.file_name, 'complete', i.checksum, i.local_path])
    clean_cache()


def clean_cache():
    expiry = float(os.environ['CACHE_EXPIRY_SECONDS'])
    for file in filter(pathlib.Path.is_file, CACHE.iterdir()):
        modification_time = file.stat().st_mtime
        age_seconds = time.time() - modification_time
        if age_seconds > expiry:
            logging.info(f'Removing cached file {file}; age is {age_seconds}s, expiry is {expiry}s')
            os.remove(file)


def worker_main(record: RecordItem):
    if not os.path.exists(record.local_path):
        utils.download_item(record)
    i = record.file_name.index('-')
    year, month = record.file_name[i - 4:i + 2].split('-')
    path = DATALAKE / f'lichess_db_standard_rated/year={year}/month={month}'
    os.makedirs(path, exist_ok=True)

    for i, batch in enumerate(itertools.batched(parse(record), n = 5_000_000)):
        df = pd.DataFrame(batch)
        df['ingest_timestamp'] = EPOCH
        df.to_parquet(
            path = path / f'{i:04d}.parquet', 
            compression = 'zstd')


def parse(record: RecordItem) -> Iterable[dict]:
    decompressor = zstandard.ZstdDecompressor()
    with open(record.local_path, 'rb') as fp, \
         tqdm.wrapattr(
             fp,
             'read',
             total = os.path.getsize(record.local_path),
             desc = f'{record.file_name}',
             unit = 'B',
             unit_scale = True,
             unit_divisor = 2 ** 10
         ) as fp_progress, \
         decompressor.stream_reader(fp_progress) as stream:
        
        text_stream = io.TextIOWrapper(stream, encoding = 'utf-8')
        tags = dict()
        for line in map(str.strip, text_stream):
            if line:
                i = line.index(' ')
                tags[line[1:i]] = line[i + 2:-2]
            else:
                pgn = next(text_stream).strip()
                # tags['PGN'] = pgn
                next(text_stream)
                yield tags
                tags = dict()

if __name__ == '__main__':
    dotenv.load_dotenv(override = True)
    main()
