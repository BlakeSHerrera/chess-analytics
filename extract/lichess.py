import dataclasses
from datetime import datetime, timedelta
import functools
import hashlib
import io
import itertools
import os
import pathlib
import posixpath
import re
import shutil
import sqlite3
import time
from typing import Callable, Iterable, Mapping
import urllib.parse

import dagster as dg
import dotenv
import polars as pl
import zstandard

from . import parse_pgn
from . import utils


dotenv.load_dotenv(override = True)

ROW_BUFFER_SIZE = int(os.environ['ROW_BUFFER_SIZE'])
TARGET_FILE_SIZE_BYTES = int(os.environ['TARGET_FILE_SIZE_MB']) * 2 ** 20

DATA = pathlib.Path('/data')
LOGS = DATA / 'logs'

for i in (DATA, LOGS):
    os.makedirs(i, exist_ok = True)

lichess_games_partitions = dg.DynamicPartitionsDefinition(name = 'lichess_games_partitions')
VARIANTS = ['standard', 'antichess', 'atomic', 'chess960', 'crazyhouse', 'horde', 'kingOfTheHill', 'racingKings', 'threeCheck', 'broadcast']
BASE_URL = 'https://database.lichess.org/'
DOWNLOAD_URL = BASE_URL + '{variant}/lichess_db_{variant}_rated_{year_month}.pgn.zst'
LIST_URL = BASE_URL + '{variant}/list.txt'
CHECKSUM_URL = BASE_URL + '{variant}/sha256sums.txt'
PUZZLE_URL = BASE_URL + 'lichess_db_puzzle.csv.zst'
EVAL_URL = BASE_URL + 'lichess_db_eval.jsonl.zst'
OPENINGS_GITHUB = 'https://github.com/lichess-org/chess-openings'
PARTITION_SEPARATOR = '|'

@dg.asset(partitions_def = lichess_games_partitions)
def lichess_games_raw(context: dg.AssetExecutionContext):
    variant, year_month = context.partition_key.split(PARTITION_SEPARATOR)
    url = DOWNLOAD_URL.format(variant = variant, year_month = year_month)
    path = DATA / 'raw' / 'lichess' / variant
    file = path / posixpath.basename(urllib.parse.urlparse(url).path)
    
    os.makedirs(path, exist_ok = True)
    with utils.request('GET', url, stream = True) as r, \
         open(file, 'wb') as fp:
        shutil.copyfileobj(r.raw, fp)
    return dg.Output(
        value = file,
        metadata = {'expected_checksum': context.run.tags.get('checksum')})

@dg.asset_check(asset = lichess_games_raw)
def verify_lichess_checksum(context: dg.AssetCheckExecutionContext):
    expected = context.run.tags.get('expected_checksum')
    with open(fp, 'rb') as fp:
        actual = hashlib.sha256(fp).hexdigest()
    return dg.AssetCheckResult(
        passed = expected == actual,
        metadata = {
            'expected_checksum': expected,
            'actual_checksum': actual
        },
        description = 'Verifies the downloaded Lichess file matches the published SHA256 checksum.')

lichess_job = dg.define_asset_job(
    'lichess_job', 
    selection = [lichess_games_raw])

@dg.sensor(
    job = lichess_job,
    minimum_interval_seconds = round(timedelta(days = 7).total_seconds()),
    default_status = dg.DefaultSensorStatus.RUNNING)
def partition_sensor(context: dg.SensorEvaluationContext):
    partitions = set()
    checksums = dict()
    for variant in VARIANTS:
        file_list = utils.request('GET', LIST_URL.format(variant = variant)).text.strip().split('\n')
        partitions |= {variant + PARTITION_SEPARATOR + url[-15:-8] for url in file_list}
        checksum_file = utils.request('GET', CHECKSUM_URL.format(variant = variant)).text.strip().split('\n')
        checksums.update(((variant + PARTITION_SEPARATOR + file[-15:-8]), checksum) for checksum, file in map(str.split, checksum_file))
    
    new_partitions = list(partitions - set(context.instance.get_dynamic_partitions('lichess_months')))
    if not new_partitions:
        return dg.SensorResult()
    return dg.SensorResult(
        run_requests = [
            dg.RunRequest(
                partition_key = key, 
                tags = {
                    'checksum': checksums[key],
                    'dagster/priority': '-' + key[key.index(PARTITION_SEPARATOR) + 1:].replace('-', '')}) 
            for key in new_partitions
        ],
        dynamic_partitions_requests = [
            lichess_games_partitions.build_add_request(new_partitions)
        ])

@dg.asset(
    partitions_def = lichess_games_partitions,
    # check_specs = [dg.AssetCheckSpec(name = 'verify_lichess_checksum', asset = lichess_games_raw)],
    ins = {'file': dg.AssetIn('lichess_games_raw')})
def staged_games(context: dg.AssetExecutionContext, file: pathlib.Path):
    variant, year_month = context.partition_key.split(PARTITION_SEPARATOR)
    year, month = year_month.split('-')
    path = DATA / 'stage' / 'lichess' / 'headers' / f'variant={variant}' / f'year={year}' / f'month={month}'
    dctx = zstandard.ZstdDecompressor()
    parquet_kwargs = {
        'compression': 'zstd',
        'mkdir': True
    }

    with open(file, 'rb') as fp:
        stream = parse_pgn.parse_headers(dctx.stream_reader(fp))
        batches = itertools.batched(stream, n = ROW_BUFFER_SIZE)
        for i in itertools.count(0):
            df = pl.LazyFrame()
            size =  0
            for j, batch in enumerate(batches):
                file = path / f'{j:08d}.tmp'
                pl.DataFrame(batch).write_parquet(file, **parquet_kwargs)
                df = pl.concat([df, pl.scan_parquet(file)], how = 'diagonal_relaxed')
                size += file.stat().st_size
                if size >= TARGET_FILE_SIZE_BYTES:
                    break
            else:
                df = df.with_columns(ingest_timestamp = datetime.now())
                df.sink_parquet(path, **parquet_kwargs)
                break
            df = df.with_columns(ingest_timestamp = datetime.now())
            df.sink_parquet(path, **parquet_kwargs)

    for file in path.glob('*.tmp'):
        file.unlink(missing_ok = True)
