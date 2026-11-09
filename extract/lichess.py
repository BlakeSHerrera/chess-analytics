from datetime import datetime, timedelta
import functools
import hashlib
import itertools
import logging
import os
import pathlib
import posixpath
import shutil
import urllib.parse

import dagster as dg
import dotenv
import polars as pl
import requests_cache
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
VARIANTS = [
    'standard', 
    'antichess', 
    'atomic', 
    'chess960', 
    'crazyhouse', 
    'horde', 
    'kingOfTheHill', 
    'racingKings', 
    'threeCheck', 
    'broadcast'
]
BASE_URL = 'https://database.lichess.org/'
DOWNLOAD_URL = BASE_URL + '{variant}/lichess_db_{variant}_rated_{year_month}.pgn.zst'
LIST_URL = BASE_URL + '{variant}/list.txt'
CHECKSUM_URL = BASE_URL + '{variant}/sha256sums.txt'
PUZZLE_URL = BASE_URL + 'lichess_db_puzzle.csv.zst'
EVAL_URL = BASE_URL + 'lichess_db_eval.jsonl.zst'
OPENINGS_GITHUB = 'https://github.com/lichess-org/chess-openings'
PARTITION_SEPARATOR = '|'


def get_url_and_path(variant: str, year_month: str) -> tuple[str, pathlib.Path]:
    url = DOWNLOAD_URL.format(variant = variant, year_month = year_month)
    if variant == 'broadcast':
        url = url.replace('_rated', '')
    path = DATA / 'raw' / 'lichess' / variant / posixpath.basename(urllib.parse.urlparse(url).path)
    return (url, path)

def get_checksums_file(variant: str) -> str:
    session = requests_cache.CachedSession(
        cache_name = DATA / 'http_cache',
        backend = 'filesystem',
        expire_after = timedelta(days = 1))
    r = session.get(CHECKSUM_URL.format(variant = variant))
    r.raise_for_status()
    return r.text.strip()

@functools.cache
def get_expected_checksum(variant: str, year_month: str) -> str:
    for checksum, file in map(str.split, get_checksums_file(variant).split('\n')):
        if file[-15:-8] == year_month:
            return checksum
    raise ValueError(f'No checksum for {variant}{PARTITION_SEPARATOR}{year_month}')

def get_checksum(file: pathlib.Path):
    with open(file, 'rb') as fp:
        # file_digest ignores open's buffering; Larger buffer sizes (4 MB) are good for HDDs 
        return hashlib.file_digest(fp, hashlib.sha256, _bufsize = 2 ** 22).hexdigest()

    
@dg.asset(partitions_def = lichess_games_partitions)
def lichess_games_raw(context: dg.AssetExecutionContext):
    variant, year_month = context.partition_key.split(PARTITION_SEPARATOR)
    url, file = get_url_and_path(variant, year_month)
    os.makedirs(file.parent, exist_ok = True)
    expected = get_expected_checksum(variant, year_month)
    logging.info(f'Found file? {(e := file.exists())}')
    if not e or get_checksum(file) != expected:
        with utils.request('GET', url, stream = True) as r, \
            open(file, 'wb') as fp:
            shutil.copyfileobj(r.raw, fp)
    return dg.MaterializeResult(metadata = {'file': file})


@dg.asset_check(
    asset = lichess_games_raw,
    partitions_def = lichess_games_partitions,
    blocking = True)
def verify_lichess_checksum(context: dg.AssetCheckExecutionContext):
    variant, year_month = context.partition_key.split(PARTITION_SEPARATOR)
    _, file = get_url_and_path(variant, year_month)
    expected = get_expected_checksum(variant, year_month)
    actual = get_checksum(file)
    return dg.AssetCheckResult(
        passed = expected == actual,
        metadata = {
            'expected_checksum': expected,
            'actual_checksum': actual,
        })


@dg.asset(
    partitions_def = lichess_games_partitions,
    # check_specs = [dg.AssetCheckSpec(name = 'verify_lichess_checksum', asset = lichess_games_raw)],
    ins = {'file': dg.AssetIn('lichess_games_raw')})
def staged_games(context: dg.AssetExecutionContext, file: pathlib.Path):
    variant, year_month = context.partition_key.split(PARTITION_SEPARATOR)
    year, month = year_month.split('-')
    path = DATA / 'stage' / 'lichess' / 'headers' / f'variant={variant}' / f'year={year}' / f'month={month}'
    os.mkdir(path, exist_ok = True)

    with open(file, 'rb') as fp:
        batches = itertools.batched(
            parse_pgn.parse_headers(zstandard.ZstdDecompressor().stream_reader(fp)), 
            n = ROW_BUFFER_SIZE)
        for i in itertools.count(0):
            size = 0
            for j, batch in enumerate(batches):
                file = path / f'{i:04d}-{j:08d}.tmp'
                pl.DataFrame(batch).write_parquet(file)
                size += file.stat().st_size
                if size >= TARGET_FILE_SIZE_BYTES:
                    break

            if not size:
                break
            pl.concat(map(pl.scan_parquet, path.glob('*.tmp')), how = 'diagonal_relaxed') \
                .with_columns(ingest_timestamp = datetime.now()) \
                .sink_parquet(path / f'{i:04d}.parquet')
            # Don't overwrite same file pointers for SSD storage.
            for file in path.glob('*.tmp'):
                file.unlink(missing_ok = True)

    return dg.MaterializeResult(
        metadata = {
            'folder': str(path),
            'file_count': i + 1,
            'size_bytes': sum(file.stat().st_size for file in path.glob('*.parquet')),
        })


lichess_job = dg.define_asset_job(
    'lichess_job', 
    selection = [lichess_games_raw, staged_games])


@dg.sensor(
    job = lichess_job,
    minimum_interval_seconds = round(timedelta(days = 7).total_seconds()),
    default_status = dg.DefaultSensorStatus.RUNNING)
def partition_sensor(context: dg.SensorEvaluationContext):
    partitions = set()
    for variant in VARIANTS:
        file_list = utils.request('GET', LIST_URL.format(variant = variant)).text.strip().split('\n')
        partitions |= {variant + PARTITION_SEPARATOR + url[-15:-8] for url in file_list}
        
    new_partitions = list(partitions - set(context.instance.get_dynamic_partitions('lichess_months')))
    if not new_partitions:
        return dg.SensorResult()
    return dg.SensorResult(
        run_requests = [
            dg.RunRequest(
                partition_key = key,
                # Older partitions run first.
                tags = {'dagster/priority': '-' + key[key.index(PARTITION_SEPARATOR) + 1:].replace('-', '')}) 
            for key in new_partitions
        ],
        dynamic_partitions_requests = [lichess_games_partitions.build_add_request(new_partitions)])


