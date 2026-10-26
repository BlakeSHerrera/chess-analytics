# import asyncio
# import dataclasses
# from datetime import datetime
# import functools
# import hashlib
# import json
# import os
# import pathlib
# import shutil
# import sqlite3
# import sys
# import urllib3.util

# import dotenv
# import httpcore
# import httpx
# import httpx_retry
# from loguru import logger
# import requests, requests.adapters
# from tqdm.asyncio import tqdm
# import zstandard


# dotenv.load_dotenv(override = True)

# for i in [
#     PROJECT_FOLDER := pathlib.Path('./data'),
#     CACHE := PROJECT_FOLDER / 'cache',
#     LOGS := PROJECT_FOLDER / 'logs',
#     PROJECT_FOLDER / 'tmp'
# ]:
#     os.makedirs(i, exist_ok = True)

# compressor = zstandard.ZstdCompressor()

# KNOWN_ERRORS = (
#     requests.exceptions.HTTPError, 
#     requests.exceptions.SSLError)
# client = httpx.AsyncClient(
#     headers = {
#         'User-Agent': 'chess-data-project (username: icy_clench; contact: blakeherrera1@gmail.com)'
#     },
#     transport = httpx.AsyncHTTPTransport(
#         limits = httpx.Limits(max_connections = 2),
#         retry = httpx_retry.RetryTransport(
#             policy = httpx_retry.RetryPolicy()
#                 .with_max_retries(3)
#                 .with_min_delay(2.0)
#                 .with_multiplier(2)
#                 .with_retry_on({429, 500, 502, 503, 504}.__contains__))))

# adapter = requests.adapters.HTTPAdapter(
#     pool_connections = 2,
#     pool_maxsize = 2,
#     max_retries = urllib3.util.Retry(
#         total = 5,
#         backoff_factor = 2,
#         status_forcelist = [429, 500, 502, 503, 504],
#         raise_on_status = True))
# session.mount('http://', adapter)
# session.mount('https://', adapter)

# logger.remove()
# logger.add(
#     LOGS / 'extract.log', 
#     level = os.environ['LOG_LEVEL'], 
#     rotation = os.environ['LOG_ROTATION'], 
#     enqueue = True)
# logger.add(sys.stdout, level = 'INFO')


# @dataclasses.dataclass
# class RecordItem:

#     username: str
#     year_month: datetime
#     completed_on: datetime | None = None
#     # file_name
#     checksum: str = ''
#     size_bytes: int = 0

#     def as_tuple(self) -> tuple[str | int, ...]:
#         return (self.username, self.year_month.isoformat(), str(self.completed_on), 
#             str(self.file_name), self.checksum, self.size_bytes)

#     @property
#     def file_name(self) -> pathlib.Path:
#         return (
#             PROJECT_FOLDER 
#             / 'chess.com_raw_games_json' 
#             / self.ym()
#             / f'{self.username}.json.zstd')

#     def ym(self, delimiter: str = '-') -> str:
#         return self.year_month.strftime(f'%Y{delimiter}%m')

#     @property
#     def url(self) -> str:
#         return f'https://api.chess.com/pub/player/{self.username}/games/{self.ym("/")}'


# @functools.cache
# def sql(file: str):
#     return pathlib.Path(f'extract/chesscom/{file}.sql').read_text()

# def get_counts(conn: sqlite3.Connection, sql_file: str):
#     logger.info(f'Running {sql_file}.sql')
#     counts = dict(conn.execute(sql(sql_file)))
#     todo = counts['todo']
#     done = counts['done']
#     total = todo + done
#     logger.info(
#         f'{todo} todo ({todo / total:.2%}). '
#         f'{done} done ({done / total:.2%}). '
#         f'{total} total.')


# async def main():
#     with sqlite3.connect(PROJECT_FOLDER / 'raw_metadata.sqlite') as conn:
#         conn.executescript(sql('initialize'))

#         get_counts(conn, 'count_players')
#         for task in tqdm.as_completed(
#             [get_player_archives(conn, *data) for data in conn.execute(sql('query_todo_players'))],
#             desc = 'Fetch player archives'
#         ):
#             await task

#         get_counts(conn, 'count_partitions')
#         for task in tqdm.as_completed(
#             [get_partition(conn, *data) for data in conn.execute(sql('query_todo_partitions'))],
#             desc = 'Fetch player partitions'
#         ):
#             await task

#         logger.debug('Committing changes.')


# async def get_player_archives(conn: sqlite3.Connection, username: str):
#     logger.debug(f'Fetching archives for {username}.')
#     try:
#         url = f'https://api.chess.com/pub/player/{username}/games/archives'
#         data = await grequests.get(url, session = session)
#         logger.debug(f'Processing {len(data["archives"])} archives.')
#     except KNOWN_ERRORS as e:
#         logger.error(f'Error fetching archives for player {username}: {e}')
#         return

#     max_date = datetime(2000, 1, 1)
#     url: str
#     for url in data['archives']:
#         year, month = map(int, url.split('/')[-2:])
#         d = datetime(year, month, 1)
#         max_date = max(d, max_date)
#         logger.debug(f'Adding partition {username}/{year}-{month:02d}')
#         conn.execute(sql('add_partition'), (username, d.isoformat()))
#     logger.debug('Updating metadata.')
#     conn.execute(
#         sql('update_player_metadata'), 
#         (username, max_date.isoformat(), datetime.now().isoformat()))
#     return username


# async def get_partition(conn: sqlite3.Connection, username: str, year_month: str):
#     record = RecordItem(username, datetime.fromisoformat(year_month))
#     logger.debug(f'Fetching partition {username}/{record.ym()}')
#     try:
#         raw = await grequests.get(record.url, session = session)
#     except KNOWN_ERRORS as e:
#         logger.error(f'Error fetching archive for {username}: {e}')
#         return

#     logger.debug(f'Processing {len(raw["games"])} games.')
#     for game in raw['games']:
#         end_time = datetime.fromtimestamp(game['end_time'])
#         for color in 'white', 'black':
#             conn.execute(
#                 sql('update_player_metadata'), 
#                 (game[color]['username'], end_time.isoformat(), None))

#     logger.debug('Writing data.')
#     data = compressor.compress(json.dumps(raw).encode('utf-8'))
#     temp = record.file_name.with_suffix('.tmp')
#     os.makedirs(str(temp.parent), exist_ok = True)
#     temp.write_bytes(data)
#     shutil.move(str(temp), str(record.file_name))

#     logger.debug('Marking complete.')
#     record.checksum = hashlib.md5(data).hexdigest()
#     record.size_bytes = len(data)
#     record.completed_on = datetime.now()
#     conn.execute(sql('mark_complete'), record.as_tuple())
#     return username, year_month


# if __name__ == '__main__':
#     start = datetime.now()
#     logger.info('Process initialized.')
#     asyncio.run(main())
#     logger.info(f'Done in {datetime.now() - start}')
