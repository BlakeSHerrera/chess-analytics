import dataclasses
from datetime import datetime
import functools
import hashlib
import json
import os
import pathlib
import shutil
import sqlite3

import dotenv
from loguru import logger
from tqdm import tqdm
import zstandard

import utils


dotenv.load_dotenv(override = True)

for i in [
    PROJECT_FOLDER := pathlib.Path('./data'),
    CACHE := PROJECT_FOLDER / 'cache',
    LOGS := PROJECT_FOLDER / 'logs',
    PROJECT_FOLDER / 'tmp'
]:
    os.makedirs(i, exist_ok = True)

compressor = zstandard.ZstdCompressor()
SQLITE_FILE = PROJECT_FOLDER / 'raw_metadata.sqlite'
HEADERS = {
    'User-Agent': 'chess-data-project (username: icy_clench; contact: blakeherrera1@gmail.com)'
}

logger.remove()
logger.add(
    LOGS / 'extract.log', 
    level = os.environ['LOG_LEVEL'], 
    rotation = os.environ['LOG_ROTATION'], 
    enqueue = True)


@dataclasses.dataclass
class RecordItem:

    username: str
    year_month: datetime
    completed_on: datetime | None = None
    # file_name
    checksum: str = ''
    size_bytes: int = 0

    def as_tuple(self) -> tuple[str | int, ...]:
        return (self.username, self.year_month.isoformat(), str(self.completed_on), 
            str(self.file_name), self.checksum, self.size_bytes)

    @property
    def file_name(self) -> pathlib.Path:
        return (
            PROJECT_FOLDER 
            / 'chess.com_raw_games_json' 
            / self.ym()
            / f'{self.username}.json.zstd')

    def ym(self, delimiter: str = '-') -> str:
        return self.year_month.strftime(f'%Y{delimiter}%m')


@functools.cache
def sql(file: str):
    return pathlib.Path(f'extract/chesscom/{file}.sql').read_text()

def get(url: str) -> dict:
    return utils.request('GET', url, headers = HEADERS).json()


def main():
    conn = sqlite3.connect(':memory:')
    with sqlite3.connect(SQLITE_FILE) as disk:
        disk.backup(conn)
    conn.executescript(sql('create_table'))

    logger.info('Fetching TODO players.')
    players = conn.execute(sql('query_todo_players')).fetchall()
    for (username,) in tqdm(players, desc = 'Fetch player archives'):
        get_player_archives(conn, username)
    
    logger.info('Fetching TODO partitions.')
    partitions = conn.execute(sql('query_todo_partitions')).fetchall()
    for username, year_month in tqdm(partitions, desc = 'Fetch player partitions'):
        get_partition(conn, username, year_month)

    conn.commit()
    temp_file = SQLITE_FILE.with_suffix('.tmp')
    with sqlite3.connect(temp_file) as disk:
        conn.backup(disk)
    os.replace(temp_file, SQLITE_FILE)


def get_player_archives(conn: sqlite3.Connection, username: str):
    logger.info(f'Fetching archives for {username}.')
    data = get(f'https://api.chess.com/pub/player/{username}/games/archives')['archives']
    logger.info(f'Processing {len(data)} archives.')
    max_date = datetime(2000, 1, 1)
    url: str
    for url in data:
        year, month = map(int, url.split('/')[-2:])
        d = datetime(year, month, 1)
        max_date = max(d, max_date)
        logger.info(f'Adding partition {username}/{year}-{month:02d}')
        conn.execute(sql('add_partition'), (username, d.isoformat()))
    logger.info('Updating metadata.')
    conn.execute(
        sql('update_player_metadata'), 
        (username, max_date.isoformat(), datetime.now().isoformat()))


def get_partition(conn: sqlite3.Connection, username: str, year_month: str):
    record = RecordItem(username, datetime.fromisoformat(year_month))
    logger.info(f'Fetching partition {username}/{record.ym()}')
    raw = get(f'https://api.chess.com/pub/player/{record.username}/games/{record.ym("/")}')

    logger.info(f'Processing {len(raw["games"])} games.')
    for game in raw['games']:
        end_time = datetime.fromtimestamp(game['end_time'])
        for color in 'white', 'black':
            conn.execute(
                sql('update_player_metadata'), 
                (game[color]['username'], end_time.isoformat(), None))

    logger.info('Writing data.')
    data = compressor.compress(json.dumps(raw).encode('utf-8'))
    temp = record.file_name.with_suffix('.tmp')
    os.makedirs(str(temp.parent), exist_ok = True)
    temp.write_bytes(data)
    shutil.move(str(temp), str(record.file_name))

    logger.info('Marking complete.')
    record.checksum = hashlib.md5(data).hexdigest()
    record.size_bytes = len(data)
    record.completed_on = datetime.now()
    conn.execute(sql('mark_complete'), record.as_tuple())


if __name__ == '__main__':
    start = datetime.now()
    logger.info('Process initialized.')
    main()
    logger.info(f'Done in {datetime.now() - start}')
