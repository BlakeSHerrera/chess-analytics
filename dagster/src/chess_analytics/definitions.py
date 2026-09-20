from pathlib import Path

from dagster.src.chess_analytics import definitions
from dagster.src.chess_analytics import load_from_defs_folder


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
