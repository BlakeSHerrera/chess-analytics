import dataclasses
import io
from typing import Callable, Iterable

import chess, chess.pgn


def parse_headers(stream: io.TextIOBase) -> Iterable[chess.pgn.Headers]:
    tags = dict()
    for line in map(str.strip, stream):
        if line:
            i = line.index(' ')
            tags[line[1:i]] = line[i + 2:-2]
        else:
            pgn = next(stream).strip()
            # tags['PGN'] = pgn
            next(stream)
            yield tags
            tags = dict()

    # python-chess is 4x slower:
    # while game := chess.pgn.read_headers(stream):
    #     yield game

    
def extract_comment(subject: str, find: str):
    i = subject.find(find)
    if i == -1:
        return None
    i += len(find)
    j = subject.index(']', i)
    return subject[i:j]


@dataclasses.dataclass
class Feature[T]:

    name: str
    func: Callable[[chess.Board, chess.Move], T]
    
    def extract(self, board: chess.Board, move: chess.Move) -> T:
        return self.func(board, move)
    

class FeatureExtractorVisitor(chess.pgn.BaseVisitor):

    FEATURES = {
        'ply': lambda b, m: b.ply(),
        'is_ep': lambda b, m: b.is_en_passant(m),
        'from_square': lambda b, m: chess.square_name(m.from_square),
        'to_square': lambda b, m: chess.square_name(m.to_square),
        'from_piece': lambda b, m: str(b.piece_at(m.from_square)),
        'to_piece': lambda b, m: str(b.piece_at(m.to_square)) if b.piece_at(m.to_square) else None,
        'promotion': lambda b, m: str(m.promotion) if m.promotion else None,
    }
    
    def __init__(self):
        super().__init__()
        self.move_data: list[dict] = []
        self.headers: dict[str, str] = dict()
        self.game_id = None

    def visit_header(self, tagname: str, tagvalue: str):
        self.headers[tagname] = tagvalue
        if tagname == 'Site':
            self.game_id = tagvalue[len('https://lichess.org/'):]

    def visit_move(self, board: chess.Board, move: chess.Move):
        data = {key: func(board, move) for key, func in self.FEATURES.items()}
        data['game_id'] = self.game_id
        self.move_data.append(data)
    
    def visit_comment(self, comment: str):
        if eval := extract_comment(comment, '%eval '):
            self.move_data[-1]['eval'] = eval
        if clk := extract_comment(comment, '%clk '):
            self.move_data[-1]['clk'] = clk

    def result(self) -> Iterable[dict]:
        return self.headers, self.move_data


def parse_moves(stream: io.TextIOBase) -> Iterable[list[dict]]:
    while data := chess.pgn.read_game(stream, Visitor = FeatureExtractorVisitor):
        headers, move_data = data
        yield move_data
