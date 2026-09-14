import pathlib
import sqlite3
import re
from itertools import repeat

from python_calamine import CalamineWorkbook


BASE_DIR = pathlib.Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "schema.sql"
DEFAULT_DB_PATH = BASE_DIR / "results.db"


def read_alias_table(sheet):
    result = {}
    id_ = 1
    for row in sheet.iter_rows():
        for alias in row:
            if alias is None:
                continue
            if isinstance(alias, str):
                alias = alias.strip()
            if not alias:
                continue
            result[alias] = id_
        id_ += 1
    return result


def next_id(alias_table):
    return max(alias_table.values(), default=0) + 1


def resolve_game(name, games):
    if name not in games:
        games[name] = next_id(games)
    return games[name]


def resolve_player(name, players):
    if name in players:
        return players[name]
    for alias, id_ in players.items():
        if alias.startswith(name):
            return id_
    players[name] = next_id(players)
    return players[name]


def parse_players(nicknames, players):
    nicks = nicknames.split("+")
    for nick in nicks:
        yield resolve_player(nick, players)


def parse_result_row(row, games, players):
    date, game, winner, *others = row
    result = {
        "date": date,
        "game": resolve_game(game, games),
    }
    tie_match = re.fullmatch(r"TIE(\d*)", winner) if isinstance(winner, str) else None
    if tie_match:
        tie_count = int(tie_match.group(1) or 2)
        winner_cells = others[:tie_count]
        others = others[tie_count:]
        result["winner"] = [player for cell in winner_cells if cell for player in parse_players(cell, players)]
    else:
        result["winner"] = list(parse_players(winner, players))
    result["others"] = [list(parse_players(p, players)) for p in others if p]
    return result


def read_results_table(sheet, games, players):
    results = []
    first_row = True
    for row in sheet.iter_rows():
        if first_row:
            first_row = False
            continue
        results.append(parse_result_row(row, games, players))
    return results


def convert_workbook(db: sqlite3.Connection, wb: CalamineWorkbook):
    games = read_alias_table(wb.get_sheet_by_name("Games"))
    players = read_alias_table(wb.get_sheet_by_name("Players"))
    results = read_results_table(wb.get_sheet_by_name("Results"), games, players)
    cur = db.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO game (id, name) VALUES (?, ?);",
        ((id_, name) for name, id_ in games.items()),
    )
    cur.executemany(
        "INSERT OR IGNORE INTO player (id, name) VALUES (?, ?);",
        ((id_, name) for name, id_ in players.items()),
    )
    db.commit()
    for result in results:
        cur.execute(
            "INSERT INTO event (date, game_id) VALUES (?, ?);",
            (result["date"].isoformat(), result["game"]),
        )
        event_id = cur.lastrowid
        cur.executemany(
            "INSERT INTO result (event_id, player_id, next_teammate, winner) VALUES (?, ?, ?, TRUE)",
            zip(repeat(event_id), result["winner"], result["winner"][1:] + [None]),
        )
        for others in result["others"]:
            cur.executemany(
                "INSERT INTO result (event_id, player_id, next_teammate, winner) VALUES (?, ?, ?, FALSE)",
                zip(repeat(event_id), others, others[1:] + [None]),
            )
    db.commit()


def create_db(db: sqlite3.Connection):
    cur = db.cursor()
    with open(SCHEMA_PATH) as fp:
        cur.executescript(fp.read())
    db.commit()


def write_workbook_to_db(workbook_path: pathlib.Path, db_path: pathlib.Path):
    wb = CalamineWorkbook.from_path(workbook_path)
    db = sqlite3.connect(db_path)
    try:
        create_db(db)
        convert_workbook(db, wb)
    finally:
        db.close()
