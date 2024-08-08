import argparse
import pathlib
import sqlite3
from itertools import repeat

from python_calamine import CalamineWorkbook


def read_alias_table(sheet):
    result = {}
    id_ = 1
    for row in sheet.iter_rows():
        for alias in row:
            result[alias] = id_
        id_ += 1
    return result


def parse_players(nicknames, players):
    nicks = nicknames.split("+")
    for nick in nicks:
        if nick in players:
            yield players[nick]
        else:
            for name, id_ in players.items():
                if name.startswith(nick):
                    yield id_
                    break


def read_results_table(sheet, games, players):
    results = []
    first_row = True
    for row in sheet.iter_rows():
        result = {}
        if first_row:
            first_row = False
            continue
        date, game, winner, *others = row
        result["date"] = date
        result["game"] = games[game]
        result["winner"] = list(parse_players(winner, players))
        result["others"] = [list(parse_players(p, players)) for p in others if p]
        results.append(result)
    return results


def convert_workbook(db: sqlite3.Connection, wb: CalamineWorkbook):
    games = read_alias_table(wb.get_sheet_by_name("Games"))
    players = read_alias_table(wb.get_sheet_by_name("Players"))
    results = read_results_table(wb.get_sheet_by_name("Results"), games, players)
    cur = db.cursor()
    cur.executemany("INSERT OR IGNORE INTO game (name, id) VALUES (?, ?);", games.items())
    cur.executemany("INSERT OR IGNORE INTO player (name, id) VALUES (?, ?);", players.items())
    db.commit()
    for result in results:
        cur.execute("INSERT INTO event (date, game_id) VALUES (?, ?);",
                    (result["date"].isoformat(), result["game"]))
        event_id = cur.lastrowid
        cur.executemany("INSERT INTO result (event_id, player_id, next_teammate, winner) VALUES (?, ?, ?, TRUE)",
                        zip(repeat(event_id), result["winner"], result["winner"][1:] + [None]))
        for others in result["others"]:
            cur.executemany("INSERT INTO result (event_id, player_id, next_teammate, winner) VALUES (?, ?, ?, FALSE)",
                            zip(repeat(event_id), others, others[1:] + [None]))
    db.commit()


def create_db(db: sqlite3.Connection):
    cur = db.cursor()
    with open("schema.sql") as fp:
        cur.executescript(fp.read())
    db.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("create_db",
                                     description="Convert a game statistics spreadsheet into a SQLite3 database")
    parser.add_argument("-c", "--create", nargs='?', const='schema.sql', type=pathlib.Path,
                        help="Create a new database using the schema file")
    parser.add_argument("-o", "--out", default='results.db', type=pathlib.Path,
                        help="Choose the output filename")
    parser.add_argument("workbook", type=pathlib.Path, help="The workbook (.xlsx or .ods) to be converted")
    args = parser.parse_args()

    wb = CalamineWorkbook.from_path(args.workbook)
    db = sqlite3.connect(args.out)
    if args.create:
        create_db(db)
    convert_workbook(db, wb)
