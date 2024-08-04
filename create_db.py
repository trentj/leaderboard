import datetime
import sqlite3

from odf.opendocument import load
from odf.table import Table, TableRow


def read_simple_table(sheet):
    result = {}
    for row in sheet.getElementsByType(TableRow):
        if not row.hasChildNodes():
            break
        id_, name, *aka = [str(n) for n in row.childNodes if n.hasChildNodes()]

        result[name] = int(id_)
        for alias in aka:
            result[alias] = int(id_)
    return result


def parse_players(nicknames, players):
    nicks = str(nicknames).split("+")
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
    for row in sheet.getElementsByType(TableRow):
        result = {}
        if first_row:
            first_row = False
            continue
        if not row.firstChild.hasChildNodes():
            break
        date, game, winner, *others = row.childNodes
        result["date"] = datetime.datetime.strptime(str(date), "%d %b %Y").date()
        result["game"] = games[str(game)]
        result["winner"] = list(parse_players(winner, players))
        result["others"] = [list(parse_players(p, players)) for p in others if p.hasChildNodes()]
        results.append(result)
    return results


if __name__ == "__main__":
    doc = load("GameNightWins.ods")
    sheets = doc.spreadsheet.getElementsByType(Table)
    sheets = {sheet.getAttribute("name"): sheet for sheet in sheets}
    games = read_simple_table(sheets["Games"])
    players = read_simple_table(sheets["Players"])
    # pprint(players)
    results = read_results_table(sheets["Results"], games, players)

    db = sqlite3.connect("results.db")
    cur = db.cursor()
    cur.execute("""
    CREATE TABLE game (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL);""")
    cur.execute("""
    CREATE TABLE player (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL);""")
    cur.execute("""
    CREATE TABLE event (
        id INTEGER PRIMARY KEY,
        date TEXT NOT NULL,
        game_id INTEGER NOT NULL,
        FOREIGN KEY (game_id) REFERENCES game (id));""")
    cur.execute("""
    CREATE TABLE result (
        event_id INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        next_teammate INTEGER,
        winner INTEGER,
        PRIMARY KEY (event_id, player_id),
        FOREIGN KEY (event_id) REFERENCES event (id),
        FOREIGN KEY (player_id) REFERENCES player (id));""")
    db.commit()

    cur.executemany("INSERT OR IGNORE INTO game (name, id) VALUES (?, ?);", games.items())
    cur.executemany("INSERT INTO player (name, id) VALUES (?, ?);", players.items())
    # pprint(results)

    for result in results:
        # pprint(result)
        cur.execute("INSERT INTO event (date, game_id) VALUES (?, ?);",
                    (result["date"].isoformat(), result["game"]))
        event_id = cur.lastrowid
        last_player = None
        for player_id in reversed(result["winner"]):
            cur.execute("INSERT INTO result (event_id, player_id, next_teammate, winner) VALUES (?, ?, ?, TRUE)",
                        (event_id, player_id, last_player))
            last_player = player_id
        for players in result["others"]:
            last_player = None
            for player_id in reversed(players):
                cur.execute("INSERT INTO result (event_id, player_id, next_teammate, winner) VALUES (?, ?, ?, FALSE)",
                            (event_id, player_id, last_player))
                last_player = player_id
    db.commit()
