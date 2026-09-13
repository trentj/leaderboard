#!python3
from flask import Flask, jsonify, render_template

import sqlite3


def get_db():
    db = sqlite3.connect("results.db")
    db.row_factory = sqlite3.Row
    return db


app = Flask(__name__)

def award_winner_circle(rows):
    ordered = sorted(rows, key=lambda row: (-row["nights_won"], row["name"]))
    distinct_scores = []
    for row in ordered:
        if not distinct_scores or distinct_scores[-1][0] != row["nights_won"]:
           distinct_scores.append([row["nights_won"], []])
        distinct_scores[-1][1].append(row)

    for index, (_, tied_rows) in enumerate(distinct_scores[:3]):
        medal = ["🏆", "🥈", "🥉"][index]
        for row in tied_rows:
           row["award"] = medal
           row["place"] = index + 1

    for row in ordered:
        row.setdefault("award", "")
        row.setdefault("place", 0)

    return ordered


def split_winner_circle_columns(rows):
    midpoint = (len(rows) + 1) // 2
    return [rows[:midpoint], rows[midpoint:]]


def winner_circle_slide(db):
    rows = db.execute("""
    WITH per_day_player_wins AS (
        SELECT
           event.date AS event_date,
           result.player_id AS player_id,
           COUNT(*) AS wins
        FROM event
        JOIN result ON result.event_id = event.id
        WHERE result.winner = TRUE
        GROUP BY event.date, result.player_id
    ),
    per_day_max AS (
        SELECT event_date, MAX(wins) AS max_wins
        FROM per_day_player_wins
        GROUP BY event_date
    ),
    per_day_winners AS (
        SELECT p.player_id
        FROM per_day_player_wins p
        JOIN per_day_max m
         ON m.event_date = p.event_date
         AND m.max_wins = p.wins
    )
    SELECT player.id, player.name, COUNT(*) AS nights_won
    FROM per_day_winners
    JOIN player ON player.id = per_day_winners.player_id
    GROUP BY player.id
    ORDER BY nights_won DESC, player.name ASC;
    """)
    winners = award_winner_circle([dict(row) for row in rows])
    return {
        "type": "winner_circle",
        "title": "Winner's Circle",
        "winner_columns": split_winner_circle_columns(winners),
    }


def ratio_roundup_slide(db):
    rows = [dict(row) for row in db.execute("""
    WITH per_player AS (
        SELECT
            player.id AS player_id,
            player.name AS name,
            SUM(CASE result.winner WHEN TRUE THEN 1 ELSE 0 END) AS wins,
            SUM(CASE result.winner WHEN TRUE THEN 0 ELSE 1 END) AS losses,
            COUNT(*) AS total
        FROM player
        JOIN result ON result.player_id = player.id
        GROUP BY player.id
    )
    SELECT
        player_id,
        name,
        wins,
        losses,
        total,
        CASE WHEN losses = 0 AND wins > 0 THEN 1 ELSE 0 END AS undefeated,
        CASE
            WHEN losses = 0 AND wins > 0 THEN NULL
            WHEN losses = 0 THEN 0.0
            ELSE (wins * 1.0) / losses
        END AS ratio
    FROM per_player
    WHERE NOT (wins > 0 AND losses = 0)
    ORDER BY
        ratio DESC,
        wins DESC,
        losses ASC,
        name ASC;
    """)]

    top = rows[:7]
    top_ids = {row["player_id"] for row in top}
    bottom = [row for row in reversed(rows) if row["player_id"] not in top_ids][:5]
    bottom.reverse()
    return {
        "type": "ratio_roundup",
        "title": "Ratio Roundup",
        "top_players": top,
        "bottom_players": bottom,
    }


def undefeated_slide(db):
    rows = [dict(row) for row in db.execute("""
    WITH per_player AS (
        SELECT
            player.id AS player_id,
            player.name AS name,
            SUM(CASE result.winner WHEN TRUE THEN 1 ELSE 0 END) AS wins,
            SUM(CASE result.winner WHEN TRUE THEN 0 ELSE 1 END) AS losses,
            COUNT(*) AS total
        FROM player
        JOIN result ON result.player_id = player.id
        GROUP BY player.id
    )
    SELECT player_id, name, wins, losses, total
    FROM per_player
    WHERE wins > 0 AND losses = 0
    ORDER BY total DESC, wins DESC, name ASC;
    """)]
    return {
        "type": "undefeated",
        "title": "The Undefeated",
        "players": rows,
    }


def game_master_slides(db):
    games = [dict(row) for row in db.execute("""
    SELECT game.id AS game_id, game.name AS game_name, COUNT(event.id) AS play_count
    FROM game
    JOIN event ON event.game_id = game.id
    GROUP BY game.id
    HAVING COUNT(event.id) >= 7
    ORDER BY game.name ASC;
    """)]
    slides = []
    for game in games:
        player_wins = [dict(row) for row in db.execute("""
        SELECT player.id AS player_id, player.name AS name, COUNT(*) AS wins
        FROM result
        JOIN event ON event.id = result.event_id
        JOIN player ON player.id = result.player_id
        WHERE event.game_id = ? AND result.winner = TRUE
        GROUP BY player.id
        ORDER BY wins DESC, name ASC;
        """, (game["game_id"],))]
        if not player_wins:
            continue
        leader_wins = player_wins[0]["wins"]
        leaders = [row["name"] for row in player_wins if row["wins"] == leader_wins]
        runner_wins = None
        for row in player_wins:
            if row["wins"] < leader_wins:
                runner_wins = row["wins"]
                break
        runners = []
        if runner_wins is not None:
            runners = [row["name"] for row in player_wins if row["wins"] == runner_wins]
        slides.append({
            "type": "game_master",
            "title": f"{game['game_name']} Master",
            "game_id": game["game_id"],
            "game_name": game["game_name"],
            "play_count": game["play_count"],
            "leader_wins": leader_wins,
            "leaders": leaders,
            "runner_wins": runner_wins,
            "runners": runners,
        })
    return slides


def build_slides_payload():
    db = get_db()
    slides = [winner_circle_slide(db), ratio_roundup_slide(db), undefeated_slide(db), *game_master_slides(db)]
    return {"slides": slides}


@app.route("/")
def slideshow():
    return render_template("slideshow.html")


@app.route("/api/slides")
def api_slides():
    return jsonify(build_slides_payload())
