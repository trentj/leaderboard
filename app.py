#!python3
import os
import pathlib
import sqlite3
import tempfile

from flask import Flask, jsonify, render_template, request

from db_utils import DEFAULT_DB_PATH, write_workbook_to_db


app = Flask(__name__)


def last_name(name):
    if name is None:
        return None
    stripped = name.strip()
    if not stripped:
        return None
    return stripped.rsplit(" ", 1)[-1]


def configure_db(db):
    db.row_factory = sqlite3.Row
    db.create_function("last_name", 1, last_name)
    return db


def get_db():
    return configure_db(sqlite3.connect(DEFAULT_DB_PATH))


def award_for_place(place):
    if place == 1:
        return "🏆"
    if place == 2:
        return "🥈"
    if place == 3:
        return "🥉"
    return ""


def award_winner_circle(rows):
    ordered = sorted(rows, key=lambda row: (-row["nights_won"], row["name"]))
    distinct_scores = []
    for row in ordered:
        if not distinct_scores or distinct_scores[-1][0] != row["nights_won"]:
            distinct_scores.append([row["nights_won"], []])
        distinct_scores[-1][1].append(row)

    place = 1
    for _, tied_rows in distinct_scores:
        medal = award_for_place(place)
        for row in tied_rows:
            row["award"] = medal
            row["place"] = place
        place += len(tied_rows)

    for row in ordered:
        row.setdefault("award", "")
        row.setdefault("place", 0)

    return ordered


def split_winner_circle_columns(rows):
    midpoint = (len(rows) + 1) // 2
    return [rows[:midpoint], rows[midpoint:]]


def winner_circle_slide(db):
    rows = db.execute("""
    WITH qualifying_days AS (
        SELECT
           event.date AS event_date
        FROM event
        JOIN result ON result.event_id = event.id
        JOIN player ON player.id = result.player_id
        GROUP BY event.date
        HAVING COUNT(DISTINCT last_name(player.name)) >= 3
           AND COUNT(DISTINCT event.id) >= 2
    ),
    per_day_player_wins AS (
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
    per_day_winner_counts AS (
        SELECT p.event_date, COUNT(*) AS winner_count
        FROM per_day_player_wins p
        JOIN per_day_max m
         ON m.event_date = p.event_date
         AND m.max_wins = p.wins
        GROUP BY p.event_date
    ),
    valid_nights AS (
        SELECT d.event_date
        FROM qualifying_days d
        JOIN per_day_winner_counts w ON w.event_date = d.event_date
        WHERE w.winner_count = 1
    )
    SELECT player.id, player.name, COUNT(*) AS nights_won
    FROM valid_nights
    JOIN per_day_player_wins ON per_day_player_wins.event_date = valid_nights.event_date
    JOIN per_day_max ON per_day_max.event_date = valid_nights.event_date
        AND per_day_max.max_wins = per_day_player_wins.wins
    JOIN player ON player.id = per_day_player_wins.player_id
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
    try:
        slides = [winner_circle_slide(db), ratio_roundup_slide(db), undefeated_slide(db), *game_master_slides(db)]
        return {"slides": slides}
    finally:
        db.close()


@app.route("/")
def index():
    return render_template("index.html")


def replace_results_database(uploaded_file):
    upload_fd, upload_name = tempfile.mkstemp(suffix=".ods")
    os.close(upload_fd)
    uploaded_path = pathlib.Path(upload_name)
    db_fd, db_name = tempfile.mkstemp(prefix="results-", suffix=".db", dir=str(DEFAULT_DB_PATH.parent))
    os.close(db_fd)
    temp_db_path = pathlib.Path(db_name)
    try:
        uploaded_file.save(uploaded_path)
        write_workbook_to_db(uploaded_path, temp_db_path)
        os.replace(temp_db_path, DEFAULT_DB_PATH)
    finally:
        if uploaded_path.exists():
            uploaded_path.unlink()
        if temp_db_path.exists():
            temp_db_path.unlink()


@app.route("/upload", methods=["GET", "POST"])
def upload():
    if request.method == "POST":
        uploaded_file = request.files.get("workbook")
        if uploaded_file is None or uploaded_file.filename == "":
            return render_template("upload.html", error="Choose an .ods file to upload.")
        if pathlib.Path(uploaded_file.filename).suffix.lower() != ".ods":
            return render_template("upload.html", error="Only .ods files are supported.")
        replace_results_database(uploaded_file)
        return render_template("upload.html", success="results.db was replaced successfully.")
    return render_template("upload.html")


@app.route("/kiosk")
def slideshow():
    return render_template("slideshow.html")

@app.route("/api/slides")
def api_slides():
    return jsonify(build_slides_payload())
