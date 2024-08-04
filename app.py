#!python3
from flask import Flask, render_template

import sqlite3


def get_db():
    db = sqlite3.connect("results.db")
    db.row_factory = sqlite3.Row
    return db


app = Flask(__name__)

@app.route("/")
def index():
    db = get_db()
    players = db.execute("""
    SELECT name, count(*) AS wins FROM player
        JOIN result ON player.id = result.player_id
        WHERE result.winner = TRUE
        GROUP BY player.id
        ORDER BY wins DESC;""")
    print(players := [dict(p) for p in players])
    return render_template("index.html", players = players)
