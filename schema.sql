PRAGMA foreign_keys = ON;

CREATE TABLE game (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL);

CREATE TABLE player (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL);

CREATE TABLE event (
    id INTEGER PRIMARY KEY,
    date TEXT NOT NULL,
    game_id INTEGER NOT NULL,
    FOREIGN KEY (game_id) REFERENCES game (id));

CREATE TABLE result (
    event_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    next_teammate INTEGER,
    winner INTEGER,
    PRIMARY KEY (event_id, player_id),
    FOREIGN KEY (event_id) REFERENCES event (id),
    FOREIGN KEY (player_id) REFERENCES player (id));
