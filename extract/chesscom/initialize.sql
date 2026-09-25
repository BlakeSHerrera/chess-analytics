CREATE TABLE IF NOT EXISTS chesscom_raw_metadata (
    username TEXT,
    year_month DATE,
    completed_on TIMESTAMP,
    file_name TEXT,
    checksum TEXT,
    size_bytes INTEGER,
    PRIMARY KEY (username, year_month));

CREATE TABLE IF NOT EXISTS chesscom_player_metadata (
    username TEXT PRIMARY KEY,
    last_seen TIMESTAMP,
    last_queried TIMESTAMP);

INSERT OR IGNORE INTO chesscom_player_metadata
    (username, last_seen, last_queried)
VALUES 
    ('Icy_Clench', '2000-01-01', NULL);
