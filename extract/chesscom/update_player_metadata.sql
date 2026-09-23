INSERT INTO chesscom_player_metadata
    (username, last_seen, last_queried)
VALUES 
    (?, ?, ?)
ON CONFLICT (username) DO UPDATE SET
    username = excluded.username,
    last_seen = COALESCE(
        MAX(last_seen, excluded.last_seen),
        last_seen,
        excluded.last_seen),
    last_queried = COALESCE(
        MAX(last_queried, excluded.last_queried),
        last_queried,
        excluded.last_queried);
