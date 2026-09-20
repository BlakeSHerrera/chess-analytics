INSERT INTO chesscom_player_metadata (
    username, 
    last_seen, 
    last_queried)
VALUES (
    ?, 
    ?, 
    ?)
ON CONFLICT (username) DO UPDATE SET
    username = excluded.username,
    last_seen = MAX(last_seen, excluded.last_seen),
    last_queried = CASE
        WHEN excluded.last_queried IS NULL THEN last_queried
        ELSE MAX(last_queried, excluded.last_queried) END;
