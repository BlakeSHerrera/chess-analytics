SELECT username
FROM chesscom_player_metadata
WHERE 
    last_queried < MIN(DATE('now', 'start of month'), DATETIME(last_seen))
    OR last_queried IS NULL
ORDER BY last_queried
LIMIT 1000;
