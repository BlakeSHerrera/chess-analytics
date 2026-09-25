WITH groups AS (
    SELECT 
        CASE
            WHEN last_queried < MIN(DATE('now', 'start of month'), DATETIME(last_seen))
                OR last_queried IS NULL
            THEN 'todo'
            ELSE 'done'
            END AS is_todo
    FROM chesscom_player_metadata
)

SELECT
    is_todo,
    COUNT(*)
FROM groups
GROUP BY is_todo;
