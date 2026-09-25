SELECT 
    CASE
        WHEN completed_on IS NULL
        THEN 'todo'
        ELSE 'done' 
        END,
    COUNT(*)
FROM chesscom_raw_metadata
GROUP BY completed_on IS NULL;
