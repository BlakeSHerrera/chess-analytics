SELECT
    username,
    year_month
FROM chesscom_raw_metadata
WHERE completed_on IS NULL
ORDER BY year_month
LIMIT 10000;
