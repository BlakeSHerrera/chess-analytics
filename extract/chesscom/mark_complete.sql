INSERT INTO chesscom_raw_metadata (
    username,
    year_month,
    completed_on,
    file_name,
    checksum,
    size_bytes)
VALUES (
    ?, 
    ?, 
    ?, 
    ?, 
    ?,
    ?)
ON CONFLICT(username, year_month) DO UPDATE SET
    username = excluded.username,
    year_month = excluded.year_month,
    completed_on = excluded.completed_on,
    file_name = excluded.file_name,
    checksum = excluded.checksum,
    size_bytes = excluded.size_bytes;
