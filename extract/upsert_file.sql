INSERT INTO files (
    url, 
    file_name, 
    checksum, 
    status, 
    local_path)
VALUES (
    ?, 
    ?, 
    ?, 
    ?, 
    ?)
ON CONFLICT(file_name) DO UPDATE SET
    url = excluded.url,
    file_name = excluded.file_name,
    checksum = excluded.checksum,
    status = excluded.status,
    local_path = excluded.local_path
