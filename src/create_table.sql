CREATE TABLE IF NOT EXISTS files (
    url TEXT,
    file_name TEXT PRIMARY KEY,
    checksum TEXT,
    status TEXT,
    local_path TEXT,
    size_bytes INTEGER)
