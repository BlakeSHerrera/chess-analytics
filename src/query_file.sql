SELECT 
    url,
    file_name,
    checksum,
    status,
    local_path
FROM files
WHERE file_name = ?
