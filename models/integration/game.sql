MODEL (
    NAME integration.game,
    KIND INCREMENTAL_BY_TIME_RANGE (
        TIME_COLUMN utc_date,
        BATCH_SIZE @default_batch_size),
    ALLOW_PARTIALS TRUE,
    GRAIN (site, game_id),
    TABLE_FORMAT 'hive',
    STORAGE_FORMAT 'parquet',
    PARTITIONED_BY (year, month),
    CLUSTERED_BY (utc_date, utc_time));

SELECT 
    @GENERATE_SURROGATE_KEY(t.time_control, t.event) AS game_format_key,
    t.game_id,
    @GENERATE_SURROGATE_KEY(t.white) AS player_key_white,
    @GENERATE_SURROGATE_KEY(t.black) AS player_key_black,
    @GENERATE_SURROGATE_KEY(t.result, t.termination) AS result_key,
    @GENERATE_SURROGATE_KEY(t.opening) AS opening_key,
    t.utc_date,  -- There is also a "Date" field?
    t.utc_time,
    t.white_elo,
    t.black_elo,
    t.white_rating_diff,
    t.black_rating_diff,
    @GENERATE_SURROGATE_KEY(t.white_title) AS title_key_white,
    @GENERATE_SURROGATE_KEY(t.black_title) AS title_key_black,
    NOW() AS ingest_timestamp,
    t.year,
    t.month
FROM prep.game t
WHERE t.utc_date BETWEEN @start_date AND @end_date;
