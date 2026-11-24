MODEL (
    NAME integration.game,
    KIND INCREMENTAL_BY_TIME_RANGE (
        TIME_COLUMN utc_datetime,
        BATCH_SIZE 1),
    ALLOW_PARTIALS TRUE,
    GRAIN (variant, year, month, site, game_id),
    TABLE_FORMAT 'hive',
    STORAGE_FORMAT 'parquet',
    PARTITIONED_BY (variant, year, month),
    CLUSTERED_BY (utc_date, utc_time));

SELECT 
    t.variant,
    t.year,
    t.month,
    @GENERATE_SURROGATE_KEY(t.variant, t.time_control, t.event) AS game_format_key,
    t.game_id,
    @GENERATE_SURROGATE_KEY(t.white) AS player_key_white,
    @GENERATE_SURROGATE_KEY(t.black) AS player_key_black,
    @GENERATE_SURROGATE_KEY(t.result, t.termination) AS result_key,
    @GENERATE_SURROGATE_KEY(t.opening) AS opening_key,
    t.utc_datetime,
    t.utc_date,  -- There is also a "Date" field?
    t.utc_time,
    t.white_rating,
    t.black_rating,
    t.white_rating_diff,
    t.black_rating_diff,
    @GENERATE_SURROGATE_KEY(t.white_title) AS title_key_white,
    @GENERATE_SURROGATE_KEY(t.black_title) AS title_key_black,
    t.is_analyzed
FROM prep.game t
WHERE t.utc_date BETWEEN @start_date AND @end_date;
