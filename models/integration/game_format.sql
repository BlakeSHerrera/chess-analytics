MODEL (
    NAME integration.game_format,
    KIND INCREMENTAL_BY_UNIQUE_KEY (
        UNIQUE_KEY time_control),
    GRAIN (time_control, event));

SELECT
    @GENERATE_SURROGATE_KEY(t.time_control, t.event) AS game_format_key,
    t.time_control,
    STRING_SPLIT(t.time_control, '+')[1]::INT AS initial_time_minutes,
    STRING_SPLIT(t.time_control, '+')[2]::INT AS increment_time_seconds,
    LIST_SUM(LIST_TRANSFORM(STRING_SPLIT(t.time_control, '+'), s -> s::INT)) AS time_control_sum,
    FILTER(['Classical', 'Rapid', 'Blitz', 'Bullet', 'Correspondence'], s -> s IN t.event)[1] AS time_category,
    t.event,
    FILTER(['Rated', 'Casual'], s -> s IN t.event)[1] AS rating_format,
    FILTER(['Game', 'Tournament'], s -> UPPER(s) IN UPPER(t.event))[1] AS game_format,
    t.site,
    NOW() AS ingest_timestamp
FROM prep.game t
WHERE t.utc_date BETWEEN @start_date AND @end_date;
