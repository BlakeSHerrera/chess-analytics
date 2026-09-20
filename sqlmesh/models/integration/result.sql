MODEL (
    NAME integration.result,
    KIND INCREMENTAL_BY_UNIQUE_KEY (
        UNIQUE_KEY (result_key),
        BATCH_SIZE @default_batch_size),
    GRAIN (result, termination));

SELECT DISTINCT
    @GENERATE_SURROGATE_KEY(t.result, t.termination) AS result_key,
    t.result,
    t.result IN ('1-0', '1/2-1/2', '0-1') AS game_completed,
    t.termination
FROM prep.game t
WHERE t.utc_date BETWEEN @start_date AND @end_date;
