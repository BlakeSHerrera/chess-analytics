MODEL (
    NAME integration.player,
    KIND INCREMENTAL_BY_UNIQUE_KEY (
        UNIQUE_KEY (player),
        WHEN_MATCHED (
            WHEN MATCHED THEN UPDATE SET
                created_at = LEAST(source.created_at, target.created_at),
                is_bot = GREATEST(source.is_bot, target.is_bot))
    ),
    GRAIN player);

WITH players AS (
    SELECT
        t.white AS player,
        t.white_title AS title,
        t.utc_date,
        t.utc_time
    FROM prep.game t
    WHERE t.utc_date BETWEEN @start_date AND @end_date

    UNION ALL
    SELECT
        t.black AS player,
        t.black_title AS title,
        t.utc_date,
        t.utc_time
    FROM prep.game t
    WHERE t.utc_date BETWEEN @start_date AND @end_date
)

SELECT DISTINCT
    @GENERATE_SURROGATE_KEY(t.player) AS player_key,
    t.player,
    MAX('BOT' IN t.title) AS is_bot,
    MIN(t.utc_date + t.utc_time) AS created_at
FROM players t
GROUP BY t.player;
