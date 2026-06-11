MODEL (
    NAME integration.player,
    KIND INCREMENTAL_BY_UNIQUE_KEY (
        UNIQUE_KEY (player),
        BATCH_SIZE @default_batch_size),
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
),

players_grouped AS (
    SELECT
        t.player,
        MAX(t.title IS NOT NULL AND 'BOT' IN t.title) AS is_bot,
        MIN(t.utc_date + t.utc_time) AS created_at
    FROM players t
    GROUP BY t.player
)

SELECT
    @GENERATE_SURROGATE_KEY(source.player)::TEXT AS player_key,
    player::TEXT,
    GREATEST(source.is_bot, target.is_bot)
        ::BOOLEAN AS is_bot,
    LEAST(source.created_at, target.created_at)::TIMESTAMP AS created_at
FROM
    players_grouped source
    LEFT JOIN @this_model target USING (player);
