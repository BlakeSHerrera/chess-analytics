MODEL (
    NAME analytics.player_counts_monthly,
    KIND INCREMENTAL_BY_UNIQUE_KEY (
        UNIQUE_KEY month_date,
        BATCH_SIZE @default_batch_size));

WITH new_player AS (
    SELECT
        DATE_TRUNC('MONTH', p.created_at) AS month_date,
        COALESCE(COUNT(*), 0) AS new_players
    FROM integration.player p
    GROUP BY DATE_TRUNC('MONTH', p.created_at)
),

active_player AS (
    SELECT
        pam.month_date,
        COALESCE(COUNT(*), 0) AS active_players,
        AVG(pam.avg_elo) AS avg_active_elo,
        STDDEV_POP(pam.std_elo) AS std_active_elo
    FROM analytics.player_activity_monthly pam
    GROUP BY pam.month_date
)

SELECT
    month_date,
    np.new_players,
    ap.active_players,
    ap.active_players - np.new_players AS returning_players
FROM
    new_player np
    FULL OUTER JOIN active_player ap USING (month_date)
WHERE month_date BETWEEN @start_date AND @end_date;
