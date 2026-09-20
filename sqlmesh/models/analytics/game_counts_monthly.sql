MODEL (
    NAME analytics.game_counts_monthly,
    KIND INCREMENTAL_BY_UNIQUE_KEY (
        UNIQUE_KEY month_date,
        BATCH_SIZE @default_batch_size));

SELECT
    DATE_TRUNC('MONTH', g.utc_date) AS month_date,
    COUNT(*) AS total_games,
    SUM(gf.time_category = 'Bullet') AS bullet_games,
    SUM(gf.time_category = 'Blitz') AS blitz_games,
    SUM(gf.time_category = 'Rapid') AS rapid_games,
    SUM(gf.time_category = 'Classical') AS classical_games,
    SUM(gf.time_category = 'Correspondence') AS corresponence_games,
    SUM(gf.game_format = 'Tournament') AS tournament_games,
    SUM(gf.game_format = 'Game') AS non_tournament_games,
    SUM(gf.game_format = 'Rated') AS rated_games,
    SUM(gf.game_format = 'Casual') AS casual_games,
    --Counts once for each player. Assume playtime is (base + inc) / 2. The two coefficients cancel.
    SUM(gf.time_control_sum) / 60 AS est_playtime_hours,
    24 * DAY(LAST_DAY(DATE_TRUNC('MONTH', g.utc_date))) AS hours_in_month
FROM 
    integration.game g
    LEFT JOIN integration.game_format gf USING (game_format_key)
WHERE g.utc_date BETWEEN @start_date AND @end_date
GROUP BY DATE_TRUNC('MONTH', g.utc_date);
