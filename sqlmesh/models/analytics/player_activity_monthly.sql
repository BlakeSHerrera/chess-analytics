MODEL (
    NAME analytics.player_activity_monthly,
    KIND INCREMENTAL_BY_UNIQUE_KEY (
        UNIQUE_KEY (month_date, player_key),
        BATCH_SIZE @default_batch_size),
    CLUSTERED_BY month_date);

SELECT
    DATE_TRUNC('MONTH', gl.utc_date) AS month_date,
    gl.player_key,
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
    AVG(gl.elo) AS avg_elo,
    MIN(gl.elo) AS min_elo,
    MAX(gl.elo) AS max_elo,
    STDDEV_POP(gl.elo) AS std_elo,
    --Maybe only count rated games?
    SUM(gl.won) AS wins,
    SUM(gl.drew) AS draws,
    SUM(gl.lost) AS losses,
    --Don't count abandoned games.
    SUM(gl.won::INT + gl.drew::INT / 2) 
        / SUM(gl.won::INT + gl.drew::INT + gl.lost::INT) 
        AS avg_score,
    --Assume avg playtime in minutes is (base + inc) / 2
    SUM(gf.time_control_sum) / 120 AS est_playtime_hours,
    24 * DAY(LAST_DAY(DATE_TRUNC('MONTH', gl.utc_date))) AS hours_in_month
FROM 
    integration.game_long gl
    LEFT JOIN integration.game_format gf USING (game_format_key)
WHERE gl.utc_date BETWEEN @start_date AND @end_date
GROUP BY 
    DATE_TRUNC('MONTH', gl.utc_date), 
    gl.player_key;
