MODEL (
    NAME analytics.player_activity_monthly,
    KIND INCREMENTAL_BY_UNIQUE_KEY (
        UNIQUE_KEY (time_category, month_date, player_key),
        BATCH_SIZE 1),
    CLUSTERED_BY month_date);

SELECT
    gf.time_category AS time_category,
    DATE_TRUNC('MONTH', gl.utc_date) AS month_date,
    gl.player_key,
    COUNT(*) AS total_games,
    SUM(gf.game_format = 'Tournament') AS tournament_games,
    SUM(gf.game_format = 'Game') AS non_tournament_games,
    SUM(gf.game_format = 'Rated') AS rated_games,
    SUM(gf.game_format = 'Casual') AS casual_games,
    --Note either player could have analyzed the game.
    SUM(gl.is_analyzed) AS analyzed_games,
    AVG(gl.rating) AS avg_rating,
    MIN(gl.rating) AS min_rating,
    MAX(gl.rating) AS max_rating,
    -- STDDEV_POP(gl.rating) AS std_rating,
    MIN_BY(gl.rating, gl.utc_datetime) AS starting_rating,
    MAX_BY(gl.rating, gl.utc_datetime) AS ending_rating,
    MAX_BY(gl.rating, gl.utc_datetime) - MIN_BY(gl.rating, gl.utc_datetime) AS rating_change,
    --Maybe only count rated games?
    SUM(gl.won) AS wins,
    SUM(gl.drew) AS draws,
    SUM(gl.lost) AS losses,
    --Don't count abandoned games.
    SUM(gl.won::INT + gl.drew::INT / 2) 
        / SUM(gl.won::INT + gl.drew::INT + gl.lost::INT) 
        AS avg_score,
    --Assume avg playtime in minutes is (base + inc) / 2
    SUM(gf.time_control_sum) / 120 AS est_play_time_hours,
    SUM(IF(gl.is_analyzed, gf.time_control_sum, 0)) / 120 AS est_analysis_time_hours
    24 * DAY(LAST_DAY(DATE_TRUNC('MONTH', gl.utc_date))) AS hours_in_month
FROM 
    integration.game_long gl
    LEFT JOIN integration.game_format gf USING (game_format_key)
WHERE gl.utc_date BETWEEN @start_date AND @end_date
GROUP BY 
    gf.time_category,
    DATE_TRUNC('MONTH', gl.utc_date), 
    gl.player_key;
