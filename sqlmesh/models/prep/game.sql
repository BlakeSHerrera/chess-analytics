MODEL (
    NAME prep.game,
    KIND INCREMENTAL_BY_TIME_RANGE (
        TIME_COLUMN utc_datetime,
        BATCH_SIZE 1),
    --There are some duplicates across year/month noted on lichess
    -- GRAIN (variant, year, month, site, game_id),
    TABLE_FORMAT 'hive',
    STORAGE_FORMAT 'parquet',
    PARTITIONED_BY (variant, year, month),
    CLUSTERED_BY (utc_datetime));

SELECT 
    t.variant AS variant,
    t.year,
    t.month,
    t.Event AS event,
    FILTER(['lichess.org'], lambda s: s IN t.Site)[1] AS site,
    REPLACE(t.Site, 'https://lichess.org/', '') AS game_id,
    t.White AS white,
    t.Black AS black,
    t.Result AS result,
    REPLACE(t.UTCDate, '.', '-')::DATE + t.UTCTime::TIME AS utc_datetime,
    REPLACE(t.UTCDate, '.', '-')::DATE AS utc_date,  -- There is also a "Date" field?
    t.UTCTime::TIME AS utc_time,
    --Lichess uses Glicko2, not Elo, despite the name.
    NULLIF(t.WhiteElo, '?')::INT AS white_rating,
    NULLIF(t.BlackElo, '?')::INT AS black_rating,
    t.WhiteRatingDiff::INT AS white_rating_diff,
    t.BlackRatingDiff::INT AS black_rating_diff,
    t.ECO AS eco_code,
    t.Opening AS opening,
    NULLIF(t.TimeControl, '-') AS time_control,
    t.Termination AS termination,
    t.BlackTitle AS black_title,
    t.WhiteTitle AS white_title,
    t.has_eval AS is_analyzed,
    t.has_clk AS has_clk,
    t.has_clkc AS has_clkc,
    --For broadcasts
    t.BlackFideID AS black_fide_id,
    t.WhiteFideID AS white_fide_id,
    t.StudyName AS study_name,
    t.ChapterName AS chapter_name,
    t.BroadcastName AS broadcast_name,
    t.BroadcastURL AS broadcast_url,
    t.GameURL AS game_url,
    --Chess960
    COALESCE(t.SetUp, '0')::BOOLEAN AS set_up,
    COALESCE(t.FEN, 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1') AS fen
FROM READ_PARQUET(
    @project_folder || '/stage/lichess/headers/**/*.parquet',
    HIVE_PARTITIONING = TRUE,
    UNION_BY_NAME = TRUE,
    HIVE_TYPES = {
        'variant': VARCHAR, 
        'year': INTEGER, 
        'month': INTEGER
    }) 
    AS t
WHERE 
    MAKE_DATE(t.year, t.month, 1) BETWEEN DATE_TRUNC('month', @start_date) AND DATE_TRUNC('month', @end_date)
    AND t.UTCDate BETWEEN STRFTIME(@start_date, '%Y.%m.%d') AND STRFTIME(@end_date, '%Y.%m.%d');
