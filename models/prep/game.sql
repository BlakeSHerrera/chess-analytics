MODEL (
    NAME prep.game,
    KIND INCREMENTAL_BY_TIME_RANGE (
        TIME_COLUMN utc_date),
    ALLOW_PARTIALS TRUE,
    GRAIN (site, game_id),
    TABLE_FORMAT 'hive',
    STORAGE_FORMAT 'parquet',
    PARTITIONED_BY (year, month),
    CLUSTERED_BY (utc_date),
    COLUMN_DESCRIPTIONS (
        event = 'The event for the game, such as an individual game or tournament. Contains the rating format, game format, and time category.',
        site = 'The location or website where the game was played. For lichess games, this should always be lichess.org.',
        game_id = 'Along with the site, this forms a unique identifier for the game.',
        white = 'The name or username of the player with the white pieces.',
        black = 'The name or username of the player with the black pieces.',
        result = 'The outcome of the game. 1-0 is a win for White, 0-1 is a win for Black, 1/2-1/2 is a draw, and * indicates the game was terminated before it began.',
        utc_date = 'The Universal Coordinated Time (UTC) date when the game began.',
        utc_time = 'The Universal Coordinated Time (UTC) time when the game began.',
        white_elo = 'The Elo rating of White. Note that players begin unrated, indicated by a NULL value.',
        black_elo = 'The Elo rating of Black. Note that players begin unrated, indicated by a NULL value.',
        white_rating_diff = 'The number of Elo rating points White gained or lost from this match. A larger Elo diff means more points are at stake for an upset.',
        black_rating_diff = 'The number of Elo rating points Black gained or lost from this match. A larger Elo diff means more points are at stake for an upset.',
        eco_code = 'Code from the Encyclopedia of Chess Openings, beginning with a letter and followed by a two-digit numeric code.',
        opening = 'The opening of the game. Not that any one opening is a branching path of multiple openings; this reflects the final leaf node. Openings are also not strictly hierarchical and can transposition. An opening name is also not unique to a one position.',
        time_control = 'The time control of the game, formatted as initial+incrmental for lichess. Correspondence games have a time control measured in days per move, so this field is blank for such games.',
        termination = 'Whether the game was decided by "Normal" game rules (checkmate, stalemate, resignation, agree to draw, threefold repetition, 50-move rule, insufficient material), "Time forfeit", or "Rules infraction".',
        black_title = 'The title for the Black player, if any. Bots have a title of "BOT".',
        white_title = 'The title for the White player, if any. Bots have a title of "BOT".',
        ingest_timestamp = 'The date and time when this row was loaded into the database.',
        year = 'The year of the game (used in Hive partitioning).',
        month = 'The month of the game (used in Hive partitioning).'));

SELECT 
    t.Event AS event,
    FILTER(['lichess.org'], s -> s IN t.Site)[1] AS site,
    REPLACE(t.Site, 'https://lichess.org/', '') AS game_id,
    t.White AS white,
    t.Black AS black,
    t.Result AS result,
    REPLACE(t.UTCDate, '.', '-')::DATE AS utc_date,  -- There is also a "Date" field?
    t.UTCTime::TIME AS utc_time,
    NULLIF(t.WhiteElo, '?')::INT AS white_elo,
    NULLIF(t.BlackElo, '?')::INT AS black_elo,
    t.WhiteRatingDiff::INT AS white_rating_diff,
    t.BlackRatingDiff::INT AS black_rating_diff,
    t.ECO AS eco_code,
    t.Opening AS opening,
    NULLIF(t.TimeControl, '-') AS time_control,
    t.Termination AS termination,
    t.BlackTitle AS black_title,
    t.WhiteTitle AS white_title,
    NOW() AS ingest_timestamp,
    t.year,
    t.month
FROM READ_PARQUET(
    @project_folder || '/lichess_standard_rated_headers/**/*.parquet', 
    HIVE_PARTITIONING = TRUE,
    UNION_BY_NAME = TRUE) 
    AS t
WHERE REPLACE(t.UTCDate, '.', '-')::DATE BETWEEN @start_date AND @end_date;
