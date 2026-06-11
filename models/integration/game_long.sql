MODEL (
    NAME integration.game_long,
    KIND VIEW);

SELECT
    *,
    'White' AS color,
    player_key_white AS player_key,
    title_key_white AS title_key,
    white_elo AS elo,
    white_rating_diff AS rating_diff,
    'Black' AS opponent_color,
    player_key_black AS player_key_opponent,
    title_key_black AS title_key_opponent,
    black_elo AS opponent_elo,
    black_rating_diff AS opponent_rating_diff
FROM integration.game

UNION ALL
SELECT
    *,
    'Black' AS color,
    player_key_black AS player_key,
    title_key_black AS title_key,
    black_elo AS elo,
    black_rating_diff AS rating_diff,
    'White' AS opponent_color,
    player_key_white AS player_key_opponent,
    title_key_white AS title_key_opponent,
    white_elo AS opponent_elo,
    white_rating_diff AS opponent_rating_diff
FROM integration.game;
