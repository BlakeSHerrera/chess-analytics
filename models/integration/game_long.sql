MODEL (
    NAME integration.game_long,
    KIND VIEW);

SELECT
    *,
    'White' AS color,
    g.player_key_white AS player_key,
    g.title_key_white AS title_key,
    g.white_elo AS elo,
    g.white_rating_diff AS rating_diff,
    r.result = '1-0' AS won,
    r.result = '1/2-1/2' AS drew,
    r.result = '0-1' AS lost,
    'Black' AS opponent_color,
    g.player_key_black AS player_key_opponent,
    g.title_key_black AS title_key_opponent,
    g.black_elo AS opponent_elo,
    g.black_rating_diff AS opponent_rating_diff
FROM 
    integration.game g
    LEFT JOIN integration.result r USING (result_key)

UNION ALL
SELECT
    *,
    'Black' AS color,
    g.player_key_black AS player_key,
    g.title_key_black AS title_key,
    g.black_elo AS elo,
    g.black_rating_diff AS rating_diff,
    r.result = '0-1' AS won,
    r.result = '1/2-1/2' AS drew,
    r.result = '1-0' AS lost,
    'White' AS opponent_color,
    g.player_key_white AS player_key_opponent,
    g.title_key_white AS title_key_opponent,
    g.white_elo AS opponent_elo,
    g.white_rating_diff AS opponent_rating_diff
FROM 
    integration.game g
    LEFT JOIN integration.result r USING (result_key);
