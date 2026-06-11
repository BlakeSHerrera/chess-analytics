MODEL (
    NAME integration.rating_bracket,
    KIND FULL,
    GRAIN class);

SELECT
    @GENERATE_SURROGATE_KEY(class) AS rating_bracket_key,
    *
FROM seed.rating_bracket;
