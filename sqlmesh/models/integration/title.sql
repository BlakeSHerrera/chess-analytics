MODEL (
    NAME integration.title,
    KIND FULL);

SELECT
    @GENERATE_SURROGATE_KEY(title) AS title_key,
    *
FROM seed.title;
