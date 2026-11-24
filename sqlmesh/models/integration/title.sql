MODEL (
    NAME integration.title,
    KIND VIEW);

SELECT
    @GENERATE_SURROGATE_KEY(title) AS title_key,
    *
FROM seed.title;
