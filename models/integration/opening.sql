MODEL (
    NAME integration.opening,
    KIND FULL,
    GRAIN opening);

WITH parts AS (
    SELECT
        *,
        STRING_SPLIT_REGEX(t.name, '(: )|(, )') AS parts
    FROM seed.eco t)

SELECT DISTINCT
    @GENERATE_SURROGATE_KEY(t.name) AS opening_key,
    t.name AS opening,
    t.eco[1] AS eco_volume,
    t.eco AS eco_code,
    t.parts[1] AS family,
    --Note that these should be taken in conjunction with the preceeding column
    --e.g., a variation may be simply listed as "Main Line" for many families
    t.parts[2] AS variation,
    t.parts[3] AS sub_variation,
    NULLIF(ARRAY_TO_STRING(t.parts[4:], ', '), '') AS line
FROM parts t;
