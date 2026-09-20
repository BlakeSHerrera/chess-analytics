MODEL (
    NAME seed.date,
    KIND FULL,
    GRAIN date);

SELECT
    r.range AS date,
    EXTRACT(YEAR FROM r.range) AS year_number,
    EXTRACT(QUARTER FROM r.range) AS quarter_number,
    EXTRACT(MONTH FROM r.range) AS month_number,
    EXTRACT(WEEK FROM r.range) AS week_number,
    EXTRACT(DAY FROM r.range) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM r.range) AS day_of_week,
    EXTRACT(DAYOFWEEK FROM r.range) IN (0, 6) AS is_weekend
FROM RANGE(
    '2012-01-01'::DATE, 
    MAKE_DATE(YEAR(NOW()), 12, 31), 
    INTERVAL 1 DAY)
    AS r;
