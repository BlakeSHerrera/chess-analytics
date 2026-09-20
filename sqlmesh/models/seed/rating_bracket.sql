MODEL (
    NAME seed.rating_bracket,
    KIND SEED (
        PATH '$root/seeds/rating_bracket.csv'),
    GRAIN class,
    COLUMN_DESCRIPTIONS (
        class = 'The rating class, approximately broken into buckets spanning 200 rating points.',
        rating_min = 'The minimum rating for the rating class.',
        rating_max = 'The maximum rating for the rating class (inclusive).'));
