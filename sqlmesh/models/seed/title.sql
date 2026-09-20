MODEL (
    NAME seed.title,
    KIND SEED (
        PATH '$root/seeds/title.csv'),
    GRAIN (organization, title_type, title),
    COLUMN_DESCRIPTIONS (
        title_short = 'A short abbreviation for the title.',
        title = 'The full name of the title.',
        organization = 'The organizing body awarding the title.',
        title_type = 'Distinguishes who the title is available, e.g., Open or Women.',
        internal_rank = 'Rank within the organization and title type.'));
