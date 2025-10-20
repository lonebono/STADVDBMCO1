CREATE SCHEMA IF NOT EXISTS imdb_model;
SET search_path TO imdb_model;

-- cleaning, only filtered titleType = 'movie'
CREATE TEMP TABLE title_akas_clean AS
SELECT
    titleId,
    region,
    language,
    isoriginaltitle
FROM public.title_akas a
JOIN public.title_basics b
    ON a.titleId = b.tconst
WHERE b.titleType = 'movie'

-- cleaning, only filtered titleType = 'movie' and removed nulls
CREATE TEMP TABLE title_basics_clean AS
SELECT
    tconst,
    primaryTitle,
    originalTitle,
    startYear,
    runtimeMinutes,
    genres
FROM public.title_basics
WHERE titleType = 'movie'
  AND startYear IS NOT NULL
  AND genres IS NOT NULL;

CREATE TABLE IF NOT EXISTS dim_region (
    region_id SERIAL PRIMARY KEY,
    region_code TEXT UNIQUE,
    region_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_language (
    language_id SERIAL PRIMARY KEY,
    language_code TEXT UNIQUE,
    language_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_id SERIAL PRIMARY KEY,
    genre_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    start_year INTEGER UNIQUE,
    decade INTEGER
);

CREATE TABLE IF NOT EXISTS fact_localization (
    fact_id SERIAL PRIMARY KEY,
    tconst TEXT NOT NULL,
    region_id INTEGER REFERENCES dim_region(region_id),
    language_id INTEGER REFERENCES dim_language(language_id),
    is_original_title BOOLEAN,
    start_year INTEGER,
    average_rating NUMERIC(3,1),
    num_votes INTEGER,
    localization_count INTEGER,
    num_regions INTEGER,
    num_languages INTEGER,
    FOREIGN KEY (start_year) REFERENCES dim_time(start_year)
);

CREATE TABLE IF NOT EXISTS fact_genre (
    fact_id INTEGER REFERENCES fact_localization(fact_id),
    genre_id INTEGER REFERENCES dim_genre(genre_id),
    PRIMARY KEY (fact_id, genre_id)
);

INSERT INTO dim_region (region_code)
SELECT DISTINCT region
FROM title_akas_clean
ON CONFLICT (region_code) DO NOTHING;

INSERT INTO dim_language (language_code)
SELECT DISTINCT language
FROM title_akas_clean
ON CONFLICT (language_code) DO NOTHING;

INSERT INTO dim_genre (genre_name)
SELECT DISTINCT UNNEST(STRING_TO_ARRAY(genres, ',')) AS genre_name
FROM title_basics_clean
ON CONFLICT (genre_name) DO NOTHING;

INSERT INTO dim_time (start_year, decade)
SELECT DISTINCT startYear,
       (startYear / 10) * 10 AS decade
FROM title_basics_clean
ON CONFLICT (start_year) DO NOTHING;

CREATE TEMP TABLE temp_akas_agg AS
SELECT
    titleId,
    COUNT(*) FILTER (WHERE isoriginaltitle = false) AS localization_count,
    COUNT(DISTINCT region) AS num_regions,
    COUNT(DISTINCT language) AS num_languages
FROM title_akas_clean
GROUP BY titleId;

INSERT INTO fact_localization (
    tconst,
    region_id,
    language_id,
    is_original_title,
    start_year,
    average_rating,
    num_votes,
    localization_count,
    num_regions,
    num_languages
)
SELECT
    b.tconst,
    r.region_id,
    l.language_id,
    a.isoriginaltitle,
    b.startYear,
    rt.averageRating,
    rt.numVotes,
    ta.localization_count,
    ta.num_regions,
    ta.num_languages
FROM title_akas_clean a
JOIN title_basics_clean b ON a.titleId = b.tconst
LEFT JOIN public.title_ratings rt ON b.tconst = rt.tconst
LEFT JOIN dim_region r ON a.region = r.region_code
LEFT JOIN dim_language l ON a.language = l.language_code
LEFT JOIN temp_akas_agg ta ON a.titleId = ta.titleId;

INSERT INTO fact_genre (fact_id, genre_id)
SELECT f.fact_id, g.genre_id
FROM fact_localization f
JOIN title_basics_clean b ON f.tconst = b.tconst
JOIN dim_genre g ON g.genre_name = ANY(STRING_TO_ARRAY(b.genres, ','));
