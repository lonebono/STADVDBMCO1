import psycopg2
import sys
import logging

# --- Local DB Configuration ---
DB_CONFIG = {
    "dbname": "mco1_imdb",
    "user": "postgres",
    "password": "EleanorKat17;",
    "host": "localhost",
    "port": "5432"
}

# --- Schema Definitions ---
SOURCE_SCHEMA = 'public'
STAGING_SCHEMA = 'staging'
DWH_SCHEMA = 'dwh'

SOURCE_TABLES = [
    'title_basics',
    'title_ratings',
    'title_akas'
]

# --- DWH Table Definitions ---
DWH_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_region (
    region_id SERIAL PRIMARY KEY,
    region_code TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_language (
    language_id SERIAL PRIMARY KEY,
    language_code TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_genre (
    genre_id SERIAL PRIMARY KEY,
    genre_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_time (
    time_id SERIAL PRIMARY KEY,
    year INTEGER UNIQUE NOT NULL,
    decade INTEGER
);

CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_title (
    title_id SERIAL PRIMARY KEY,
    tconst TEXT UNIQUE NOT NULL,
    primary_title TEXT,
    title_type TEXT,
    runtime_minutes INTEGER,
    average_rating NUMERIC(3,1),
    num_votes INTEGER,
    localization_count INTEGER,
    num_regions INTEGER,
    num_languages INTEGER
);

CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.bridge_title_genre (
    title_id INTEGER NOT NULL REFERENCES {DWH_SCHEMA}.dim_title(title_id),
    genre_id INTEGER NOT NULL REFERENCES {DWH_SCHEMA}.dim_genre(genre_id),
    PRIMARY KEY (title_id, genre_id)
);

CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.fact_film_version (
    version_id SERIAL PRIMARY KEY,
    title_id INTEGER NOT NULL REFERENCES {DWH_SCHEMA}.dim_title(title_id),
    region_id INTEGER NOT NULL REFERENCES {DWH_SCHEMA}.dim_region(region_id),
    language_id INTEGER NOT NULL REFERENCES {DWH_SCHEMA}.dim_language(language_id),
    time_id INTEGER NOT NULL REFERENCES {DWH_SCHEMA}.dim_time(time_id),
    is_original_title BOOLEAN
);
"""

# --- Shchema and Table Setup ---
def setup_schemas_and_dwh_tables(conn):
    logging.info("Setting up schemas and DWH tables...")
    try:
        with conn.cursor() as cur:
            # create schemas
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DWH_SCHEMA};")
            conn.commit()
            logging.info("Schemas ensured.")
            # create DWH tables
            cur.execute(DWH_TABLE_SQL)
            conn.commit()
            logging.info("DWH tables created or already exist.")
        conn.commit()
        logging.info("--- Step 0: Schema Setup COMPLETED ---")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error in Step 0 (Setup): {error}")
        conn.rollback()
        raise

# --- Extract and Load ---
def run_step_1_extract_load(conn):
    logging.info("Starting Step 1: Extract and Load...")
    try:
        with conn.cursor() as cursor:
            for table_name in SOURCE_TABLES:
                source_table = f"{SOURCE_SCHEMA}.{table_name}"
                staging_table = f"{STAGING_SCHEMA}.{table_name}"
                
                logging.info(f"Copying {source_table} -> {staging_table}...")
                
                # 1. Drop the old staging table if it exists
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table} CASCADE;")
                # 2. Create the new staging table as a copy of the source
                sql = f"CREATE TABLE {staging_table} AS SELECT * FROM {source_table};"
                cursor.execute(sql)
                
            logging.info("All source tables copied to staging.")
            
        conn.commit()
        logging.info("--- Step 1: Extract & Load COMPLETED ---")
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error in Step 1 (Extract & Load): {error}")
        conn.rollback()
        raise

# --- DWH Transformation ---
TRANSFORM_SQL = f"""
-- 1. POPULATE DIMENSION TABLES
TRUNCATE TABLE {DWH_SCHEMA}.dim_region RESTART IDENTITY CASCADE;
TRUNCATE TABLE {DWH_SCHEMA}.dim_language RESTART IDENTITY CASCADE;
TRUNCATE TABLE {DWH_SCHEMA}.dim_genre RESTART IDENTITY CASCADE;
TRUNCATE TABLE {DWH_SCHEMA}.dim_time RESTART IDENTITY CASCADE;
TRUNCATE TABLE {DWH_SCHEMA}.dim_title RESTART IDENTITY CASCADE;

-- Populate dim_region 
INSERT INTO {DWH_SCHEMA}.dim_region (region_code) VALUES ('UNK') ON CONFLICT (region_code) DO NOTHING;
INSERT INTO {DWH_SCHEMA}.dim_region (region_code)
SELECT DISTINCT region FROM {STAGING_SCHEMA}.title_akas
WHERE region IS NOT NULL AND region != '\\N'
ON CONFLICT (region_code) DO NOTHING;

-- Populate dim_language
INSERT INTO {DWH_SCHEMA}.dim_language (language_code) VALUES ('UNK') ON CONFLICT (language_code) DO NOTHING;
INSERT INTO {DWH_SCHEMA}.dim_language (language_code)
SELECT DISTINCT language FROM {STAGING_SCHEMA}.title_akas
WHERE language IS NOT NULL AND language != '\\N'
ON CONFLICT (language_code) DO NOTHING;

-- Populate dim_genre
INSERT INTO {DWH_SCHEMA}.dim_genre (genre_name) VALUES ('Unknown') ON CONFLICT (genre_name) DO NOTHING;
INSERT INTO {DWH_SCHEMA}.dim_genre (genre_name)
SELECT DISTINCT unnest(string_to_array(genres, ','))
FROM {STAGING_SCHEMA}.title_basics
WHERE genres IS NOT NULL AND genres != '\\N'
ON CONFLICT (genre_name) DO NOTHING;

-- Populate dim_time
INSERT INTO {DWH_SCHEMA}.dim_time (year, decade) VALUES (-1, -1) ON CONFLICT (year) DO NOTHING;
INSERT INTO {DWH_SCHEMA}.dim_time (year, decade)
SELECT
    DISTINCT(CAST(startYear AS INTEGER)) AS year,
    FLOOR(CAST(startYear AS INTEGER) / 10) * 10 AS decade
FROM (
    SELECT startYear
    FROM {STAGING_SCHEMA}.title_basics
    WHERE startYear IS NOT NULL AND startYear != '\\N' AND startYear ~ '^[0-9]+$'
) AS numeric_years
ON CONFLICT (year) DO NOTHING;

-- Populate dim_title
WITH akas_agg AS (
    SELECT
        titleIdentifier AS tconst,
        COUNT(*) AS localization_count,
        COUNT(DISTINCT region) AS num_regions,
        COUNT(DISTINCT language) AS num_languages
    FROM {STAGING_SCHEMA}.title_akas
    GROUP BY titleIdentifier
)
INSERT INTO {DWH_SCHEMA}.dim_title (
    tconst, primary_title, title_type, runtime_minutes,
    average_rating, num_votes,
    localization_count, num_regions, num_languages
)
SELECT
    tb.tconst,
    tb.primaryTitle,
    tb.titleType,
    CASE
        WHEN tb.runtimeMinutes = '\\N' THEN NULL
        ELSE CAST(tb.runtimeMinutes AS INTEGER)
    END,
    tr.averageRating,
    tr.numVotes,
    COALESCE(aa.localization_count, 0),
    COALESCE(aa.num_regions, 0),
    COALESCE(aa.num_languages, 0)
FROM {STAGING_SCHEMA}.title_basics tb
LEFT JOIN {STAGING_SCHEMA}.title_ratings tr ON tb.tconst = tr.tconst
LEFT JOIN akas_agg aa ON tb.tconst = aa.tconst
WHERE tb.titleType IN ('movie', 'tvMovie', 'tvSeries', 'tvMiniSeries', 'tvShort', 'short')
ON CONFLICT (tconst) DO NOTHING;

-- 2. POPULATE BRIDGE TABLE
TRUNCATE TABLE {DWH_SCHEMA}.bridge_title_genre CASCADE;

INSERT INTO {DWH_SCHEMA}.bridge_title_genre (title_id, genre_id)
SELECT
    dt.title_id,
    COALESCE(dg.genre_id, (SELECT genre_id FROM {DWH_SCHEMA}.dim_genre WHERE genre_name = 'Unknown'))
FROM {STAGING_SCHEMA}.title_basics tb
CROSS JOIN LATERAL unnest(string_to_array(tb.genres, ',')) AS g(genre_name)
JOIN {DWH_SCHEMA}.dim_title dt ON tb.tconst = dt.tconst
LEFT JOIN {DWH_SCHEMA}.dim_genre dg ON g.genre_name = dg.genre_name
WHERE tb.genres IS NOT NULL AND tb.genres != '\\N'
ON CONFLICT (title_id, genre_id) DO NOTHING;


-- 3. POPULATE FACT TABLE
TRUNCATE TABLE {DWH_SCHEMA}.fact_film_version RESTART IDENTITY CASCADE;

WITH unk_region AS (
    SELECT region_id FROM {DWH_SCHEMA}.dim_region WHERE region_code = 'UNK'
), unk_lang AS (
    SELECT language_id FROM {DWH_SCHEMA}.dim_language WHERE language_code = 'UNK'
), unk_time AS (
    SELECT time_id FROM {DWH_SCHEMA}.dim_time WHERE year = -1
)
INSERT INTO {DWH_SCHEMA}.fact_film_version (
    title_id,
    region_id,
    language_id,
    time_id,
    is_original_title
)
SELECT
    dt.title_id,
    COALESCE(dr.region_id, (SELECT region_id FROM unk_region)),
    COALESCE(dl.language_id, (SELECT language_id FROM unk_lang)),
    COALESCE(dtime.time_id, (SELECT time_id FROM unk_time)),
    CASE
        WHEN ta.isOriginalTitle = '1' THEN TRUE
        ELSE FALSE
    END
FROM {STAGING_SCHEMA}.title_akas ta
JOIN {DWH_SCHEMA}.dim_title dt ON ta.titleIdentifier = dt.tconst
LEFT JOIN {STAGING_SCHEMA}.title_basics tb ON ta.titleIdentifier = tb.tconst
LEFT JOIN {DWH_SCHEMA}.dim_time dtime
    ON (tb.startYear != '\\N' AND tb.startYear ~ '^[0-9]+$' AND CAST(tb.startYear AS INTEGER) = dtime.year)
LEFT JOIN {DWH_SCHEMA}.dim_region dr ON ta.region = dr.region_code
LEFT JOIN {DWH_SCHEMA}.dim_language dl ON ta.language = dl.language_code;
"""

def run_step_2_transform_dwh(conn):
    logging.info("Starting Step 2: Transform DWH...")
    try:
        with conn.cursor() as cursor:
            logging.info("Executing full transform script...")
            cursor.execute(TRANSFORM_SQL)
            logging.info("Transform script executed.")
            
        # If all queries succeeded, commit the transaction
        conn.commit()
        logging.info("--- Step 2: Transform (DWH) COMPLETED ---")
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error in Step 2 (Transform): {error}")
        logging.error("--- TRANSACTION ROLLED BACK ---")
        conn.rollback()
        raise

# --- Main ETL Process ---
def main():
    conn = None
    try:
        logging.info("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("Connection successful.")
        
        # Run setup first to ensure schemas and DWH tables exist
        setup_schemas_and_dwh_tables(conn)
        # Run Step 1: Extract and Load
        run_step_1_extract_load(conn)
        # Run Step 2: Transform DWH
        run_step_2_transform_dwh(conn)

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"ETL process FAILED: {error}")
        sys.exit(1) # Exit with an error code
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()