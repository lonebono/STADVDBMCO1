import psycopg2
import sys
import logging

# --- Local DB Configuration ---
DB_CONFIG = {
    "dbname": "mco1_imdb",
    "user": "postgres",
    "password": "12EleanorKat17;",
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

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"ETL process FAILED: {error}")
        sys.exit(1) # Exit with an error code
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()