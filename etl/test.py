import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ================= Configuration =================
load_dotenv()

LOCAL_CONN = f"postgresql+psycopg2://{os.getenv('LOCAL_USER')}:{os.getenv('LOCAL_PASSWORD')}@" \
             f"{os.getenv('LOCAL_HOST')}:{os.getenv('LOCAL_PORT')}/{os.getenv('LOCAL_DB')}"

TARGET_CONN = os.getenv("DATABASE_URL")

local_engine = create_engine(LOCAL_CONN)
target_engine = create_engine(TARGET_CONN)

# ================= Schema Setup =================
def create_tables_and_insert_unknowns(engine):
    with engine.begin() as conn:
        # Dimensions
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_region (
                region_id SERIAL PRIMARY KEY,
                region_name TEXT UNIQUE
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_language (
                language_id SERIAL PRIMARY KEY,
                language_name TEXT UNIQUE
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_genre (
                genre_id SERIAL PRIMARY KEY,
                genre_name TEXT UNIQUE
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_time (
                time_id SERIAL PRIMARY KEY,
                year INTEGER UNIQUE,
                decade INTEGER
            );
        """))

        # Fact & Bridge
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_film_version (
                fact_id SERIAL PRIMARY KEY,
                tconst TEXT NOT NULL,
                region_id INTEGER NOT NULL DEFAULT -1,
                language_id INTEGER NOT NULL DEFAULT -1,
                time_id INTEGER NOT NULL DEFAULT -1,
                is_original_title BOOLEAN,
                average_rating NUMERIC(3,1),
                num_votes INTEGER
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_genre_bridge (
                fact_id INTEGER NOT NULL,
                genre_id INTEGER NOT NULL,
                PRIMARY KEY (fact_id, genre_id)
            );
        """))

        # Insert "Unknown" (-1) rows
        conn.execute(text("""
            INSERT INTO dim_region (region_id, region_name) VALUES (-1, 'Unknown') ON CONFLICT (region_id) DO NOTHING;
        """))
        conn.execute(text("""
            INSERT INTO dim_language (language_id, language_name) VALUES (-1, 'Unknown') ON CONFLICT (language_id) DO NOTHING;
        """))
        conn.execute(text("""
            INSERT INTO dim_genre (genre_id, genre_name) VALUES (-1, 'Unknown') ON CONFLICT (genre_id) DO NOTHING;
        """))
        conn.execute(text("""
            INSERT INTO dim_time (time_id, year, decade) VALUES (-1, NULL, NULL) ON CONFLICT (time_id) DO NOTHING;
        """))
    print("Tables created and 'Unknown' rows inserted.")
    
def apply_foreign_keys(engine):
    """Add foreign key constraints after tables and data exist."""
    with engine.begin() as conn:
        # Fact table FKs
        conn.execute(text("""
            ALTER TABLE fact_film_version 
            DROP CONSTRAINT IF EXISTS fk_region;
        """))
        conn.execute(text("""
            ALTER TABLE fact_film_version 
            ADD CONSTRAINT fk_region 
            FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
            ON UPDATE CASCADE ON DELETE RESTRICT;
        """))

        conn.execute(text("""
            ALTER TABLE fact_film_version 
            DROP CONSTRAINT IF EXISTS fk_language;
        """))
        conn.execute(text("""
            ALTER TABLE fact_film_version 
            ADD CONSTRAINT fk_language 
            FOREIGN KEY (language_id) REFERENCES dim_language(language_id)
            ON UPDATE CASCADE ON DELETE RESTRICT;
        """))

        conn.execute(text("""
            ALTER TABLE fact_film_version 
            DROP CONSTRAINT IF EXISTS fk_time;
        """))
        conn.execute(text("""
            ALTER TABLE fact_film_version 
            ADD CONSTRAINT fk_time 
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
            ON UPDATE CASCADE ON DELETE RESTRICT;
        """))

        # Bridge table FKs
        conn.execute(text("""
            ALTER TABLE fact_genre_bridge 
            DROP CONSTRAINT IF EXISTS fact_genre_bridge_fact_id_fkey;
        """))
        conn.execute(text("""
            ALTER TABLE fact_genre_bridge 
            ADD CONSTRAINT fact_genre_bridge_fact_id_fkey 
            FOREIGN KEY (fact_id) REFERENCES fact_film_version(fact_id)
            ON DELETE CASCADE;
        """))

        conn.execute(text("""
            ALTER TABLE fact_genre_bridge 
            DROP CONSTRAINT IF EXISTS fact_genre_bridge_genre_id_fkey;
        """))
        conn.execute(text("""
            ALTER TABLE fact_genre_bridge 
            ADD CONSTRAINT fact_genre_bridge_genre_id_fkey 
            FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
            ON DELETE RESTRICT;
        """))

    print("Foreign key constraints applied.")

# ================= Cleaning =================
def clean_akas_chunk(df):
    df = df.rename(columns=str.lower)
    df = df[["titleid", "ordering", "region", "language", "isoriginaltitle"]]
    df["region"] = df["region"].replace({"\\N": None})
    df["language"] = df["language"].replace({"\\N": None})
    df["isoriginaltitle"] = df["isoriginaltitle"].astype(str).str.lower().isin(["1","t","true"])
    df = df.sort_values(["titleid", "ordering"]).drop_duplicates(subset=["titleid", "ordering"])
    return df

def clean_basics(df):
    df = df.rename(columns=str.lower)
    df = df.replace({"\\N": None})
    df["startyear"] = pd.to_numeric(df["startyear"], errors="coerce")
    df["genres"] = df["genres"].fillna("Unknown")
    return df

def clean_ratings(df):
    df = df.rename(columns=str.lower)
    df["averagerating"] = pd.to_numeric(df["averagerating"], errors="coerce").fillna(0)
    df["numvotes"] = pd.to_numeric(df["numvotes"], errors="coerce").fillna(0).astype(int)
    return df

def insert_dim_table(df, table_name, engine, pk_col):
    """
    Inserts dimension data into Postgres safely.
    Avoids duplicate primary key errors.
    """
    df = df.copy()
    columns = df.columns.tolist()
    values_placeholder = ", ".join([f"%({col})s" for col in columns])
    insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({values_placeholder})
        ON CONFLICT ({pk_col}) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(insert_sql), df.to_dict(orient="records"))

# ================= Extraction in chunks =================
def extract_table_in_chunks(table_name, engine, process_chunk_fn, chunksize=100_000, limit=None):
    query = f"SELECT * FROM {table_name}"
    if limit:
        query += f" LIMIT {limit}"
    with engine.connect() as conn:
        for chunk in pd.read_sql(query, conn, chunksize=chunksize):
            process_chunk_fn(chunk)

# ================= Build dimensions incrementally =================
def build_dimensions_incrementally(akas_temp_file, basics_temp_file):
    """Build dimensions by reading files in chunks to avoid memory issues"""
    
    # Initialize sets for unique values
    regions = set()
    languages = set()
    genres = set()
    years = set()
    
    # Process AKAs in chunks to get regions and languages
    print("Processing AKAs for regions and languages...")
    for chunk in pd.read_csv(akas_temp_file, chunksize=100000):
        regions.update(chunk["region"].dropna().unique())
        languages.update(chunk["language"].dropna().unique())
    
    # Process basics in chunks to get genres and years
    print("Processing basics for genres and years...")
    for chunk in pd.read_csv(basics_temp_file, chunksize=100000):
        # Genres
        chunk_genres = chunk["genres"].str.split(",").explode().str.strip().str.title()
        genres.update(chunk_genres.unique())
        
        # Years
        valid_years = chunk["startyear"].dropna()
        years.update(valid_years.unique())
    
    # Create dimension DataFrames
    dim_region = pd.DataFrame({"region_name": list(regions)})
    dim_region["region_id"] = range(1, len(dim_region) + 1)
    
    dim_language = pd.DataFrame({"language_name": list(languages)})
    dim_language["language_id"] = range(1, len(dim_language) + 1)
    
    dim_genre = pd.DataFrame({"genre_name": list(genres)})
    dim_genre["genre_id"] = range(1, len(dim_genre) + 1)
    
    dim_time = pd.DataFrame({"year": list(years)})
    dim_time["decade"] = (dim_time["year"] // 10) * 10
    dim_time["time_id"] = range(1, len(dim_time) + 1)
    
    return dim_region, dim_language, dim_genre, dim_time

# ================= ETL =================
def run_etl(limit=None):
    create_tables_and_insert_unknowns(target_engine)

    # Temporary files for storing cleaned chunks
    akas_temp_file = "akas_temp.csv"
    basics_temp_file = "basics_temp.csv"
    
    # Remove temp files if they exist
    for temp_file in [akas_temp_file, basics_temp_file]:
        if os.path.exists(temp_file):
            os.remove(temp_file)

    # 1️⃣ Extract & clean title_akas in chunks
    print("Extracting and cleaning title_akas...")
    def process_akas_chunk(chunk):
        clean_chunk = clean_akas_chunk(chunk)
        clean_chunk.to_csv(akas_temp_file, mode="a", index=False, header=not os.path.exists(akas_temp_file))

    extract_table_in_chunks("title_akas", local_engine, process_akas_chunk, limit=limit)

    # 2️⃣ Extract & clean title_basics in chunks
    print("Extracting and cleaning title_basics...")
    def process_basics_chunk(chunk):
        clean_chunk = clean_basics(chunk)
        clean_chunk.to_csv(basics_temp_file, mode="a", index=False, header=not os.path.exists(basics_temp_file))

    extract_table_in_chunks("title_basics", local_engine, process_basics_chunk, limit=limit)

    # 3️⃣ Extract & clean ratings fully (usually smaller, can fit in memory)
    print("Extracting and cleaning ratings...")
    ratings = clean_ratings(pd.read_sql("SELECT * FROM title_ratings", local_engine))

    # 4️⃣ Build dimensions incrementally
    print("Building dimensions...")
    dims = build_dimensions_incrementally(akas_temp_file, basics_temp_file)
    dim_region, dim_language, dim_genre, dim_time = dims

    # Load dimensions
    print("Loading dimensions to target database...")
    dim_region.to_sql("dim_region", target_engine, if_exists="append", index=False, method="multi")
    dim_language.to_sql("dim_language", target_engine, if_exists="append", index=False, method="multi")
    dim_genre.to_sql("dim_genre", target_engine, if_exists="append", index=False, method="multi")
    dim_time.to_sql("dim_time", target_engine, if_exists="append", index=False, method="multi")

    # Create mapping dictionaries
    region_map = dict(zip(dim_region["region_name"], dim_region["region_id"]))
    lang_map = dict(zip(dim_language["language_name"], dim_language["language_id"]))
    time_map = dict(zip(dim_time["year"], dim_time["time_id"]))
    genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_id"]))

    # 5️⃣ Build fact table and bridge in chunks
    print("Building fact table and bridge table...")
    fact_id_counter = 1

    def process_fact_chunk(basics_chunk):
        nonlocal fact_id_counter
        
        # Process basics chunk
        merged = basics_chunk.merge(ratings, on="tconst", how="left")
        
        # Get the first aka for each tconst by reading akas in chunks
        akas_first = []
        for aka_chunk in pd.read_csv(akas_temp_file, chunksize=100000):
            first_akas = aka_chunk.groupby("titleid").first().reset_index()
            akas_first.append(first_akas)
        
        if akas_first:
            all_first_akas = pd.concat(akas_first, ignore_index=True)
            # Group again in case there were duplicates across chunks
            all_first_akas = all_first_akas.groupby("titleid").first().reset_index()
            merged = merged.merge(all_first_akas.rename(columns={"titleid": "tconst"}), on="tconst", how="left")
        
        # Map dimensions
        merged["region_id"] = merged["region"].map(region_map).fillna(-1).astype(int)
        merged["language_id"] = merged["language"].map(lang_map).fillna(-1).astype(int)
        merged["time_id"] = merged["startyear"].map(time_map).fillna(-1).astype(int)

        fact_rows = []
        bridge_rows = []

        for _, row in merged.iterrows():
            fact_rows.append({
                "fact_id": fact_id_counter,
                "tconst": row["tconst"],
                "region_id": row["region_id"],
                "language_id": row["language_id"],
                "time_id": row["time_id"],
                "is_original_title": row.get("isoriginaltitle"),
                "average_rating": row.get("averagerating"),
                "num_votes": row.get("numvotes")
            })
            
            # Handle genres
            genres_list = str(row["genres"]).split(",")
            for g in genres_list:
                gid = genre_map.get(g.strip().title(), -1)
                bridge_rows.append({"fact_id": fact_id_counter, "genre_id": gid})
                
            fact_id_counter += 1

        # Load chunks into target
        if fact_rows:
            pd.DataFrame(fact_rows).to_sql("fact_film_version", target_engine, if_exists="append", index=False, method="multi")
        if bridge_rows:
            pd.DataFrame(bridge_rows).to_sql("fact_genre_bridge", target_engine, if_exists="append", index=False, method="multi")

    # Process basics in chunks for fact table
    extract_table_in_chunks("title_basics", local_engine, process_fact_chunk, chunksize=50_000, limit=limit)

    # 6️⃣ Enforce foreign keys
    apply_foreign_keys(target_engine)

    # Clean up temporary files
    for temp_file in [akas_temp_file, basics_temp_file]:
        if os.path.exists(temp_file):
            os.remove(temp_file)

    print("ETL completed successfully!")

if __name__ == "__main__":
    run_etl(limit=None)