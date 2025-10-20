import os
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ============ LOAD ENVIRONMENT VARIABLES ============
load_dotenv()
LOCAL_DB = os.getenv("LOCAL_DB")
LOCAL_USER = os.getenv("LOCAL_USER")
LOCAL_PASSWORD = os.getenv("LOCAL_PASSWORD")
LOCAL_HOST = os.getenv("LOCAL_HOST")
LOCAL_PORT = os.getenv("LOCAL_PORT")
DATABASE_URL = os.getenv("DATABASE_URL")

local_engine = create_engine(
    f"postgresql+psycopg2://{LOCAL_USER}:{LOCAL_PASSWORD}@{LOCAL_HOST}:{LOCAL_PORT}/{LOCAL_DB}"
)
supabase_engine = create_engine(DATABASE_URL)

# =====================================================
# EXTRACT
# =====================================================
def read_table(table_name, engine, limit=100):
    print(f"Extracting {table_name} from local PostgreSQL ...")
    with engine.connect() as conn:
        output = StringIO()
        sql = f"COPY (SELECT * FROM {table_name} {'LIMIT ' + str(limit) if limit else ''}) TO STDOUT WITH CSV HEADER"
        conn.connection.cursor().copy_expert(sql, output)
        output.seek(0)
        df = pd.read_csv(output, low_memory=False)
    print(f"Loaded {len(df):,} rows from {table_name}")
    return df

# =====================================================
# CLEAN
# =====================================================
def clean_title_akas(df):
    df.columns = df.columns.str.lower()
    for col in ["region", "language"]:
        df[col] = df[col].replace({"\\N": None})
    df["isoriginaltitle"] = df["isoriginaltitle"].astype(str).str.lower().isin(["true", "t", "1"])
    df = df.drop_duplicates(subset=["titleid", "ordering"])
    print(f"Filtered to original titles only: {df['isoriginaltitle'].sum():,} rows remain.")
    return df

def clean_title_basics(df):
    df.columns = df.columns.str.lower()
    df = df.replace({"\\N": None})
    for col in ["isadult", "startyear", "endyear", "runtimeminutes"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["genres"] = df["genres"].fillna("Unknown")
    return df

def clean_title_ratings(df):
    df.columns = df.columns.str.lower()
    df["averagerating"] = pd.to_numeric(df["averagerating"], errors="coerce")
    df["numvotes"] = pd.to_numeric(df["numvotes"], errors="coerce").fillna(0).astype(int)
    return df.drop_duplicates(subset=["tconst"])

# =====================================================
# DIMENSIONS
# =====================================================
def build_dimensions(title_akas, title_basics):
    print("Building dimension tables ...")

    dim_region = pd.DataFrame(title_akas["region"].dropna().unique(), columns=["region_name"])
    dim_region["region_id"] = range(1, len(dim_region) + 1)

    dim_language = pd.DataFrame(title_akas["language"].dropna().unique(), columns=["language_name"])
    dim_language["language_id"] = range(1, len(dim_language) + 1)

    genres = title_basics["genres"].dropna().str.split(",").explode().str.strip().unique()
    dim_genre = pd.DataFrame(genres, columns=["genre_name"])
    dim_genre["genre_id"] = range(1, len(dim_genre) + 1)

    dim_time = title_basics[["startyear"]].dropna().drop_duplicates().rename(columns={"startyear": "year"})
    dim_time["decade"] = (dim_time["year"] // 10) * 10
    dim_time["time_id"] = range(1, len(dim_time) + 1)

    print(f"Dimensions built: {len(dim_region)} regions, {len(dim_language)} languages, {len(dim_genre)} genres, {len(dim_time)} years")
    return dim_region, dim_language, dim_genre, dim_time

# =====================================================
# FACT + BRIDGE (CHUNKED)
# =====================================================
def build_fact_and_bridge(title_akas, title_basics, title_ratings, dim_region, dim_language, dim_genre, dim_time, chunk_size=1_000_000):
    print("Building fact and bridge tables (chunked)...")

    region_map = dict(zip(dim_region["region_name"], dim_region["region_id"]))
    language_map = dict(zip(dim_language["language_name"], dim_language["language_id"]))
    genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_id"]))
    time_map = dict(zip(dim_time["year"], dim_time["time_id"]))

    akas_subset = title_akas[["titleid", "region", "language", "isoriginaltitle"]].rename(columns={"titleid": "tconst"})
    chunks = [title_basics[i:i+chunk_size] for i in range(0, len(title_basics), chunk_size)]
    fact_chunks, bridge_chunks = [], []

    for i, chunk in enumerate(chunks, start=1):
        merged = chunk.merge(title_ratings, on="tconst", how="left")
        merged = merged.merge(akas_subset, on="tconst", how="left")

        merged["region_id"] = merged["region"].map(region_map).fillna(-1).astype(int)
        merged["language_id"] = merged["language"].map(language_map).fillna(-1).astype(int)
        merged["time_id"] = merged["startyear"].map(time_map).fillna(-1).astype(int)

        fact_part = merged[[
            "tconst", "region_id", "language_id", "time_id", "isoriginaltitle", "averagerating", "numvotes"
        ]].rename(columns={
            "isoriginaltitle": "is_original_title",
            "averagerating": "average_rating",
            "numvotes": "num_votes"
        })

        fact_part["average_rating"] = fact_part["average_rating"].fillna(0)
        fact_part["num_votes"] = fact_part["num_votes"].fillna(0).astype(int)

        genres = chunk[["tconst", "genres"]].dropna()
        genres["genres"] = genres["genres"].str.split(",")
        bridge_part = genres.explode("genres")
        bridge_part["genres"] = bridge_part["genres"].str.strip()
        bridge_part["genre_id"] = bridge_part["genres"].map(genre_map).fillna(-1).astype(int)

        fact_chunks.append(fact_part)
        bridge_chunks.append(bridge_part)

        print(f"Processed chunk {i}/{len(chunks)}")

    fact_film_version = pd.concat(fact_chunks, ignore_index=True)
    fact_genre_bridge = pd.concat(bridge_chunks, ignore_index=True)

    print(f"Built fact_film_version ({len(fact_film_version):,} rows) and fact_genre_bridge ({len(fact_genre_bridge):,} rows)")
    return fact_film_version, fact_genre_bridge

# =====================================================
# LOAD (CHUNKED UPLOAD + CASCADING KEYS)
# =====================================================
def upload_to_supabase(df, table_name, chunk_size=500_000):
    with supabase_engine.begin() as conn:
        table_exists = conn.execute(text(f"SELECT to_regclass('{table_name}');")).scalar()
        if table_exists:
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"))
            print(f"Truncated existing table: {table_name}")
        else:
            print(f"Table {table_name} does not exist — will be created.")

    # Upload in smaller chunks
    for start in range(0, len(df), chunk_size):
        end = start + chunk_size
        chunk = df.iloc[start:end]
        try:
            chunk.to_sql(table_name, supabase_engine, if_exists="append", index=False)
            print(f"Uploaded rows {start:,}–{end:,} to {table_name}")
        except SQLAlchemyError as e:
            print(f"Error uploading chunk {start:,}–{end:,} of {table_name}: {e}")
            with supabase_engine.connect() as conn:
                conn.rollback()
                
def ensure_unknown_rows(dim_dfs):
    """Ensure all dimension tables have a '-1' Unknown entry."""
    for name, df, id_col, name_col in dim_dfs:
        if -1 not in df[id_col].values:
            unknown_row = pd.DataFrame([{id_col: -1, name_col: "Unknown"}])
            df = pd.concat([unknown_row, df], ignore_index=True)
            print(f"Added Unknown row to {name}")
        yield name, df
        
def add_primary_keys():
    print("Adding primary keys ...")
    with supabase_engine.begin() as conn:
        for table, pk in [
            ("dim_region", "region_id"),
            ("dim_language", "language_id"),
            ("dim_genre", "genre_id"),
            ("dim_time", "time_id")
        ]:
            # Check if PK already exists
            exists_query = text(f"""
                SELECT COUNT(*) 
                FROM information_schema.table_constraints 
                WHERE table_name = '{table}' 
                AND constraint_type = 'PRIMARY KEY';
            """)
            exists = conn.execute(exists_query).scalar()

            if exists == 0:
                try:
                    conn.execute(text(f'ALTER TABLE {table} ADD PRIMARY KEY ({pk});'))
                    print(f"Primary key added to {table} ({pk})")
                except Exception as e:
                    print(f"Could not add PK to {table}: {e}")
            else:
                print(f"Primary key already exists for {table}")


def add_foreign_keys():
    print("Adding cascading foreign keys ...")
    with supabase_engine.begin() as conn:
        foreign_keys = [
            ("fact_film_version", "region_id", "dim_region", "region_id", "fk_region"),
            ("fact_film_version", "language_id", "dim_language", "language_id", "fk_language"),
            ("fact_film_version", "time_id", "dim_time", "time_id", "fk_time"),
            ("fact_genre_bridge", "genre_id", "dim_genre", "genre_id", "fk_genre")
        ]

        for table, col, ref_table, ref_col, constraint in foreign_keys:
            try:
                # Check existence of both tables
                check = conn.execute(text(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name IN ('{table}', '{ref_table}');
                """)).scalar()
                if check < 2:
                    print(f"Skipping FK {constraint}: one or both tables do not exist.")
                    continue

                conn.execute(text(f"""
                    ALTER TABLE {table}
                    ADD CONSTRAINT {constraint}
                    FOREIGN KEY ({col}) REFERENCES {ref_table}({ref_col})
                    ON DELETE CASCADE ON UPDATE CASCADE;
                """))
                print(f"Added cascading FK {constraint} ({table}.{col} → {ref_table}.{ref_col})")
            except Exception as e:
                print(f"Skipped FK (likely exists): {e}")


# =====================================================
# MAIN
# =====================================================
def main():
    print("Starting IMDb ETL from local PostgreSQL to Supabase ...")

    title_akas = clean_title_akas(read_table("title_akas", local_engine))
    title_basics = clean_title_basics(read_table("title_basics", local_engine))
    title_ratings = clean_title_ratings(read_table("title_ratings", local_engine))

    dim_region, dim_language, dim_genre, dim_time = build_dimensions(title_akas, title_basics)
    fact_film_version, fact_genre_bridge = build_fact_and_bridge(
        title_akas, title_basics, title_ratings, dim_region, dim_language, dim_genre, dim_time
    )

    for name, df in ensure_unknown_rows([
    ("dim_region", dim_region, "region_id", "region_name"),
    ("dim_language", dim_language, "language_id", "language_name"),
    ("dim_genre", dim_genre, "genre_id", "genre_name"),
    ("dim_time", dim_time, "time_id", "year")
    ]):
        upload_to_supabase(df, name)

    add_primary_keys()
    add_foreign_keys()
    print("IMDb dimensional model successfully uploaded to Supabase with cascading keys!")

if __name__ == "__main__":
    main()
