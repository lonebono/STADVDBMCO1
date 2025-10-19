import os
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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
#                     EXTRACTION
# =====================================================
def read_table(table_name, engine, limit=None):
    """Reads a PostgreSQL table using COPY for speed."""
    print(f"Extracting {table_name} from local PostgreSQL ...")
    with engine.connect() as conn:
        output = StringIO()
        sql = f"COPY (SELECT * FROM {table_name} {'LIMIT ' + str(limit) if limit else ''}) TO STDOUT WITH CSV HEADER"
        conn.connection.cursor().copy_expert(sql, output)
        output.seek(0)
        df = pd.read_csv(output)
        print(f"Loaded {len(df):,} rows from {table_name}")
        return df


# =====================================================
#                     CLEANING
# =====================================================
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


def clean_title_akas(df):
    df.columns = df.columns.str.lower()
    for col in ["region", "language"]:
        df[col] = df[col].replace({"\\N": None})
    df["isoriginaltitle"] = df["isoriginaltitle"].astype(str).str.lower().eq("true")
    df = df.drop_duplicates(subset=["titleid", "ordering"])

    # ✅ Option 2: Keep only original titles to reduce memory load
    df = df[df["isoriginaltitle"]]
    print(f"Filtered to original titles only: {len(df):,} rows remain.")
    return df


# =====================================================
#                     DIMENSIONS
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

    dim_time = (
        title_basics[["startyear"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"startyear": "year"})
    )
    dim_time["decade"] = (dim_time["year"] // 10) * 10
    dim_time["time_id"] = range(1, len(dim_time) + 1)

    print(
        f"Dimensions built: {len(dim_region)} regions, "
        f"{len(dim_language)} languages, {len(dim_genre)} genres, {len(dim_time)} years"
    )
    return dim_region, dim_language, dim_genre, dim_time


# =====================================================
#                  FACT + BRIDGE (Chunked)
# =====================================================
def build_fact_and_bridge(
    title_akas,
    title_basics,
    title_ratings,
    dim_region,
    dim_language,
    dim_genre,
    dim_time,
    chunk_size=1_000_000,
):
    print("Building fact and bridge tables (chunked)...")

    # Prepare lookup maps
    region_map = dict(zip(dim_region["region_name"], dim_region["region_id"]))
    language_map = dict(zip(dim_language["language_name"], dim_language["language_id"]))
    genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_id"]))
    time_map = dict(zip(dim_time["year"], dim_time["time_id"]))

    # Keep only essential akas columns (already filtered to originals)
    akas_subset = title_akas[["titleid", "region", "language", "isoriginaltitle"]].rename(
        columns={"titleid": "tconst"}
    )

    fact_chunks = []
    n = len(title_basics)
    for i in range(0, n, chunk_size):
        chunk = title_basics.iloc[i:i + chunk_size]
        merged = chunk.merge(title_ratings, on="tconst", how="left")
        merged = merged.merge(akas_subset, on="tconst", how="left")

        merged["region_id"] = merged["region"].map(region_map).fillna(-1).astype(int)
        merged["language_id"] = merged["language"].map(language_map).fillna(-1).astype(int)
        merged["time_id"] = merged["startyear"].map(time_map).fillna(-1).astype(int)

        fact_chunk = merged[
            ["tconst", "region_id", "language_id", "time_id", "isoriginaltitle", "averagerating", "numvotes"]
        ].rename(
            columns={
                "isoriginaltitle": "is_original_title",
                "averagerating": "average_rating",
                "numvotes": "num_votes",
            }
        )

        fact_chunk["average_rating"] = fact_chunk["average_rating"].fillna(0)
        fact_chunk["num_votes"] = fact_chunk["num_votes"].fillna(0).astype(int)

        fact_chunks.append(fact_chunk)
        print(f"Processed chunk {i // chunk_size + 1}/{(n // chunk_size) + 1}")

    fact_film_version = pd.concat(fact_chunks, ignore_index=True)

    # Bridge table (film ↔ genre)
    film_genres = title_basics[["tconst", "genres"]].dropna()
    film_genres["genres"] = film_genres["genres"].str.split(",")
    fact_genre_bridge = film_genres.explode("genres")
    fact_genre_bridge["genres"] = fact_genre_bridge["genres"].str.strip()
    fact_genre_bridge["genre_id"] = (
        fact_genre_bridge["genres"].map(genre_map).fillna(-1).astype(int)
    )

    print(
        f"Built fact_film_version ({len(fact_film_version):,} rows) "
        f"and fact_genre_bridge ({len(fact_genre_bridge):,} rows)"
    )
    return fact_film_version, fact_genre_bridge


# =====================================================
#                    UPLOAD
# =====================================================
def upload_to_supabase(df, table_name):
    with supabase_engine.begin() as conn:
        table_exists = conn.execute(
            text(f"SELECT to_regclass('{table_name}');")
        ).scalar()

        if table_exists:
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"))
            print(f"Truncated existing table: {table_name}")
        else:
            print(f"Table {table_name} does not exist — will be created.")

    df.to_sql(table_name, supabase_engine, if_exists="append", index=False)
    print(f"{table_name} uploaded successfully.\n")


# =====================================================
#                     MAIN
# =====================================================
def main():
    print("Starting IMDb ETL from local PostgreSQL to Supabase ...")

    title_akas = read_table("title_akas", local_engine)
    title_basics = read_table("title_basics", local_engine)
    title_ratings = read_table("title_ratings", local_engine)

    title_akas = clean_title_akas(title_akas)
    title_basics = clean_title_basics(title_basics)
    title_ratings = clean_title_ratings(title_ratings)

    dim_region, dim_language, dim_genre, dim_time = build_dimensions(title_akas, title_basics)

    fact_film_version, fact_genre_bridge = build_fact_and_bridge(
        title_akas,
        title_basics,
        title_ratings,
        dim_region,
        dim_language,
        dim_genre,
        dim_time,
    )

    upload_to_supabase(dim_region, "dim_region")
    upload_to_supabase(dim_language, "dim_language")
    upload_to_supabase(dim_genre, "dim_genre")
    upload_to_supabase(dim_time, "dim_time")
    upload_to_supabase(fact_film_version, "fact_film_version")
    upload_to_supabase(fact_genre_bridge, "fact_genre_bridge")

    print("IMDb dimensional model successfully uploaded to Supabase!")


if __name__ == "__main__":
    main()
