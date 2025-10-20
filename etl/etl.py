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
        conn.execute(text("INSERT INTO dim_region (region_id, region_name) VALUES (-1, 'Unknown') ON CONFLICT (region_id) DO NOTHING;"))
        conn.execute(text("INSERT INTO dim_language (language_id, language_name) VALUES (-1, 'Unknown') ON CONFLICT (language_id) DO NOTHING;"))
        conn.execute(text("INSERT INTO dim_genre (genre_id, genre_name) VALUES (-1, 'Unknown') ON CONFLICT (genre_id) DO NOTHING;"))
        conn.execute(text("INSERT INTO dim_time (time_id, year, decade) VALUES (-1, NULL, NULL) ON CONFLICT (time_id) DO NOTHING;"))
    print("Tables created and 'Unknown' rows inserted.")

# ================= Foreign Keys =================
def apply_foreign_keys(engine):
    with engine.begin() as conn:
        # Fact table FKs
        conn.execute(text("""
            ALTER TABLE fact_film_version 
            DROP CONSTRAINT IF EXISTS fk_region;
            ALTER TABLE fact_film_version 
            ADD CONSTRAINT fk_region FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
            ON UPDATE CASCADE ON DELETE RESTRICT;
        """))
        conn.execute(text("""
            ALTER TABLE fact_film_version 
            DROP CONSTRAINT IF EXISTS fk_language;
            ALTER TABLE fact_film_version 
            ADD CONSTRAINT fk_language FOREIGN KEY (language_id) REFERENCES dim_language(language_id)
            ON UPDATE CASCADE ON DELETE RESTRICT;
        """))
        conn.execute(text("""
            ALTER TABLE fact_film_version 
            DROP CONSTRAINT IF EXISTS fk_time;
            ALTER TABLE fact_film_version 
            ADD CONSTRAINT fk_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
            ON UPDATE CASCADE ON DELETE RESTRICT;
        """))

        # Bridge table FKs
        conn.execute(text("""
            ALTER TABLE fact_genre_bridge 
            DROP CONSTRAINT IF EXISTS fact_genre_bridge_fact_id_fkey;
            ALTER TABLE fact_genre_bridge 
            ADD CONSTRAINT fact_genre_bridge_fact_id_fkey FOREIGN KEY (fact_id) REFERENCES fact_film_version(fact_id)
            ON DELETE CASCADE;
        """))
        conn.execute(text("""
            ALTER TABLE fact_genre_bridge 
            DROP CONSTRAINT IF EXISTS fact_genre_bridge_genre_id_fkey;
            ALTER TABLE fact_genre_bridge 
            ADD CONSTRAINT fact_genre_bridge_genre_id_fkey FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
            ON DELETE RESTRICT;
        """))
    print("Foreign key constraints applied.")

# ================= Cleaning Functions =================
def clean_akas(df):
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

def run_etl():
    create_tables_and_insert_unknowns(target_engine)

    akas_file = "akas_temp.csv"
    basics_file = "basics_temp.csv"

    print("Loading AKAs...")
    akas = pd.read_csv(akas_file)
    print("Loading Basics...")
    basics = pd.read_csv(basics_file)

    print("Extracting ratings from DB...")
    ratings = pd.read_sql("SELECT * FROM title_ratings", local_engine)
    ratings["averagerating"] = pd.to_numeric(ratings["averagerating"], errors="coerce").fillna(0)
    ratings["numvotes"] = pd.to_numeric(ratings["numvotes"], errors="coerce").fillna(0).astype(int)
    ratings.rename(columns={"tconst": "tconst"}, inplace=True)

    dim_region = pd.DataFrame({"region_name": akas["region"].dropna().unique()})
    dim_region["region_id"] = range(1, len(dim_region)+1)

    dim_language = pd.DataFrame({"language_name": akas["language"].dropna().unique()})
    dim_language["language_id"] = range(1, len(dim_language)+1)

    genres_list = list(set(g.strip().title() 
                           for sublist in basics["genres"].str.split(",") 
                           for g in sublist if g.strip().title() != "Unknown"))
    dim_genre = pd.DataFrame({"genre_name": genres_list})
    dim_genre["genre_id"] = range(1, len(dim_genre)+1)

    dim_time = pd.DataFrame({"year": basics["startyear"].dropna().unique()})
    dim_time["decade"] = (dim_time["year"] // 10) * 10
    dim_time["time_id"] = range(1, len(dim_time) + 1)

    print("Loading dimensions...")
    dim_region.to_sql("dim_region", target_engine, if_exists="append", index=False, method="multi")
    dim_language.to_sql("dim_language", target_engine, if_exists="append", index=False, method="multi")
    dim_genre.to_sql("dim_genre", target_engine, if_exists="append", index=False, method="multi")
    dim_time.to_sql("dim_time", target_engine, if_exists="append", index=False, method="multi")

    region_map = dict(zip(dim_region["region_name"], dim_region["region_id"]))
    lang_map = dict(zip(dim_language["language_name"], dim_language["language_id"]))
    genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_id"]))
    time_map = dict(zip(dim_time["year"], dim_time["time_id"]))

    print("Building fact and bridge tables...")
    fact_rows = []
    bridge_rows = []
    fact_id_counter = 1

    first_akas = akas.groupby("titleid").first().reset_index()

    merged = basics.merge(ratings, on="tconst", how="left")
    merged = merged.merge(first_akas.rename(columns={"titleid": "tconst"}), on="tconst", how="left")

    for _, row in merged.iterrows():
        fact_rows.append({
            "fact_id": fact_id_counter,
            "tconst": row["tconst"],
            "region_id": region_map.get(row["region"], -1),
            "language_id": lang_map.get(row["language"], -1),
            "time_id": time_map.get(row["startyear"], -1),
            "is_original_title": row.get("isoriginaltitle"),
            "average_rating": row.get("averagerating"),
            "num_votes": row.get("numvotes")
        })
        for g in str(row["genres"]).split(","):
            gid = genre_map.get(g.strip().title(), -1)
            bridge_rows.append({"fact_id": fact_id_counter, "genre_id": gid})
        fact_id_counter += 1

        if len(fact_rows) >= 10000:
            pd.DataFrame(fact_rows).to_sql("fact_film_version", target_engine, if_exists="append", index=False, method="multi")
            pd.DataFrame(bridge_rows).to_sql("fact_genre_bridge", target_engine, if_exists="append", index=False, method="multi")
            fact_rows.clear()
            bridge_rows.clear()

    if fact_rows:
        pd.DataFrame(fact_rows).to_sql("fact_film_version", target_engine, if_exists="append", index=False, method="multi")
        pd.DataFrame(bridge_rows).to_sql("fact_genre_bridge", target_engine, if_exists="append", index=False, method="multi")

    apply_foreign_keys(target_engine)

    print("ETL completed successfully!")


if __name__ == "__main__":
    run_etl()
