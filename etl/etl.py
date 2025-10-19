# This is an ETL script for to load the IMDB dataset into a Supabase PostgreSQL database.
# It extracts data from TSV.GV files, transforms it as needed, and loads it into the database

import pandas as pd
from sqlalchemy import create_engine, text

# ============================
# 1. Database connection setup
# ============================
DB_USER = "postgres"
DB_PASS = "123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "imdb_project"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# ============================
# 2. Extract - Load raw IMDb data to staging
# ============================
print("Extracting IMDb TSV files...")

title_basics = pd.read_csv("data/title.basics.tsv", sep="\t", na_values="\\N", low_memory=False)
title_ratings = pd.read_csv("data/title.ratings.tsv", sep="\t", na_values="\\N", low_memory=False)
name_basics = pd.read_csv("data/name.basics.tsv", sep="\t", na_values="\\N", low_memory=False)

print("Loading to staging schema...")
title_basics.to_sql("title_basics", engine, schema="staging", if_exists="replace", index=False)
title_ratings.to_sql("title_ratings", engine, schema="staging", if_exists="replace", index=False)
name_basics.to_sql("name_basics", engine, schema="staging", if_exists="replace", index=False)

# ============================
# 3. Transform
# ============================
print("Transforming data...")

# Clean startYear → integer, filter only movies
movies = title_basics[title_basics["titleType"] == "movie"].copy()
movies["startYear"] = pd.to_numeric(movies["startYear"], errors="coerce")
movies["runtimeMinutes"] = pd.to_numeric(movies["runtimeMinutes"], errors="coerce")

# Merge ratings with movies
merged = movies.merge(title_ratings, on="tconst", how="left")

# Expand genre column (split by comma)
merged["genres"] = merged["genres"].fillna("Unknown")
merged = merged.assign(genre=merged["genres"].str.split(",")).explode("genre")

# Drop duplicates and irrelevant columns
merged = merged.drop_duplicates(subset=["tconst", "genre"])
merged = merged[["tconst", "primaryTitle", "originalTitle", "startYear", "runtimeMinutes", "genre", "averageRating", "numVotes"]]

# ============================
# 4. Load - Write to warehouse schema
# ============================
print("Loading to data warehouse...")

# Load dimension tables
dim_genre = pd.DataFrame({"genre_name": merged["genre"].unique()})
dim_genre.to_sql("dim_genre", engine, schema="warehouse", if_exists="replace", index=False)

dim_time = (
    merged[["startYear"]]
    .dropna()
    .drop_duplicates()
    .assign(decade=(merged["startYear"] // 10) * 10)
)
dim_time.to_sql("dim_time", engine, schema="warehouse", if_exists="replace", index=False)

# Load fact table
merged.to_sql("fact_movie_ratings", engine, schema="warehouse", if_exists="replace", index=False)

# ============================
# 5. Post-load SQL operations (optional)
# ============================
with engine.begin() as conn:
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_fact_genre ON warehouse.fact_movie_ratings(genre);
        CREATE INDEX IF NOT EXISTS idx_fact_year ON warehouse.fact_movie_ratings(startYear);
    """))

print("✅ ETL complete!")
