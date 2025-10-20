from config import local_engine, supabase_engine
from extract import read_table
from transform.title_akas import clean_title_akas
from transform.title_basics import clean_title_basics
from transform.title_ratings import clean_title_ratings
from transform.dimensions import build_dimensions
from load.upload_utils import upload_to_supabase
from load.fact_film_version import load_fact_film_version
from load.fact_genre_bridge import load_fact_genre_bridge
from load.constraints import add_primary_keys, add_foreign_keys

# =============== GLOBAL CONFIG ===============
ROW_LIMIT = 100 
# =============================================

def main():
    print(f"Starting IMDb ETL (ROW_LIMIT={ROW_LIMIT or 'ALL'})")

    # ===== Extract =====
    title_akas = clean_title_akas(read_table("title_akas", local_engine, limit=ROW_LIMIT))
    title_basics = clean_title_basics(read_table("title_basics", local_engine, limit=ROW_LIMIT))
    title_ratings = clean_title_ratings(read_table("title_ratings", local_engine, limit=ROW_LIMIT))

    # ===== Transform Dimensions =====
    dim_region, dim_language, dim_genre, dim_time = build_dimensions(title_akas, title_basics)

    # ===== Load Dimensions =====
    for name, df in {
        "dim_region": dim_region,
        "dim_language": dim_language,
        "dim_genre": dim_genre,
        "dim_time": dim_time,
    }.items():
        upload_to_supabase(df, name, supabase_engine)

    # ===== Load Facts =====
    load_fact_film_version(
        title_akas, title_basics, title_ratings,
        dim_region, dim_language, dim_time
    )
    load_fact_genre_bridge(title_basics, dim_genre)

    # ===== Add Constraints =====
    add_primary_keys(supabase_engine)
    add_foreign_keys(supabase_engine)

    print("IMDb ETL completed successfully!")

if __name__ == "__main__":
    main()
