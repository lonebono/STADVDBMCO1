import pandas as pd

def build_fact_genre_bridge(title_basics, dim_genre):
    print("Building fact_genre_bridge ...")

    genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_id"]))
    genres = title_basics[["tconst", "genres"]].dropna()

    genres["genres"] = genres["genres"].str.split(",")
    bridge = genres.explode("genres")
    bridge["genres"] = bridge["genres"].str.strip()
    bridge["genre_id"] = bridge["genres"].map(genre_map).fillna(-1).astype(int)

    bridge = bridge[["tconst", "genre_id"]].drop_duplicates()
    print(f"Built fact_genre_bridge ({len(bridge):,} rows)")
    return bridge
