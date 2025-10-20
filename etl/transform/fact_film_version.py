import pandas as pd

def build_fact_film_version(
    title_akas, title_basics, title_ratings,
    dim_region, dim_language, dim_time,
    chunk_size=1_000_000
):
    print("Building fact_film_version (chunked)...")

    region_map = dict(zip(dim_region["region_name"], dim_region["region_id"]))
    language_map = dict(zip(dim_language["language_name"], dim_language["language_id"]))
    time_map = dict(zip(dim_time["year"], dim_time["time_id"]))

    akas_subset = title_akas[["titleid", "region", "language", "isoriginaltitle"]].rename(
        columns={"titleid": "tconst"}
    )

    chunks = [title_basics[i:i+chunk_size] for i in range(0, len(title_basics), chunk_size)]
    fact_chunks = []

    for i, chunk in enumerate(chunks, start=1):
        merged = (
            chunk.merge(title_ratings, on="tconst", how="left")
                 .merge(akas_subset, on="tconst", how="left")
        )

        merged["region_id"] = merged["region"].map(region_map).fillna(-1).astype(int)
        merged["language_id"] = merged["language"].map(language_map).fillna(-1).astype(int)
        merged["time_id"] = merged["startyear"].map(time_map).fillna(-1).astype(int)

        fact_part = merged[[
            "tconst", "region_id", "language_id", "time_id",
            "isoriginaltitle", "averagerating", "numvotes"
        ]].rename(columns={
            "isoriginaltitle": "is_original_title",
            "averagerating": "average_rating",
            "numvotes": "num_votes"
        })

        fact_part["average_rating"] = fact_part["average_rating"].fillna(0)
        fact_part["num_votes"] = fact_part["num_votes"].fillna(0).astype(int)
        fact_chunks.append(fact_part)

        print(f"Processed chunk {i}/{len(chunks)} — {len(fact_part):,} rows")

    fact_film_version = pd.concat(fact_chunks, ignore_index=True)
    print(f"Built fact_film_version ({len(fact_film_version):,} rows)")
    return fact_film_version
