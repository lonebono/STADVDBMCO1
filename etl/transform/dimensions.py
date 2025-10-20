import pandas as pd

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

    return dim_region, dim_language, dim_genre, dim_time
