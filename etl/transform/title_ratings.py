import pandas as pd

def clean_title_ratings(df):
    df.columns = df.columns.str.lower()
    df["averagerating"] = pd.to_numeric(df["averagerating"], errors="coerce")
    df["numvotes"] = pd.to_numeric(df["numvotes"], errors="coerce").fillna(0).astype(int)
    df = df.drop_duplicates(subset=["tconst"])
    print(f"title_ratings cleaned — {len(df):,} rows remain.")
    return df
