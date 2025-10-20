import pandas as pd

def clean_title_akas(df):
    df.columns = df.columns.str.lower()
    for col in ["region", "language"]:
        df[col] = df[col].replace({"\\N": None})
    df["isoriginaltitle"] = df["isoriginaltitle"].astype(str).str.lower().isin(["true", "t", "1"])
    df = df.drop_duplicates(subset=["titleid", "ordering"])
    print(f"title_akas cleaned — {len(df):,} rows remain.")
    return df
