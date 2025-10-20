import pandas as pd

def clean_title_basics(df):
    df.columns = df.columns.str.lower()
    df = df.replace({"\\N": None})
    for col in ["isadult", "startyear", "endyear", "runtimeminutes"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["genres"] = df["genres"].fillna("Unknown")
    print(f"title_basics cleaned — {len(df):,} rows remain.")
    return df
