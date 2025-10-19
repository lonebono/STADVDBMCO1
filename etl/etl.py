import gzip
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO

PG_USER = "postgres"
PG_PASSWORD = "123"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "postgres"

FILES = {
    "title.akas.tsv.gz": "title_akas",
    "title.basics.tsv.gz": "title_basics",
    "title.ratings.tsv.gz": "title_ratings",
}

def extract_tsv_gzip(file_path: str) -> pd.DataFrame:
    """Extract TSV data from a gzip file into a pandas DataFrame."""
    print(f"📦 Extracting {file_path}...")
    with gzip.open(file_path, "rb") as f:
        data = f.read()
    df = pd.read_csv(BytesIO(data), sep="\t", na_values="\\N")
    print(f"Extracted {len(df)} rows from {file_path}")
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleanup: standardize column names and drop empty rows."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.dropna(how="all")
    return df


def load_to_postgres(df: pd.DataFrame, table_name: str):
    """Load a DataFrame into PostgreSQL."""
    print(f"⬆Loading into table '{table_name}'...")
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Loaded {len(df)} rows into {table_name}\n")


def main():
    for file_name, table_name in FILES.items():
        df = extract_tsv_gzip(file_name)
        df = transform_data(df)
        load_to_postgres(df, table_name)
    print("All files loaded successfully!")


if __name__ == "__main__":
    main()
