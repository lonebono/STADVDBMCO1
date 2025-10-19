import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

# Load environment variables from .env file
load_dotenv()

# === CONNECTIONS ===
LOCAL_DB = os.getenv("LOCAL_DB")
LOCAL_USER = os.getenv("LOCAL_USER")
LOCAL_PASSWORD = os.getenv("LOCAL_PASSWORD")
LOCAL_HOST = os.getenv("LOCAL_HOST")
LOCAL_PORT = os.getenv("LOCAL_PORT")
DATABASE_URL = os.getenv("DATABASE_URL")

# Tables to transfer
TABLES = ["title_ratings"]

def transfer_table(table_name, source_engine, dest_engine, limit=100):
    print(f"Reading first {limit} rows from '{table_name}' in local PostgreSQL...")

    # Use SQLAlchemy text query to only get first `limit` rows
    query = text(f"SELECT * FROM {table_name} LIMIT {limit};")
    df = pd.read_sql_query(query, source_engine)

    print(f"Retrieved {len(df)} rows from {table_name}")

    print(f"Uploading '{table_name}' to Supabase...")
    df.to_sql(table_name, dest_engine, if_exists="replace", index=False)
    print(f"✅ Successfully transferred '{table_name}' to Supabase!\n")

def main():
    print("Connecting to databases...")
    source_engine = create_engine(
        f"postgresql+psycopg2://{LOCAL_USER}:{LOCAL_PASSWORD}@{LOCAL_HOST}:{LOCAL_PORT}/{LOCAL_DB}"
    )
    dest_engine = create_engine(DATABASE_URL)

    for table in TABLES:
        try:
            transfer_table(table, source_engine, dest_engine, limit=100)
        except Exception as e:
            print(f"Error transferring {table}: {e}")

    print("All tables processed.")

if __name__ == "__main__":
    main()
