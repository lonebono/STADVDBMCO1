from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

def upload_to_supabase(df, table_name, engine, chunk_size=500_000):
    with engine.begin() as conn:
        table_exists = conn.execute(text(f"SELECT to_regclass('{table_name}');")).scalar()
        if table_exists:
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"))
            print(f"Truncated {table_name}")
        else:
            print(f"Table {table_name} does not exist — will be created.")

    for start in range(0, len(df), chunk_size):
        end = start + chunk_size
        chunk = df.iloc[start:end]
        try:
            chunk.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"Uploaded {start:,}–{end:,} rows to {table_name}")
        except SQLAlchemyError as e:
            print(f"Error uploading {table_name} ({start:,}–{end:,}): {e}")
