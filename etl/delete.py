import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
SUPABASE_DB_URL = os.getenv("DATABASE_URL")

def drop_all_tables(schema="public"):
    print(f"Connecting to Supabase schema: {schema}")
    engine = create_engine(SUPABASE_DB_URL)
    with engine.begin() as conn:
        print("Retrieving tables...")
        result = conn.execute(text(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :schema AND table_type = 'BASE TABLE';
        """), {"schema": schema})
        tables = [r[0] for r in result]

        if not tables:
            print("No tables found — nothing to delete.")
            return

        print(f"\nFound {len(tables)} tables:")
        for t in tables:
            print(f" - {t}")

        confirm = input("\nType 'DELETE' to confirm dropping all tables: ")
        if confirm.strip().upper() != "DELETE":
            print("Aborted.")
            return

        print("\nDropping all tables ...")
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE;"))
        conn.execute(text(f"CREATE SCHEMA {schema};"))
        print(f"All tables in schema '{schema}' deleted and schema recreated.")

if __name__ == "__main__":
    drop_all_tables()
