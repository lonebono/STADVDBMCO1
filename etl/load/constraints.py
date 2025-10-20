from sqlalchemy import text

def add_primary_keys(engine):
    pk_tables = {
        "dim_region": "region_id",
        "dim_language": "language_id",
        "dim_genre": "genre_id",
        "dim_time": "time_id"
    }
    with engine.begin() as conn:
        for table, pk in pk_tables.items():
            conn.execute(text(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = '{table}'
                        AND constraint_type = 'PRIMARY KEY'
                    ) THEN
                        ALTER TABLE {table} ADD PRIMARY KEY ({pk});
                        RAISE NOTICE 'Added PK on {table}';
                    END IF;
                END;
                $$;
            """))

def add_foreign_keys(engine):
    fks = [
        ("fact_film_version", "region_id", "dim_region", "region_id", "fk_region"),
        ("fact_film_version", "language_id", "dim_language", "language_id", "fk_language"),
        ("fact_film_version", "time_id", "dim_time", "time_id", "fk_time"),
        ("fact_genre_bridge", "genre_id", "dim_genre", "genre_id", "fk_genre")
    ]
    with engine.begin() as conn:
        for table, col, ref_table, ref_col, fk in fks:
            try:
                conn.execute(text(f"""
                    ALTER TABLE {table}
                    ADD CONSTRAINT {fk}
                    FOREIGN KEY ({col}) REFERENCES {ref_table}({ref_col})
                    ON DELETE CASCADE ON UPDATE CASCADE;
                """))
                print(f"Added FK {fk}: {table}.{col} → {ref_table}.{ref_col}")
            except Exception as e:
                print(f"Skipped {fk} (likely exists): {e}")
