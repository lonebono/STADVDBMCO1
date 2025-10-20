import pandas as pd
from io import StringIO

def read_table(table_name, engine, limit=None):
    """Reads a table from PostgreSQL into a DataFrame."""
    print(f"Extracting {table_name} (limit={limit or 'ALL'}) ...")
    with engine.connect() as conn:
        output = StringIO()
        sql = f"COPY (SELECT * FROM {table_name} {'LIMIT ' + str(limit) if limit else ''}) TO STDOUT WITH CSV HEADER"
        conn.connection.cursor().copy_expert(sql, output)
        output.seek(0)
        df = pd.read_csv(output, low_memory=False)
    print(f"Loaded {len(df):,} rows from {table_name}")
    return df
