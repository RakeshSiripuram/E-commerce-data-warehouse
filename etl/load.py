import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "warehouse"),
        user=os.getenv("PGUSER", "warehouse"),
        password=os.getenv("PGPASSWORD", "warehouse"),
        host=os.getenv("PGHOST", "postgres"),
        port=int(os.getenv("PGPORT", "5432"))
    )

def load_dataframe(df: pd.DataFrame, table: str):
    cols = ",".join(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    template = "(" + ",".join(["%s"] * len(df.columns)) + ")"
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, f"INSERT INTO {table} ({cols}) VALUES %s", values, template=template)

if __name__ == "__main__":
    # This module is intended to be called from Airflow DAG
    pass
