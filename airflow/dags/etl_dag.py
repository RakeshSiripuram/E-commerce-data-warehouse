# airflow/dags/etl_dag.py
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Our local modules (mounted into the container)
from etl.extract import extract_sales, extract_inventory
from etl.transform import (
    clean_sales,
    clean_inventory,
    aggregate_sales_daily,
    validate_sales,
)

import psycopg2
from psycopg2.extras import execute_values


# ---------- helpers (module-level so they're always in scope) ----------
def get_conn():
    """Get a Postgres connection using environment variables."""
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "warehouse"),
        user=os.getenv("PGUSER", "warehouse"),
        password=os.getenv("PGPASSWORD", "warehouse"),
        host=os.getenv("PGHOST", "postgres"),
        port=int(os.getenv("PGPORT", "5432")),
    )


def bulk_insert(cur, df, table):
    """
    Simple bulk loader: truncates the table (demo-style) then inserts rows.
    Pass an open cursor (cur). Safe if df is empty.
    """
    # Clear table (no placeholders here -> use execute, not execute_values)
    cur.execute(f"TRUNCATE TABLE {table};")

    if df is None or df.empty:
        return

    cols = ",".join(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    template = "(" + ",".join(["%s"] * len(df.columns)) + ")"
    execute_values(
        cur,
        f"INSERT INTO {table} ({cols}) VALUES %s",
        values,
        template=template,
    )


# ---------- DAG ----------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_ecommerce_daily",
    default_args=default_args,
    description="Daily ETL for e-commerce data into Postgres",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    def run_etl(**_):
        # file paths (mounted via docker-compose volumes)
        sales_path = os.getenv("SALES_PATH", "/opt/airflow/data/sales.csv")
        inv_path = os.getenv("INVENTORY_PATH", "/opt/airflow/data/inventory.csv")

        # 1) EXTRACT
        sales_raw = extract_sales(sales_path)
        inv_raw = extract_inventory(inv_path)

        # 2) TRANSFORM (+ DQ)
        sales = clean_sales(sales_raw)
        inv = clean_inventory(inv_raw)

        # Data-quality checks (raise ValueError on bad data)
        validate_sales(sales)

        # 3) LOAD
        conn = get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    # Ensure schema exists (id columns optional; keep simple types)
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS dim_store (
                            store_id TEXT PRIMARY KEY
                        );

                        CREATE TABLE IF NOT EXISTS dim_item (
                            sku TEXT PRIMARY KEY
                        );

                        CREATE TABLE IF NOT EXISTS fact_sales (
                            sale_date DATE,
                            store_id  TEXT REFERENCES dim_store(store_id),
                            sku       TEXT REFERENCES dim_item(sku),
                            quantity  INT,
                            unit_price NUMERIC(10,2),
                            total_amount NUMERIC(12,2)
                        );

                        CREATE TABLE IF NOT EXISTS fact_inventory (
                            store_id TEXT REFERENCES dim_store(store_id),
                            sku      TEXT REFERENCES dim_item(sku),
                            on_hand INT,
                            reorder_point INT
                        );

                        CREATE TABLE IF NOT EXISTS agg_sales_daily (
                            sale_date DATE,
                            store_id TEXT,
                            total_qty INT,
                            total_revenue NUMERIC(12,2),
                            avg_unit_price NUMERIC(10,4),
                            distinct_items INT
                        );

                        -- Helpful indexes (idempotent)
                        CREATE INDEX IF NOT EXISTS ix_sales_date  ON fact_sales (sale_date);
                        CREATE INDEX IF NOT EXISTS ix_sales_store ON fact_sales (store_id);
                        CREATE INDEX IF NOT EXISTS ix_sales_sku   ON fact_sales (sku);
                        CREATE INDEX IF NOT EXISTS ix_inv_store   ON fact_inventory (store_id);
                        CREATE INDEX IF NOT EXISTS ix_inv_sku     ON fact_inventory (sku);
                        """
                    )

                    # Upsert dims (insert-ignore behavior)
                    # unique stores/items from sales; include from inventory as well
                    import pandas as pd

                    store_ids = pd.Series(sales["store_id"].unique())
                    inv_store_ids = pd.Series(inv["store_id"].unique())
                    all_stores = pd.Series(pd.concat([store_ids, inv_store_ids]).unique())

                    item_skus = pd.Series(sales["sku"].unique())
                    inv_skus = pd.Series(inv["sku"].unique())
                    all_skus = pd.Series(pd.concat([item_skus, inv_skus]).unique())

                    for s in all_stores.dropna():
                        cur.execute(
                            "INSERT INTO dim_store (store_id) VALUES (%s) ON CONFLICT DO NOTHING;",
                            (str(s),),
                        )
                    for sku in all_skus.dropna():
                        cur.execute(
                            "INSERT INTO dim_item (sku) VALUES (%s) ON CONFLICT DO NOTHING;",
                            (str(sku),),
                        )

                    # Facts
                    bulk_insert(
                        cur,
                        sales[
                            [
                                "sale_date",
                                "store_id",
                                "sku",
                                "quantity",
                                "unit_price",
                                "total_amount",
                            ]
                        ],
                        "fact_sales",
                    )

                    bulk_insert(
                        cur,
                        inv[["store_id", "sku", "on_hand", "reorder_point"]],
                        "fact_inventory",
                    )

                    # Aggregates
                    agg = aggregate_sales_daily(sales)
                    bulk_insert(
                        cur,
                        agg[
                            [
                                "sale_date",
                                "store_id",
                                "total_qty",
                                "total_revenue",
                                "avg_unit_price",
                                "distinct_items",
                            ]
                        ],
                        "agg_sales_daily",
                    )

        finally:
            conn.close()

    etl = PythonOperator(task_id="run_etl", python_callable=run_etl)

    etl