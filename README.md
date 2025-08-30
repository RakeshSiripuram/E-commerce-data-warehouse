# E-Commerce Data Warehouse (Local Demo)

A local-first, end-to-end retail data platform project.

- **ETL**: Python (Pandas) orchestrated by **Apache Airflow**
- **Warehouse**: PostgreSQL (as a Redshift stand-in)
- **Data**: sample `sales.csv` and `inventory.csv`
- **Features**: daily aggregate metrics + basic data-quality validation

---

## Architecture

CSV → **Extract → Transform → Load** → PostgreSQL  
DAG: **`etl_ecommerce_daily`** runs daily in Airflow.

---

## Quick Start

```bash
cd docker
docker compose up -d
