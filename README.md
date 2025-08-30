# E-Commerce Data Warehouse (Local Demo)

Build a local-first, end-to-end retail data platform:

- **ETL**: Python (Pandas) orchestrated by **Apache Airflow**
- **Warehouse**: PostgreSQL (as a Redshift stand-in)
- **Data**: sample `sales.csv` and `inventory.csv`
- **Features**: daily aggregated metrics, simple data-quality validation

## Architecture

CSV → **Extract → Transform → Load** → PostgreSQL  
(DAG: `etl_ecommerce_daily`) runs daily via Airflow.

## Quick Start

```bash
cd docker
docker compose up -d
