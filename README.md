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

## Verify

SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM fact_inventory;
SELECT * FROM agg_sales_daily ORDER BY sale_date DESC, store_id LIMIT 10;

## Troubleshooting
 Airflow UI won't load:
docker compose run --rm airflow airflow db init
# then restart
docker compose up -d

## 	Port conflicts (e.g., 8080 or 5433) — change in docker/docker-compose.yml:

ports:
  - "8081:8080"
  - "5434:5432"

Then: docker compose down -v && docker compose up -d


