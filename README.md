# E-Commerce Data Warehouse Solution

End-to-end, local-first demo of a retail data platform:
- **ETL** with Python (Pandas / optional PySpark)
- **Orchestration** with Apache Airflow
- **Warehouse** on PostgreSQL (Redshift substitute)
- **Containers** via Docker (optional K8s with kind/minikube)

> Mirrors a resume project: Lambda/Glue → (here) Python/PySpark; Redshift → PostgreSQL; Airflow/Kubernetes for orchestration/deploy.

## Architecture
```
CSV/API → Extract → Transform (Pandas/PySpark) → Load (PostgreSQL)
                          ↑
                     Orchestrate (Airflow)
```
Optional: deploy ETL container to Kubernetes.

## Quick Start
1) Install **Docker Desktop**.
2) In a terminal, run:
```bash
cd docker
docker compose up -d
```
3) Open Airflow: http://localhost:8080 (user: airflow / pass: airflow)
4) Turn on `etl_ecommerce_daily` DAG.
5) Connect to Postgres (localhost:5433, db: warehouse, user: warehouse, pass: warehouse) and run queries from `warehouse/schema.sql` and `warehouse/indexes.sql` (Airflow init will create tables automatically on first run).

## Repo Layout
```
ecommerce-data-warehouse/
├─ data/                     # sample CSVs (sales, inventory)
├─ etl/                      # extract/transform/load scripts (reusable)
├─ airflow/
│  └─ dags/etl_dag.py        # Airflow DAG calling ETL
├─ docker/
│  ├─ docker-compose.yml     # Airflow + Postgres
│  └─ Dockerfile.etl         # (Optional) ETL container image
├─ warehouse/
│  ├─ schema.sql             # star-ish schema
│  └─ indexes.sql            # performance helpers
├─ k8s/deployment.yaml       # optional K8s deploy for ETL image
└─ README.md
```

## Next Steps (nice-to-have)
- Swap Pandas for **PySpark** to mimic Glue.
- Add **dbt** models on top of Postgres.
- Publish a short demo video & link it here.
