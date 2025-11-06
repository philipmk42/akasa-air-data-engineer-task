# Akasa Air — Data Engineer Task (Starter Solution)

This repo provides a clean, runnable solution for the interview task using **two approaches**:

1. **Table-based**: MySQL + SQLAlchemy (ORM) with efficient SQL for KPIs.
2. **In-memory**: Pandas for fast local analysis.

It ingests **CSV (customers)** and **XML (orders)**, handles timezone-aware timestamps, and computes:
- Repeat customers
- Monthly order trends
- Regional revenue
- Top customers by spend (last 30 days)

## Quick Start

### 0) Prereqs
- Python 3.10+
- Docker + docker-compose

### 1) Clone & Install
```
pip install -r requirements.txt
cp .env.example .env
```

### 2) Start MySQL
```
docker compose up -d
# wait a few seconds for DB to be ready
```

### 3) Load Sample Data and Run KPIs
```
python -m src.main --ingest --kpis-sql --kpis-memory
```

This will:
- Create tables (if not present)
- Load the sample CSV/XML in `src/sample_data`
- Print KPIs computed via SQL and via Pandas

You can also run individual steps:
```
# Only ingest
python -m src.main --ingest

# Only SQL KPIs
python -m src.main --kpis-sql

# Only in-memory KPIs
python -m src.main --kpis-memory
```

### 4) Adminer (optional)
Open Adminer at http://localhost:8080 to inspect `akasa` DB:
- System: MySQL
- Server: db
- User: akasa
- Password: akasa
- Database: akasa

## Project Structure
```
akasa_air_task/
├─ docker-compose.yml
├─ requirements.txt
├─ .env.example
├─ README.md
├─ src/
│  ├─ config.py
│  ├─ db.py
│  ├─ models.py
│  ├─ ingest.py
│  ├─ kpis_sql.py
│  ├─ kpis_memory.py
│  ├─ utils.py
│  ├─ main.py
│  └─ sample_data/
│     ├─ customers.csv
│     └─ orders.xml
└─ tests/
   └─ test_memory_kpis.py
```

## Notes on Design
- **Freshness**: Pipeline assumes daily deliveries with only "previous-day" new records. Upserts use `(mobile_number, order_id)` uniqueness to avoid dupes.
- **Scalability**: Proper indexes on join keys and timestamp fields; modular KPIs and chunked ingestion (for large files).
- **Time Zone Awareness**: All timestamps normalized to UTC internally; CLI displays in local Asia/Kolkata when helpful.
- **Security**: Uses parameterized SQL (SQLAlchemy), .env for secrets. Never logs secrets.
- **Error Handling**: Robust parsing with validation; minimal but clear logging.

## If I had more time
- Add Airflow/Prefect orchestration DAG
- S3-based raw/stage/curated layers
- Great Expectations data quality checks
- dbt for modeling + documentation
- Incremental watermarks and late-arrival handling
- Superset/Metabase dashboard

## Running Tests
```
pytest -q
```

––



 ## Enhancements and Key Features

- Streaming XML ingestion (low memory) via `lxml.iterparse`.
- CLI flags:
  - `--order-granularity header|line` (default: `header`)
  - `--date-window <days>` (default: 30)
  - `--now <YYYY-MM-DD>` to make runs deterministic (default: current UTC)
  - `--tz <IANA timezone>` for monthly trend bucketing (default: Asia/Kolkata)
  - `--mask-pii true|false` to mask mobile numbers in outputs (default: true)
- Outputs:
  - CSVs in `outputs/`
  - Plots in `outputs/plots/`
  - Run summary in `outputs/summary.md`
- Defaults for input paths updated to: `task_DE_new_customers.csv` and `task_DE_new_orders.xml`.
