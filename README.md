
# Akasa Air - Customer & Order Data Pipeline

A data engineering pipeline built to ingest, process, and analyze customer and order data for Akasa Air.

## Overview

This project was developed as part of a data engineering assignment. It processes two data sources:
- Customer records (CSV format)
- Order records (XML format)

The pipeline supports two execution modes:
1. **SQL-based approach** using MySQL and SQLAlchemy
2. **In-memory approach** using Pandas for fast local processing

## Key Features

- Ingests and parses both CSV and XML data sources
- Computes important business KPIs:
  - Repeat customer identification
  - Monthly order trends
  - Regional revenue analysis
  - Top customers by spending (last 30 days)
- Supports both database-backed and in-memory computation paths
- Includes Docker setup for easy local development
- Timezone handling (normalized to UTC with Asia/Kolkata display)
- PII masking support
- Unit tests for core KPI calculations

## Tech Stack

- **Python 3.10+**
- **Pandas** (in-memory data processing)
- **SQLAlchemy + MySQL** (database approach)
- **Docker & Docker Compose**
- **lxml** (streaming XML parsing)
- **Pytest** (testing)

## Project Structure

```
akasa_air_task_v4/
├── src/
│   ├── ingest.py          # Data ingestion logic
│   ├── kpis_sql.py        # KPI calculations using SQL
│   ├── kpis_memory.py     # KPI calculations using Pandas
│   ├── db.py              # Database connection and models
│   ├── main.py            # CLI entry point
│   └── utils.py
├── tests/
├── docker-compose.yml
└── requirements.txt
```

## How to Run

```bash
# Using Docker (recommended)
docker-compose up

# Or run locally
python src/main.py --mode memory
```

## Key Learnings

- Handling multiple data formats (CSV + XML) in a single pipeline
- Designing dual execution paths (SQL vs in-memory)
- Importance of proper data modeling and indexing for performance
- Building testable and configurable data pipelines

## Author

**Philip Mathew**  
GitHub: [@philipmk42](https://github.com/philipmk42)
```


Would you like me to give you the exact commands for this repo now?
