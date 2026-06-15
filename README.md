# Akasa Air - Customer & Order Data Pipeline

A data engineering pipeline to ingest, process, and analyze customer and order data.

## Overview

This project was developed as part of a Data Engineering assignment for Akasa Air. It processes two data sources:
- **Customers** data (CSV format)
- **Orders** data (XML format)

The pipeline supports **two execution modes**:
1. **SQL-based approach** using MySQL and SQLAlchemy
2. **In-memory approach** using Pandas for fast local analysis

## Key Features

- Ingests and parses both CSV and XML data
- Computes useful business KPIs:
  - Repeat customers
  - Monthly order trends
  - Regional revenue
  - Top customers by spend (last 30 days)
- Supports both database-backed and in-memory computation
- Dockerized setup for easy local development
- Timezone handling and PII masking support
- Includes unit tests

## Tech Stack

- **Python 3.10+**
- **Pandas** (in-memory processing)
- **SQLAlchemy + MySQL** (database approach)
- **Docker & Docker Compose**
- **lxml** (streaming XML parsing)
- **Pytest** (testing)

## Project Structure
akasa_air_task_v4/
├── src/
│   ├── ingest.py           # Data ingestion logic (CSV + XML)
│   ├── kpis_sql.py         # KPI calculations using SQL
│   ├── kpis_memory.py      # KPI calculations using Pandas
│   ├── db.py               # Database models and connection
│   ├── main.py             # CLI entry point
│   └── utils.py
├── tests/
│   └── test_memory_kpis.py
├── docker-compose.yml
├── requirements.txt
└── .env.example
text## How to Run

```bash
# Using Docker (Recommended)
docker-compose up

# Or run locally
python src/main.py --mode memory
Key Learnings & Skills Demonstrated

Handling multiple data formats (CSV + XML) in a single pipeline
Building dual execution paths (SQL vs In-memory)
Data modeling, indexing, and query optimization
Creating configurable and testable data pipelines
Docker-based development environment

Author
Philip Mathew
GitHub: @philipmk42
