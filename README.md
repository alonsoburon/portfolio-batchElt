# DDD Stack Data Pipeline

ELT pipeline implementing medallion architecture with Dagster, dlt, and DuckDB.

## Architecture

**Bronze Layer**: Raw data ingestion (Parquet, CSV, API)  
**Silver Layer**: Cleaned, enriched, and quality-validated data  
**Gold Layer**: Business-ready aggregated metrics

## Tech Stack

- **Orchestration**: Dagster
- **Data Loading**: dlt
- **Storage**: DuckDB

## Features

- Dynamic metadata reporting
- Asset quality checks
- Column-level lineage
- Freshness policies
- Parallel Bronze layer execution

## Quick Start

```bash
git clone <repo-url>
cd portfolio-batchElt
uv sync
dagster dev
```

Access UI at `http://localhost:3000` and materialize assets.