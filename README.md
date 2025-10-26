# NYC Taxi Analytics Pipeline

ELT pipeline implementing medallion architecture with Dagster, dlt, dbt, DuckDB and Plotly for Orchestration, Ingestion, Transformation, Storage and Visualization.

## Architecture

**Bronze Layer**: Raw data ingestion from NYC Open Data
- Yellow taxi trips (Parquet)
- Taxi zones (CSV) 
- Weather data (API)

**Silver Layer**: Cleaned, enriched, and quality-validated data
- Data type standardization
- Null value handling
- Data quality checks

**Gold Layer**: Business-ready aggregated metrics
- Hourly trip metrics with weather correlation
- Daily metrics by borough
- Statistical analysis and trends

## Tech Stack

- **Orchestration**: Dagster
- **Data Loading**: dlt  
- **Transformation**: dbt
- **Storage**: DuckDB
- **Visualization**: Plotly (Interactive HTML dashboards)

## Features

- **Parallel Bronze ingestion** with asset checks
- **dbt transformations** with built-in testing and schema validation
- **Column-level lineage** tracking across all layers
- **Freshness policies** for data quality monitoring
- **Interactive analytics dashboard** with:
  - Hourly trip volume trends
  - 7-day moving averages
  - Correlation matrix (trips vs weather, fare, distance)
  - Temperature vs trip volume scatter plot with trendline
  - Hourly patterns analysis
  - Borough distribution table

## Data Sources

- **NYC Yellow Taxi Trips**: January 2024 data from NYC Open Data
- **Taxi Zones**: NYC taxi zone lookup table
- **Weather Data**: Historical weather from Open-Meteo API (NYC coordinates)

## Quick Start

```bash
git clone <repo-url>
cd portfolio-batchElt
uv sync
dagster dev
```

Access UI at `http://localhost:3000` and materialize assets. The dashboard will be generated as `dashboard.html` in the project root.

## Project Structure

```
├── src/portfolio_batchelt/
│   ├── assets_dlt.py          # Bronze layer assets
│   ├── assets_dbt.py          # dbt model assets  
│   ├── assets_viz.py          # Dashboard generation
│   └── definitions.py         # Dagster definitions
├── dbt_project/
│   ├── models/
│   │   ├── silver/            # Silver layer models
│   │   └── gold/              # Gold layer models
│   └── dbt_project.yml        # dbt configuration
└── data/                      # Raw data files
```
