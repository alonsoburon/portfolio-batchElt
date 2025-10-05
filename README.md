# Portfolio ELT Pipeline: Data Engineering Showcase

This repository contains a complete, end-to-end ELT pipeline designed to demonstrate modern data engineering principles and best practices. The goal was to build a robust, scalable, and observable system using a production-grade technology stack.

This project integrates data from multiple sources (APIs and batch files), processes it through a medallion architecture, and models it for analysis, also using declarative orchestration and infrastructure as code.

## 📊 Architecture: The Medallion Approach

I implemented a **medallion architecture** (Bronze-Silver-Gold) to ensure a clear separation of data layers, facilitating governance, quality, and scalability.

- **🏗️ Infrastructure**: Initial setup on GCP and BigQuery connections, managed as code.
- **🥉 Bronze Layer**: Raw data ingestion in its original format. This layer serves as an immutable, persistent source of truth for the system.
- **🥈 Silver Layer**: Cleaned, standardized, and conformed data. Quality rules and transformations are applied here to unify data from different sources.
- **🥇 Gold Layer**: Aggregated and business-optimized datasets. These are the data models ready to be consumed by BI tools, analysts, or machine learning applications.

## 🔄 Data Flow

The following diagram illustrates the data lineage across the different layers and technologies.

![Data Flow](./Global_Asset_Lineage.svg)

## 🛠️ Technology Stack

The tool selection focused on modern, efficient, and scalable solutions within the data ecosystem.

-   **Orchestration**: [Dagster](https://dagster.io/) - Chosen for its declarative, asset-centric approach and its excellent UI for visualizing lineage and pipeline status.
-   **Data Loading**: [dlt](https://dlthub.com/) - Selected for its simplicity and efficiency in data ingestion, enabling rapid implementation of new sources.
-   **Data Warehouse**: [Google BigQuery](https://cloud.google.com/bigquery) - Used as a serverless and highly scalable data warehouse (Also its free tier is really generous!).
-   **Object Storage**: [Google Cloud Storage](https://cloud.google.com/storage) - Employed as the foundation of the Data Lake for the Bronze layer and staging (No free tier here, but dirt cheap).
-   **Table Format**: [Apache Iceberg](https://iceberg.apache.org/) - Implemented to demonstrate a modern lakehouse architecture over BigQuery (BigLake), offering ACID transactions and time travel capabilities.

## 📁 Project Structure

```
portfolio-batchElt/
├── dlt_sources/                  # Extraction logic for each data source
│   ├── jsonplaceholder_users.py  # API source for user data
│   ├── nyc_taxi_data.py          # Parquet source for yellow taxi data
│   ├── fhv_taxi_data.py          # Parquet source for for-hire vehicle data 
│   └── green_taxi_data.py        # Parquet source for green taxi data
├── src/portfolio_batchelt/
│   ├── assets.py                 # Dagster asset definitions
│   └── definitions.py            # Dagster definitions
├── .dlt/
│   └── secrets.toml              # DLT configuration and credentials
├── bq_service_account.json       # GCP service account credentials for BigQuery
├── pyproject.toml                # Project dependencies
└── README.md                     # This file
```

**⚠️ Remember to never push your .json files!**

## 🚀 Getting Started

### Prerequisites

-   Python 3.12+
-   Google Cloud Platform account with BigQuery and Cloud Storage enabled
-   A service account with the following roles:
    -   BigQuery Admin
    -   Storage Admin
    -   BigQuery Connection Admin
    -   Service Account Token Creator
-   A GCS bucket: `your-bucket-name`
-   A BigQuery project: `your-project-id`

### Installation

1.  **Clone the repository**
    ```bash
    git clone https://github.com/alonsoburon/portfolio-batchElt.git
    cd portfolio-batchElt
    ```

2.  **Create a virtual environment**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    ```

3.  **Install dependencies**
    ```bash
    pip install -e .
    ```

4.  **Configure credentials**

    Create two credential files:
    -   `bq_service_account.json` - Your GCP service account JSON file.
    -   `.dlt/secrets.toml` - The DLT configuration file (see Configuration section below).

### Running the Pipeline

1.  **Start the Dagster UI**
    ```bash
    dagster dev
    ```

2.  **Access the UI**
    Open your browser to `http://localhost:3000`

3.  **Materialize Assets**
    -   Click on individual assets to run them.
    -   Or use the "Materialize All" button to run the entire pipeline.

## 📋 Asset Overview

All assets are configured with **kinds** to provide branded icons in the Dagster UI:

-   **`gcp`** ☁️ - Google Cloud Platform infrastructure
-   **`dlt`** 🔄 - Data loading and transformation using dlt
-   **`table`** 📊 - Standard database tables
-   **`iceberg`** 🧊 - Apache Iceberg lakehouse tables

### 🏗️ Infrastructure Assets

| Asset          | Type  | Icon | Description                                            |
| -------------- | ----- | ---- | ------------------------------------------------------ |
| `infrastructure` | `gcp` | ☁️   | Creates GCP infrastructure (datasets, connections, IAM) |

### 🥉 Bronze Layer Assets

| Asset          | Type  | Icon | Description                                    |
| -------------- | ----- | ---- | ---------------------------------------------- |
| `users_bronze`   | `dlt` | 🔄   | Raw user data from JSONPlaceholder API         |
| `taxi_bronze`    | `dlt` | 🔄   | Raw NYC yellow taxi trip data                  |
| `fhv_bronze`     | `dlt` | 🔄   | Raw for-hire vehicle trip data                 |
| `green_bronze`   | `dlt` | 🔄   | Raw green taxi data (Iceberg lakehouse)        |

### 🥈 Silver Layer Assets

| Asset        | Type      | Icon | Description                        |
| ------------ | --------- | ---- | ---------------------------------- |
| `users_silver` | `table`   | 📊   | Cleaned and flattened user data    |
| `taxi_silver`  | `table`   | 📊   | Standardized yellow taxi data      |
| `fhv_silver`   | `table`   | 📊   | Standardized FHV data              |
| `green_silver` | `iceberg` | 🧊   | Green taxi data in Iceberg format  |

### 🥇 Gold Layer Assets

| Asset      | Type    | Icon | Description                             |
| ---------- | ------- | ---- | --------------------------------------- |
| `users_gold` | `table` | 📊   | User counts by city                     |
| `trips_gold` | `table` | 📊   | Combined trip data by location          |
| `green_gold` | `table` | 📊   | Green taxi aggregations by payment type |