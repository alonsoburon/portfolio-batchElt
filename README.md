# 🚀 Portfolio Batch ELT Pipeline

A comprehensive data pipeline built with **Dagster** and **dlt** that demonstrates modern data engineering practices including Bronze-Silver-Gold architecture, Iceberg lakehouse patterns, and multi-source data integration.

## 📊 Architecture Overview

This pipeline implements a **medallion architecture** (Bronze-Silver-Gold) with the following layers:

- **🏗️ Infrastructure**: GCP setup and BigQuery connections
- **🥉 Bronze Layer**: Raw data ingestion from multiple sources
- **🥈 Silver Layer**: Cleaned and standardized data
- **🥇 Gold Layer**: Business-ready aggregated datasets

## 🔄 Data Flow

![Data Flow](./Global_Asset_Lineage.svg)

## 🛠️ Technology Stack

- **Orchestration**: [Dagster](https://dagster.io/) - Modern data orchestration platform
- **Data Loading**: [dlt](https://dlthub.com/) - Open-source data loading library
- **Data Warehouse**: [Google BigQuery](https://cloud.google.com/bigquery)
- **Object Storage**: [Google Cloud Storage](https://cloud.google.com/storage)
- **Data Format**: [Apache Iceberg](https://iceberg.apache.org/) - Open table format for lakehouse
- **Cloud Provider**: [Google Cloud Platform](https://cloud.google.com/)

## 📁 Project Structure

```
portfolio-batchElt/
├── dlt_sources/                  # Data source definitions
│   ├── jsonplaceholder_users.py  # API source for user data
│   ├── nyc_taxi_data.py          # Yellow taxi data source
│   ├── fhv_taxi_data.py          # For-hire vehicle data source
│   └── green_taxi_data.py        # Green taxi data source
├── src/portfolio_batchelt/
│   ├── assets.py                 # Dagster asset definitions
│   └── definitions.py            # Dagster definitions
├── .dlt/
│   └── secrets.toml              # DLT configuration and credentials
├── bq_service_account.json       # GCP service account credentials for BigQuery
├── pyproject.toml                # Project dependencies
└── README.md                     # This file
```

**⚠️ Security Note**: The service account JSON file contains sensitive credentials and should never be committed to version control. Add it to `.gitignore`.

## 🚀 Getting Started

### Prerequisites

- Python 3.12+
- Google Cloud Platform account with BigQuery and Cloud Storage enabled
- Service account with the following roles:
  - BigQuery Admin
  - Storage Admin
  - BigQuery Connection Admin
  - Service Account Token Creator
- GCS bucket: `your-bucket-name`
- BigQuery project: `your-project-id`

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd portfolio-batchElt
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -e .
   ```

4. **Configure credentials**
   
   Create two credential files:
   - `bq_service_account.json` - GCP service account JSON file
   - `.dlt/secrets.toml` - DLT configuration (see Configuration section below)

### Running the Pipeline

1. **Start Dagster UI**
   ```bash
   dagster dev
   ```

2. **Access the UI**
   Open your browser to `http://localhost:3000`

3. **Materialize assets**
   - Click on individual assets to run them
   - Or use the "Materialize All" button to run the entire pipeline

## 📋 Assets Overview

All assets are configured with **kinds** that provide branded icons in the Dagster UI:

- **`gcp`** ☁️ - Google Cloud Platform infrastructure
- **`dlt`** 🔄 - Data loading and transformation using dlt
- **`table`** 📊 - Standard database tables
- **`iceberg`** 🧊 - Apache Iceberg lakehouse tables

### 🏗️ Infrastructure Assets

| Asset | Type | Icon | Description |
|-------|------|------|-------------|
| `infrastructure` | `gcp` | ☁️ | Creates GCP infrastructure (datasets, connections, IAM) |

### 🥉 Bronze Layer Assets

| Asset | Type | Icon | Description |
|-------|------|------|-------------|
| `users_bronze` | `dlt` | 🔄 | Raw user data from JSONPlaceholder API |
| `taxi_bronze` | `dlt` | 🔄 | Raw NYC yellow taxi trip data |
| `fhv_bronze` | `dlt` | 🔄 | Raw for-hire vehicle trip data |
| `green_bronze` | `dlt` | 🔄 | Raw green taxi data (Iceberg lakehouse) |

### 🥈 Silver Layer Assets

| Asset | Type | Icon | Description |
|-------|------|------|-------------|
| `users_silver` | `table` | 📊 | Cleaned and flattened user data |
| `taxi_silver` | `table` | 📊 | Standardized yellow taxi data |
| `fhv_silver` | `table` | 📊 | Standardized FHV data |
| `green_silver` | `iceberg` | 🧊 | Green taxi data in Iceberg format |

### 🥇 Gold Layer Assets

| Asset | Type | Icon | Description |
|-------|------|------|-------------|
| `users_gold` | `table` | 📊 | User counts by city |
| `trips_gold` | `table` | 📊 | Combined trip data by location |
| `green_gold` | `table` | 📊 | Green taxi aggregations by payment type |

## 🔍 Key Features

### 1. **Multi-Source Data Integration**
- REST API data (JSONPlaceholder)
- Remote Parquet files (NYC taxi data)
- Different data formats and structures

### 2. **Iceberg Lakehouse Pattern**
- Demonstrates modern lakehouse architecture
- Managed BigLake Iceberg tables
- Efficient data storage and querying

### 3. **Medallion Architecture**
- Clear separation of concerns
- Data quality improvements at each layer
- Business-ready analytics datasets

### 4. **Infrastructure as Code**
- Automated GCP resource provisioning
- BigQuery dataset creation
- IAM permissions management

### 5. **Data Quality Checks**
- Asset checks for data validation
- Error handling and logging
- Dependency management

## 🔧 Configuration

### Credentials Setup

**Service Account JSON**: Download from GCP Console and save as `bq_service_account.json`

**DLT Configuration**: Create `.dlt/secrets.toml`:

```toml
[destination.filesystem]
bucket_url = "gs://your-bucket-name/dlt_staging"

[destination.filesystem.credentials]
project_id = "your-project-id"
private_key = """YOUR_PRIVATE_KEY_HERE"""
client_email = "your-service-account@your-project.iam.gserviceaccount.com"

[destination.bigquery]
location = "us-east1"

[destination.bigquery.credentials]
project_id = "your-project-id"
private_key = """YOUR_PRIVATE_KEY_HERE"""
client_email = "your-service-account@your-project.iam.gserviceaccount.com"
```

**Note**: Use the same service account credentials for both files. Private keys must be in triple quotes.

### GCP Resources

The pipeline creates and manages:
- **BigQuery Datasets**: `bronze_layer`, `silver_layer`, `gold_layer`
- **BigQuery Connection**: For BigLake/GCS integration
- **IAM Permissions**: Service account access to GCS bucket

## 📊 Data Sources

### 1. JSONPlaceholder API
- **Source**: `https://jsonplaceholder.typicode.com/users`
- **Data**: User profiles with addresses
- **Frequency**: On-demand

### 2. NYC Taxi Data
- **Source**: NYC TLC public datasets
- **Data**: Yellow taxi trips (January 2023)
- **Format**: Parquet files
- **Size**: ~100 records per run

### 3. FHV (For-Hire Vehicle) Data
- **Source**: NYC TLC public datasets
- **Data**: For-hire vehicle trips (January 2023)
- **Format**: Parquet files

### 4. Green Taxi Data
- **Source**: NYC TLC public datasets
- **Data**: Green taxi trips (January 2023)
- **Format**: Parquet files
- **Special**: Used for Iceberg lakehouse demo

## 🎯 Use Cases

This pipeline demonstrates:

1. **Data Engineering Best Practices**
   - Modern data stack integration
   - Scalable architecture patterns
   - Infrastructure automation

2. **Real-World Scenarios**
   - Multi-source data integration
   - Data quality and validation
   - Performance optimization

3. **Cloud-Native Solutions**
   - GCP service integration
   - Serverless data processing
   - Cost-effective storage patterns

## 🚀 Next Steps

To extend this pipeline, consider:

1. **Adding More Data Sources**
   - Database connections
   - Streaming data sources
   - File-based data sources

2. **Enhanced Data Quality**
   - Great Expectations integration
   - Data profiling
   - Anomaly detection

3. **Advanced Analytics**
   - ML feature engineering
   - Real-time dashboards
   - Data science workflows

4. **Monitoring & Alerting**
   - Data quality metrics
   - Performance monitoring
   - Error notifications

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions or issues:
- Check the [Dagster documentation](https://docs.dagster.io/)
- Review the [dlt documentation](https://dlthub.com/docs)
- Open an issue in this repository

---

**Built with ❤️ using Dagster and dlt**
