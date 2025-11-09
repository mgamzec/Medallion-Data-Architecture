
# Medallion Architecture Implementation (PostgreSQL)

This repository demonstrates the implementation of a modern **Data Lake/Data Warehouse architecture**, specifically the **Medallion Architecture (Bronze, Silver, Gold)**, using **PostgreSQL**.

The patterns used here are generally **SQL-agnostic** and can be adapted to any SQL-based database system (e.g., SQL Server, MySQL, Snowflake, Redshift) by adjusting the syntax.

---

## Project Goal

The primary goal of this project is to illustrate a robust and traceable **ETL/ELT process** by segregating data into three distinct layers:

- 🥉 **Bronze (Raw)**: For data ingestion and storage.  
- 🥈 **Silver (Cleaned)**: For data quality, validation, and standardization.  
- 🥇 **Gold (Analytic)**: For business-ready aggregations and reporting.  

The sample data used for this demonstration is the **NYC Taxi Vendor Data Set**.

---

## Architecture Layers and SQL Scripts

The project consists of three main SQL scripts (`ddl_bronze.sql`, `ddl_silver.sql`, `ddl_gold.sql`) that execute the creation, transformation, and analysis within each layer.

---

### 🥉 1. BRONZE Layer (Raw Data)

The Bronze layer serves as the raw ingestion zone. Data is stored **as-is** from the source, maintaining its original structure and data types.

#### 🔑 Key Functions
- **Schema and Table Creation**: Creates the `bronze` schema and the `taxi_trips_raw` table.  
- **Traceability**: Includes essential metadata fields like `load_timestamp` and `source_file` for lineage and auditing.  
- **Indexing**: Index on `load_timestamp` for efficient downstream querying.  
- **Data Integrity Check**: Raw data intentionally includes quality issues (e.g., negative fares, invalid coordinates) for demonstration.  

####  Code Snippet & Description

| Code | Description |
|------|--------------|
| `CREATE SCHEMA IF NOT EXISTS bronze;` | Creates the logical container for raw data. |
| `CREATE TABLE bronze.taxi_trips_raw (...);` | Defines the raw structure, mirroring the source data. |
| `CREATE INDEX idx_bronze_load_timestamp ...;` | Optimizes queries by ingestion time. |
| `INSERT INTO bronze.taxi_trips_raw (...);` | Inserts sample data, including data quality issues. |
| `SELECT COUNT(*), SELECT * ...;` | Basic validation for data load size and integrity. |

---

### 🥈 2. SILVER Layer (Cleaned and Conformed Data)

The Silver layer handles **data cleansing, standardization, and validation**, ensuring consistency and reliability before advanced analysis.

#### 🔑 Key Functions
- **Data Quality Enforcement**: Filters or flags invalid records (negative fares, out-of-bounds trips, unreasonable durations).  
- **Standardization**: Converts raw codes (e.g., `payment_type`) into human-readable text.  
- **Feature Engineering**: Derives new fields:
  - `trip_duration_minutes`
  - `trip_speed_mph`
- **Quality Flags**: Introduces boolean columns:
  - `is_valid_coordinates`
  - `is_valid_fare`
  - `is_valid_record`
- **Data Quality Report**: Generates a summary in `data_quality_report` showing quality metrics.

#### Code Snippet & Description

| Code | Description |
|------|--------------|
| `CREATE TABLE silver.taxi_trips_cleaned (...);` | Defines the cleaned table structure with quality flags and derived fields. |
| `CREATE INDEX idx_silver_valid_trips ...;` | Enables fast filtering for valid records. |
| `INSERT INTO silver.taxi_trips_cleaned SELECT ...;` | Main ELT transformation, includes CASE statements and logical checks. |
| `CREATE TABLE silver.data_quality_report AS SELECT ...;` | Summarizes cleaning results and valid record ratios. |

---

### 🥇 3. GOLD Layer (Business-Ready Analytics)

The Gold layer is the **presentation/analytics** layer — tightly modeled, aggregated, and ready for BI tools or dashboards.

#### 🔑 Key Functions
- **Daily Trip Summary**: Aggregates total trips, revenue, and average speed.  
- **Performance Analysis**: Breaks down data by vendor and payment type.  
- **Executive Views**: Pre-calculated dashboards (e.g., “Today vs. Yesterday Revenue”).  
- **Data Quality Monitoring**: Final monitoring view comparing all layers.

#### Code Snippet & Description

| Code | Description |
|------|--------------|
| `CREATE TABLE gold.daily_trip_summary AS SELECT ... GROUP BY date;` | Aggregates Silver data into daily operational metrics. |
| `CREATE TABLE gold.vendor_performance AS SELECT ...;` | Business logic summarization by vendor. |
| `CREATE OR REPLACE VIEW gold.executive_summary_dashboard AS ...;` | BI-ready executive dashboard. |
| `CREATE OR REPLACE VIEW gold.data_quality_monitor_view AS ...;` | End-to-end quality transparency view. |

---

## Prerequisites

To run these scripts, you’ll need:

- **Database**: A running PostgreSQL instance (or any other SQL DB).  
- **Client**: A database client (e.g., DBeaver, pgAdmin) to connect and execute the scripts.  
