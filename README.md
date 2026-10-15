# Formula 1 Data Engineering Project

## Project Overview
This project implements a Formula 1 data engineering pipeline using Azure Databricks and PySpark to ingest, transform, and analyze Formula 1 racing data.

The project organizes data into Bronze, Silver, and Gold layers and prepares curated datasets for analytics.

## Technologies Used
- Azure Databricks
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- PySpark
- Spark SQL
- Delta Lake
- Databricks SQL

## Project Architecture

### 1. Common
Contains shared environment configuration and reusable helper functions for data processing.

### 2. Setup
Contains the SQL scripts used to configure the project environment.

### 3. Bronze Layer
Ingests Formula 1 source data, including circuits, races, constructors, drivers, results, and sprint events.

### 4. Silver Layer
Cleans, transforms, and prepares the ingested data for downstream processing.

### 5. Gold Layer
Builds analytical dimension and fact tables, including race, constructor, driver, and results datasets.

### 6. Analytics
Contains SQL views and analysis related to driver standings, constructor standings, and dominant drivers and constructors.

### 7. Datasets
Contains the Formula 1 source datasets used by the project.

## Key Learning Outcomes
- Data ingestion using PySpark
- Data transformation with Spark SQL
- Layered data processing using the Bronze, Silver, and Gold approach
- Building dimension and fact tables
- Preparing datasets for analytics
- Organizing a data engineering project using reusable scripts

## Repository Structure
- `00-common/` — Shared configurations and helper functions
- `01-setup/` — Project setup scripts
- `02-bronze/` — Data ingestion
- `03-silver/` — Data transformation
- `04-Gold/` — Dimension and fact table creation
- `05-Analytics/` — SQL views and analytics
- `Datasets/` — Source data

## How to Use
1. Clone this repository.
2. Open the notebooks and scripts in a compatible Databricks workspace.
3. Configure the required storage paths, catalog, schema, and permissions.
4. Make sure the required source datasets are available.
5. Execute the scripts in the appropriate order: setup, Bronze, Silver, Gold, and Analytics.

The scripts may require environment-specific configuration before execution.

## Author
Manisha R P

GitHub: https://github.com/ManishaCodes
