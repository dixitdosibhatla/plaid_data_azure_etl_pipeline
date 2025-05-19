# Plaid Azure ETL Pipeline

## Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline that integrates Plaid's financial transaction data with Microsoft Azure cloud services. The pipeline automates the extraction of transaction and account data from Plaid's sandbox environment, processes and transforms this data, and stores the results in Azure Blob Storage for downstream analysis or reporting.

The pipeline is designed to run on a schedule and showcases best practices for cloud-native serverless architectures using Azure Functions and Blob Storage.

---

## Tech Stack

- **Azure Functions (Python)**: Serverless compute for orchestrating extraction and transformation workflows.
- **Plaid API (Python SDK)**: Financial data provider API to retrieve transaction and account information.
- **Azure Blob Storage**: Cloud storage service to store raw JSON data and transformed CSV files.
- **Pandas**: Data processing and transformation library used to clean, normalize, and structure financial data.
- **Azure Synapse Analytics**: Data warehouse and analytics service to run complex queries and analytics on transformed datasets.
- **Python 3.12**: Programming language used for all development.
- **Environment Variables**: Secure configuration via environment variables for Plaid credentials and Azure connection strings.

---

## End-to-End Data Flow

1. **Scheduled Extraction**
   - An Azure Function triggers monthly (on the 1st day at midnight) to extract transactions from Plaid’s Sandbox API.
   - It dynamically computes the previous month’s date range.
   - The function obtains a sandbox public token, exchanges it for an access token, and requests transaction data for the specified date range.
   - Raw JSON data is saved into Azure Blob Storage under `raw-data/to_process/` with a timestamped filename.

2. **Transformation and Loading**
   - A second Azure Function is triggered automatically when new blobs appear in `raw-data/to_process/`.
   - It reads all new JSON files, extracts relevant transaction and account data, and performs data cleansing and normalization using Pandas.
   - Transformed data is saved as CSV files into Blob Storage containers `transactions_data/` and `accounts_data/` within the `transformed-data` container.
   - Processed raw JSON files are moved from `to_process/` to `processed/` folder to avoid re-processing.

3. **Querying with Azure Synapse Analytics**
   - The `transformed-data` Blob Storage container is linked as an **external data source** within Azure Synapse Analytics.
   - Using `OPENROWSET` SQL commands, Synapse directly queries the CSV files without explicit ingestion or data movement.

## Environment Configuration

The following environment variables must be set for the Azure Functions:

- `PLAID_CLIENT_ID` — Plaid API client ID.
- `PLAID_SECRET` — Plaid API secret.
- `AZURE_CONN_STR` — Azure Blob Storage connection string.
- `EXTRACT_CONTAINER_NAME` — Blob container name for raw data (default: `raw-data`).
- `OUTPUT_CONTAINER_NAME` — Blob container name for transformed data (default: `transformed-data`).

---

## Key Features and Benefits

- Fully serverless architecture with Azure Functions, reducing infrastructure management.
- Automated monthly scheduling and event-driven processing.
- Robust error handling and logging for traceability.
- Modular code design allowing easy extension for additional financial data products or integrations.
- Secure handling of credentials via environment variables.
- Efficient data transformation using Pandas ensuring clean and analytics-ready datasets.
- Direct integration with Azure Synapse Analytics for enterprise-level data warehousing and analytics.


---

**Author:** Dixit Dosibhatla  
**Date:** 2025  
