# Data Modeling Implementation – Bronze to Gold with SCD Handling (Databricks)

## Overview

This folder contains an **end-to-end data modeling implementation** built on **Databricks**, demonstrating how raw transactional data is transformed into **analytics-ready dimensional models** using a **Bronze → Silver → Gold architecture**.

The implementation highlights:
- Incremental data ingestion
- Data standardization and enrichment
- Dimensional modeling (Facts & Dimensions)
- Slowly Changing Dimensions (SCD Type 1 & Type 2)

This project emphasizes **why strong data modeling is critical** for reliable analytics, reporting, and downstream BI/AI workloads.

---

## Architecture Flow

**Source → Bronze → Silver → Gold**

- **Source Layer**: Simulated transactional data
- **Bronze Layer**: Incremental raw ingestion
- **Silver Layer**: Cleaned, standardized, and merged data
- **Gold Layer**: Star schema with fact & dimension tables
- **SCD Modules**: Demonstrates historical data handling patterns

---

## Folder Structure & Purpose

### `source.py`
Creates and populates the **source transactional table**.

**Key Concepts**
- Simulates raw operational data
- Includes both initial and incremental loads
- Acts as the upstream system for all downstream layers

---

### `bronze.py`
Implements the **Bronze layer ingestion logic**.

**Key Concepts**
- Incremental loading using `max(order_date)`
- Preserves raw data structure
- Ensures only new records are ingested
- Acts as the system of record for downstream processing

---

### `silver.py`
Implements the **Silver layer transformation and merge logic**.

**Key Concepts**
- Data cleansing and standardization
- Derived attributes for analytics readiness
- `MERGE` logic to handle inserts and updates
- Prepares data for dimensional modeling

---

### `gold.py`
Implements the **Gold layer dimensional model (Star Schema)**.

**Dimensions**
- `DimCustomers`
- `DimProducts`
- `DimPaymentKey`
- `DimRegion`
- `DimSales`

**Fact Table**
- `FactSales`

**Key Concepts**
- Surrogate key generation
- De-duplication of dimension records
- Fact-to-dimension joins
- Optimized schema for BI and analytics queries

---

## Slowly Changing Dimensions (SCD)

### SCD Type 1
Demonstrates **SCD Type 1** behavior where historical data is overwritten.

**Key Concepts**
- Updates replace existing values
- No historical tracking
- Suitable for non-historical attributes

---

### SCD Type 2
Demonstrates **SCD Type 2** behavior where historical changes are preserved.

**Key Concepts**
- Maintains full history using:
  - `start_date`
  - `end_date`
  - `is_current`
- Closes existing records and inserts new versions
- Enables accurate historical reporting

---

## Why This Matters

Incorrect data modeling often results in:
- Inconsistent metrics
- Slow or complex queries
- Broken dashboards
- Loss of trust in analytics

This implementation demonstrates how **well-designed data models**, combined with **layered architecture and SCD handling**, create scalable, reliable analytics foundations.

---

## Technologies Used

- Databricks
- Spark SQL
- SQL MERGE operations
- Dimensional modeling techniques

---

## Use Cases

- Learning data warehousing fundamentals
- Understanding Bronze/Silver/Gold architecture
- Implementing SCD Type 1 and Type 2 patterns
- Preparing data for BI and analytics workloads

---

## Notes

- This implementation is for **learning and demonstration purposes**
- Logic can be extended for production-scale pipelines
- Can be integrated with orchestration tools and BI platforms
