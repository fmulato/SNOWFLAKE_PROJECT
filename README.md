# 📊 End-to-End Data Pipeline: Kaggle → Snowflake → Power BI

## 1. Overview

This project demonstrates a **complete data pipeline**:

- Extraction of datasets from **Kaggle** using local Python (VS Code) to CSV files
- Storage in **Snowflake** from CSV to raw table
- Transformation into a **Star Schema** in **Power Query**
- Dashboard in **Power BI** consuming the Star Schema

The focus is on **practical data engineering**, dimensional modelling, and BI best practices.

## 2. Pipeline Architecture
Kaggle Dataset
↓
Python (VS Code - Local)
↓
Snowflake (Raw / Repository Schema / Flat Table)
↓
Power Query → Flat Table to Star Schema (Fact + Dimensions)
↓
Power BI Dashboard

## 3. Project Stages

### 3.1 Data Extraction

- Public dataset from Kaggle
- Downloaded via API
- Initial cleaning and processing using `pandas`

**Technologies:** Python, pandas, Kaggle API

### 3.2 Loading into Snowflake

- Single flat table in schema `ANALYTICS_REPO`
- Consistent data types
- Prepared for modelling in Power Query

### 3.3 Transformation in Power Query

- Entity separation → Dimensions and Fact Table
- Creation of dimensions:
  - `Dim_Date`
  - `Dim_Customer`
  - `Dim_Product`
  - `Dim_AgeGroup`
- Creation of the **Fact Table** with metrics:
  - Revenue
  - Quantity
  - Average Ticket

*Note:* The Star Schema is generated **within Power Query** after loading into Snowflake.

## 4. Dimensional Model (Star Schema)

Dim_Date
    |
Dim_Customer — Fact_Sales — Dim_Product
    |
Dim_AgeGrou


## 5. Pipeline Illustrations

![Whiteboard Star Schema via Power Query](./src/images/flow2.png)

## 6. Next Steps

- Automation with Airflow or dbt
- SQL transformations in Snowflake
- Pipeline version control and CI/CD
- Advanced dashboard with dynamic KPIs