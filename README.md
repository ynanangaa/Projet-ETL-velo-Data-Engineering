# Mobility Analysis ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline to process mobility data, specifically focusing on bicycle-sharing systems and related city data. The pipeline ingests real-time bicycle station data, consolidates it into structured tables, and aggregates it for further analysis.

## Table of Contents
1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Installation](#installation)
4. [Usage](#usage)
5. [ETL Workflow](#etl-workflow)

---

## Overview
This ETL pipeline processes and prepares data for mobility analysis by:
- **Ingesting** real-time data from APIs and static sources.
- **Consolidating** raw data into structured formats.
- **Aggregating** data into dimensional and fact tables.

The output is stored in a **DuckDB database** for analytics and visualization.

---

## Project Structure
- **data_ingestion.py**: Fetches data from external sources.
- **data_consolidation.py**: Consolidates raw data into structured tables.
- **data_agregation.py**: Creates dimensional and fact tables for analysis.
- **main.py**: Orchestrates the entire ETL process.

---

## Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/ynanangaa/Projet-ETL-velo-Data-Engineering
   cd Projet-ETL-velo-Data-Engineering
   ```

2. Create a Python virtual environment:
   ```bash
   python -m venv .venv
   ```

3. Activate the virtual environment:
   - **On Windows:**
     ```bash
     .venv\Scripts\activate
     ```
   - **On macOS/Linux:**
     ```bash
     source venv/bin/activate
     ```

4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. Ensure **DuckDB** is installed.

---

## Usage
Run the ETL pipeline using the `main.py` script:
```bash
python src/main.py
```
The process includes:
1. Ingesting data.
2. Consolidating it into structured tables.
3. Aggregating the data for analysis.

Logs will indicate the progress of each phase.

After running the ETL pipeline, you can perform SQL queries on the resulting **DuckDB database** to analyze the processed data. For example:

- **Number of available bicycle docks in a city**:
   ```sql
   SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
   FROM DIM_CITY dm INNER JOIN (
       SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
       FROM FACT_STATION_STATEMENT
       WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
       GROUP BY CITY_ID
   ) tmp ON dm.ID = tmp.CITY_ID
   WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse', 'montpellier');
   ```

- **Average number of available bicycles at each station**:
   ```sql
   SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
   FROM DIM_STATION ds JOIN (
       SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
       FROM FACT_STATION_STATEMENT
       GROUP BY station_id
   ) AS tmp ON ds.id = tmp.station_id;
   ```

---

## ETL Workflow
1. **Data Ingestion**:
   - Fetches real-time bicycle station data and city information.
2. **Data Consolidation**:
   - Combines and structures raw data into intermediate tables.
3. **Data Aggregation**:
   - Produces dimensional and fact tables for analysis.
