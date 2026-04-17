# Mobility Analysis ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline to process mobility data from bicycle-sharing systems and expose it through an interactive dashboard.

---

## Overview

The pipeline:
- Ingests real-time bicycle station and city data
- Transforms and structures the data into analytical tables
- Stores results in a DuckDB database
- Displays insights through an interactive Streamlit dashboard

---

## Project Structure

- **data_ingestion.py**: Fetches data from external APIs
- **data_consolidation.py**: Cleans and structures raw data
- **data_agregation.py**: Builds analytical tables (dimensions & facts)
- **data_visualization.py**: Streamlit dashboard (maps, charts, KPIs)
- **main.py**: Orchestrates the full pipeline (ETL + visualization)

---

## Installation

## Installation

Clone the repository and move into the project directory:

```bash
git clone <repo-url>
cd <repo-folder>
```

Install **uv** if not already available:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Documentation: https://docs.astral.sh/uv/

Sync the environment:

```bash
uv sync
```

---

## Usage

You can run the project in two ways:

### Option 1 — Using uv

```bash
uv run streamlit run src/main.py
```

---

### Option 2 — Using Docker

1. Activate the virtual environment:

Build and run the application:

```bash
docker compose up -d
```
The app will be available at: http://localhost:8501

---

This will:

1. Execute the full ETL pipeline (ingestion → consolidation → aggregation)
2. Launch the interactive Streamlit dashboard

---

## Dashboard Features

The Streamlit interface provides:

* 📍 **Interactive map of bicycle stations**

  * Location, capacity, and status filtering

* 🏙️ **Available docks by city**

  * Aggregated view for major French cities

* 🚲 **Station analysis**

  * Top stations with most bikes available
  * Distribution of bike availability

* 📊 **Key KPIs**

  * Total stations
  * Available docks
  * Average bikes per station

---

## ETL Workflow (Simplified)

1. **Ingestion**
   Collects real-time station and city data

2. **Consolidation**
   Cleans and structures the raw data

3. **Aggregation**
   Builds analytical tables used by the dashboard

---
