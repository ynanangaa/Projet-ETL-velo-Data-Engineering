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

1. Clone the repository:
   ```bash
   git clone https://github.com/ynanangaa/Projet-ETL-velo-Data-Engineering.git
   cd Projet-ETL-velo-Data-Engineering
   ```

2. Make sure **uv** is installed. If not:

   * **Linux / macOS**

     ```bash
     curl -LsSf https://astral.sh/uv/install.sh | sh
     ```

   * **Windows (PowerShell)**

     ```powershell
     powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
     ```

3. Create and sync the environment:

   ```bash
   uv sync
   ```

---

## Usage

You can run the project in two ways:

### Option 1 — Using uv (recommended)

```bash
uv run streamlit run src/main.py
```

---

### Option 2 — Using the virtual environment

1. Activate the virtual environment:

* **Linux / macOS**

  ```bash
  source .venv/bin/activate
  ```

* **Windows**

  ```bash
  .venv\Scripts\activate
  ```

2. Run the application:

```bash
streamlit run src/main.py
```

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