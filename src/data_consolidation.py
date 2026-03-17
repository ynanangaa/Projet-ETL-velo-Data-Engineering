from datetime import datetime
import logging

import duckdb

logging.basicConfig(level=logging.INFO)

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3
MONTPELLIER_CITY_CODE = 4


def create_consolidate_tables():
    """
    Creates necessary consolidated tables in the DuckDB database.
    Executes the SQL statements from the provided file.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


def consolidate_paris_station():
    """
    Consolidates the Paris station data by reading from the Paris real-time bicycle data
    and normalizing it before storing it into the CONSOLIDATE_STATION table.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    # Consolidation logic for Paris Bicycle data

    con.execute(f"""
    INSERT OR REPLACE INTO CONSOLIDATE_STATION
    SELECT 
        '{PARIS_CITY_CODE}' || '-' || stationcode AS id,
        stationcode AS code,
        name,
        nom_arrondissement_communes AS city_name,
        code_insee_commune AS city_code,
        NULL AS address,
        coordonnees_geo.lon AS longitude,
        coordonnees_geo.lat AS latitude,
        is_installed AS status,
        CURRENT_date AS created_date,
        capacity
    FROM read_json('data/raw_data/{today_date}/paris_realtime_bicycle_data.json')
    """)

    logging.info("Paris Bicycle data consolidated successfully.")


def consolidate_montpellier_station():
    """
    Consolidates the Montpellier station data by reading both real-time bicycle status
    and station information data, merging them, and storing the result in the
    CONSOLIDATE_STATION table.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    con.execute(f"""
    INSERT OR REPLACE INTO CONSOLIDATE_STATION
    WITH status_data AS (
        SELECT *
        FROM read_json(
            'data/raw_data/{today_date}/montpellier_realtime_bicycle_station_status_data.json',
            columns = {{
                data: 'STRUCT(stations STRUCT(
                    station_id VARCHAR,
                    is_installed INTEGER
                )[])'
            }}
        )
    ),
    station_status AS (
        SELECT UNNEST(data.stations) AS s
        FROM status_data
    ),
    info_data AS (
        SELECT *
        FROM read_json(
            'data/raw_data/{today_date}/montpellier_realtime_bicycle_station_information_data.json',
            columns = {{
                data: 'STRUCT(stations STRUCT(
                    station_id VARCHAR,
                    name VARCHAR,
                    lon DOUBLE,
                    lat DOUBLE,
                    capacity INTEGER
                )[])'
            }}
        )
    ),
    station_info AS (
        SELECT UNNEST(data.stations) AS i
        FROM info_data
    )

    SELECT
        '{MONTPELLIER_CITY_CODE}' || '-' || s.station_id AS id,
        s.station_id AS code,
        i.name,
        'Montpellier' AS city_name,
        '34172' AS city_code,
        NULL AS address,
        i.lon AS longitude,
        i.lat AS latitude,
        CASE
            WHEN s.is_installed = 1 THEN 'OUI'
            WHEN s.is_installed = 0 THEN 'NON'
        END AS status,
        CURRENT_DATE AS created_date,
        i.capacity
    FROM station_status s
    JOIN station_info i
    ON s.station_id = i.station_id
    """)

    logging.info("Montpellier Bicycle data consolidated")


def consolidate_nantes_toulouse_station_data(city="Nantes"):
    """
    Consolidates the station data for Nantes and Toulouse by reading the corresponding
    real-time bicycle data, processing it, and storing it in the CONSOLIDATE_STATION table.

    Args:
        city (str): The city name for consolidation ('Nantes' or 'Toulouse').
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    city_code = NANTES_CITY_CODE
    city_code_insee_commune = "44109"

    # Handle Toulouse specific data
    if city == "Toulouse":
        city_code = TOULOUSE_CITY_CODE
        city_code_insee_commune = "31555"
    
    json_path = f"data/raw_data/{today_date}/{city.lower()}_realtime_bicycle_data.json"

    con.execute(f"""
    INSERT OR REPLACE INTO CONSOLIDATE_STATION
    SELECT 
        '{city_code}' || '-' || number AS id,
        CAST(number AS VARCHAR) AS code,
        COALESCE(SPLIT_PART(name, '-', 2), name) AS name,
        '{city}' AS city_name,
        {city_code_insee_commune} AS city_code,
        address,
        position.lon AS longitude,
        position.lat AS latitude,
        CASE
            WHEN status = 'OPEN' THEN 'OUI'
            WHEN status = 'CLOSED' THEN 'NON'
            ELSE status
        END AS status,
        CURRENT_date AS created_date,
        bike_stands AS capacity
    FROM read_json(
        '{json_path}',
        columns = {{
            number: 'INTEGER',
            name: 'VARCHAR',
            address: 'VARCHAR',
            position: 'STRUCT(lon DOUBLE, lat DOUBLE)',
            status: 'VARCHAR',
            bike_stands: 'INTEGER'
    }})
    """)
    
    logging.info(f"{city} Bicycle data consolidated successfully.")


def consolidate_station_data():
    """
    Consolidates station data for Paris, Nantes, Toulouse, and Montpellier by
    calling the respective consolidation functions.
    """
    consolidate_paris_station()
    consolidate_nantes_toulouse_station_data("Nantes")
    consolidate_nantes_toulouse_station_data("Toulouse")
    consolidate_montpellier_station()


def consolidate_city_data():
    """
    Consolidates city data by reading from the commune data JSON, processing it,
    and inserting it into the CONSOLIDATE_CITY table.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    con.execute(f"""
    INSERT OR REPLACE INTO CONSOLIDATE_CITY
    SELECT 
        code AS id,
        nom AS name,
        population AS nb_inhabitants,
        CURRENT_DATE as created_date
    FROM read_json('data/raw_data/{today_date}/commune_data.json')
    """)

    logging.info("Cities data consolidated successfully")


def consolidate_station_statement_paris_data():
    """
    Consolidates station statement data for Paris by processing real-time bicycle data
    and inserting it into the CONSOLIDATE_STATION_STATEMENT table.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    # Insert data into database
    con.execute(f"""
    INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT
    SELECT 
        '{PARIS_CITY_CODE}' || '-' || stationcode AS station_id,
        numdocksavailable AS bicycle_docks_available,
        numbikesavailable AS bicycle_available,
        duedate AS last_statement_date,
        CURRENT_date AS created_date,
    FROM read_json('data/raw_data/{today_date}/paris_realtime_bicycle_data.json')
    """)

    logging.info("Paris Station Statement data consolidated successfully.")


def consolidate_station_statement_montpellier_data():
    """
    Consolidates station statement data for Montpellier by processing real-time station
    status data and inserting it into the CONSOLIDATE_STATION_STATEMENT table.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    con.execute(f"""
    INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT
    WITH raw_status AS (
        SELECT UNNEST(data.stations) AS s
        FROM read_json(
            'data/raw_data/{today_date}/montpellier_realtime_bicycle_station_status_data.json',
            columns = {{
                data: 'STRUCT(stations STRUCT(
                    station_id VARCHAR,
                    num_docks_available INTEGER,
                    num_bikes_available INTEGER,
                    last_reported BIGINT
                )[])'
            }}
        )
    )

    SELECT
        '{MONTPELLIER_CITY_CODE}' || '-' || s.station_id AS station_id,
        s.num_docks_available AS bicycle_docks_available,
        s.num_bikes_available AS bicycle_available,
        STRFTIME(
            TO_TIMESTAMP(s.last_reported),
            '%Y-%m-%dT%H:%M:%S+00:00'
        ) AS last_statement_date,
        CURRENT_DATE AS created_date
    FROM raw_status
    """)

    logging.info("Montpellier Station Statement data consolidated successfully.")


def consolidate_station_statement_nantes_toulouse_data(city="Nantes"):
    """
    Consolidates station statement data for Nantes or Toulouse by processing real-time
    bicycle data and inserting it into the CONSOLIDATE_STATION_STATEMENT table.

    Args:
        city (str): The city name for consolidation ('Nantes' or 'Toulouse').
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    city_code = NANTES_CITY_CODE

    # Add city-specific identifiers
    if city == "Toulouse":
        city_code = TOULOUSE_CITY_CODE

    json_path = f"data/raw_data/{today_date}/{city.lower()}_realtime_bicycle_data.json"

    # Insert data into database
    con.execute(f"""
    INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT
    SELECT 
        '{city_code}' || '-' || number AS station_id,
        available_bike_stands AS bicycle_docks_available,
        available_bikes AS bicycle_available,
        last_update AS last_statement_date,
        CURRENT_date AS created_date,
    FROM read_json(
        '{json_path}',
        columns = {{
            number: 'INTEGER',
            available_bike_stands: 'INTEGER',
            available_bikes: 'INTEGER',
            last_update: 'DATE',
    }})
    """)

    logging.info(f"{city} Station Statement data consolidated successfully.")


def consolidate_station_statement_data():
    """
    Consolidates station statement data for all cities: Paris, Nantes, Toulouse, and Montpellier.
    """
    consolidate_station_statement_paris_data()
    consolidate_station_statement_nantes_toulouse_data("Nantes")
    consolidate_station_statement_nantes_toulouse_data("Toulouse")
    consolidate_station_statement_montpellier_data()
