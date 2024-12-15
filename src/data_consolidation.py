import json
from datetime import datetime, date

import duckdb
import pandas as pd
import re

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
    data = {}

    # Consolidation logic for Paris Bicycle data
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(
        lambda x: f"{PARIS_CITY_CODE}-{x}"
    )
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    paris_station_data_df = paris_raw_data_df[
        [
            "id",
            "stationcode",
            "name",
            "nom_arrondissement_communes",
            "code_insee_commune",
            "address",
            "coordonnees_geo.lon",
            "coordonnees_geo.lat",
            "is_installed",
            "created_date",
            "capacity",
        ]
    ]

    paris_station_data_df.rename(
        columns={
            "stationcode": "code",
            "name": "name",
            "coordonnees_geo.lon": "longitude",
            "coordonnees_geo.lat": "latitude",
            "is_installed": "status",
            "nom_arrondissement_communes": "city_name",
            "code_insee_commune": "city_code",
        },
        inplace=True,
    )

    con.execute(
        "INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;"
    )


# Function to remove the prefix and retrieve the text after the hyphen
def remove_prefix(text):
    """
    Removes the prefix from the station name and returns the part of the name after the hyphen.
    """
    match = re.search(r"[A-Za-z]", text)  # Search for the first alphabetical character
    if match:
        return text[match.start() :]  # Return the text starting from the first letter
    return text  # If no letter is found, return the text as is


def consolidate_montpellier_station():
    """
    Consolidates the Montpellier station data by reading both real-time bicycle status
    and station information data, merging them, and storing the result in the
    CONSOLIDATE_STATION table.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )
    data = {}

    # Consolidation logic for Montpellier Bicycle data (status)
    with open(
        f"data/raw_data/{today_date}/montpellier_realtime_bicycle_station_status_data.json"
    ) as fd:
        data = json.load(fd)
        data = data["data"]["stations"]

    montpellier_raw_status_data_df = pd.json_normalize(data)

    # Consolidation logic for Montpellier Bicycle data (information)
    with open(
        f"data/raw_data/{today_date}/montpellier_realtime_bicycle_station_information_data.json"
    ) as file:
        data = json.load(file)
        data = data["data"]["stations"]

    montpellier_raw_station_information_data_df = pd.json_normalize(data)

    # Merging status and station information data
    montpellier_raw_data_df = pd.merge(
        montpellier_raw_status_data_df,
        montpellier_raw_station_information_data_df,
        on="station_id",
    )

    montpellier_raw_data_df = montpellier_raw_data_df.astype({"is_installed": str})

    # Add required columns and clean up the data
    montpellier_raw_data_df["id"] = montpellier_raw_data_df["station_id"].apply(
        lambda x: f"{MONTPELLIER_CITY_CODE}-{x}"
    )
    montpellier_raw_data_df["address"] = None
    montpellier_raw_data_df["created_date"] = date.today()
    montpellier_raw_data_df["nom_arrondissement_communes"] = "Montpellier"
    montpellier_raw_data_df["code_insee_commune"] = "34172"
    montpellier_raw_data_df.loc[
        montpellier_raw_data_df["is_installed"] == "1", "is_installed"
    ] = "YES"
    montpellier_raw_data_df.loc[
        montpellier_raw_data_df["is_installed"] == "0", "is_installed"
    ] = "NO"

    montpellier_station_data_df = montpellier_raw_data_df[
        [
            "id",
            "station_id",
            "name",
            "nom_arrondissement_communes",
            "code_insee_commune",
            "address",
            "lon",
            "lat",
            "is_installed",
            "created_date",
            "capacity",
        ]
    ]

    montpellier_station_data_df.rename(
        columns={
            "station_id": "code",
            "name": "name",
            "lon": "longitude",
            "lat": "latitude",
            "is_installed": "status",
            "nom_arrondissement_communes": "city_name",
            "code_insee_commune": "city_code",
        },
        inplace=True,
    )

    con.execute(
        "INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM montpellier_station_data_df;"
    )


def consolidate_nantes_toulouse_station_data(city="nantes"):
    """
    Consolidates the station data for Nantes and Toulouse by reading the corresponding
    real-time bicycle data, processing it, and storing it in the CONSOLIDATE_STATION table.

    Args:
        city (str): The city name for consolidation ('nantes' or 'toulouse').
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )
    data = {}

    # Consolidation logic for Nantes and Toulouse Bicycle data
    with open(f"data/raw_data/{today_date}/{city}_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    city_raw_data_df = pd.json_normalize(data)
    city_raw_data_df = city_raw_data_df.astype({"number": str})

    # Handle Toulouse specific data
    if city == "toulouse":
        city_raw_data_df["id"] = city_raw_data_df["number"].apply(
            lambda x: f"{TOULOUSE_CITY_CODE}-{x}"
        )
        city_raw_data_df["nom_arrondissement_communes"] = "Toulouse"
        city_raw_data_df["code_insee_commune"] = "31555"

    # Handle Nantes specific data
    elif city == "nantes":
        city_raw_data_df["id"] = city_raw_data_df["number"].apply(
            lambda x: f"{NANTES_CITY_CODE}-{x}"
        )
        city_raw_data_df["nom_arrondissement_communes"] = "Nantes"
        city_raw_data_df["code_insee_commune"] = "44109"

    # Process common columns
    city_raw_data_df["created_date"] = date.today()
    city_raw_data_df["name"] = city_raw_data_df["name"].apply(remove_prefix)

    # Normalize station status
    city_raw_data_df.loc[city_raw_data_df["status"] == "OPEN", "status"] = "YES"
    city_raw_data_df.loc[city_raw_data_df["status"] == "CLOSED", "status"] = "NO"

    city_station_data_df = city_raw_data_df[
        [
            "id",
            "number",
            "name",
            "nom_arrondissement_communes",
            "code_insee_commune",
            "address",
            "position.lon",
            "position.lat",
            "status",
            "created_date",
            "bike_stands",
        ]
    ]

    city_station_data_df.rename(
        columns={
            "number": "code",
            "name": "name",
            "position.lon": "longitude",
            "position.lat": "latitude",
            "status": "status",
            "nom_arrondissement_communes": "city_name",
            "code_insee_commune": "city_code",
        },
        inplace=True,
    )

    con.execute(
        "INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM city_station_data_df;"
    )


def consolidate_station_data():
    """
    Consolidates station data for Paris, Nantes, Toulouse, and Montpellier by
    calling the respective consolidation functions.
    """
    consolidate_paris_station()
    consolidate_nantes_toulouse_station_data("nantes")
    consolidate_nantes_toulouse_station_data("toulouse")
    consolidate_montpellier_station()


def consolidate_city_data():
    """
    Consolidates city data by reading from the commune data JSON, processing it,
    and inserting it into the CONSOLIDATE_CITY table.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )
    data = {}

    with open(f"data/raw_data/{today_date}/commune_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    city_data_df = raw_data_df[["code", "nom", "population"]]
    city_data_df.rename(
        columns={"code": "id", "nom": "name", "population": "nb_inhabitants"},
        inplace=True,
    )
    city_data_df.drop_duplicates(inplace=True)

    city_data_df["created_date"] = date.today()
    print(city_data_df)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")


def consolidate_station_statement_paris_data():
    """
    Consolidates station statement data for Paris by processing real-time bicycle data
    and inserting it into the CONSOLIDATE_STATION_STATEMENT table.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    # Load data
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(
        lambda x: f"{PARIS_CITY_CODE}-{x}"
    )
    paris_raw_data_df["created_date"] = date.today()

    # Select and rename relevant columns
    paris_station_statement_data_df = paris_raw_data_df[
        [
            "station_id",
            "numdocksavailable",
            "numbikesavailable",
            "duedate",
            "created_date",
        ]
    ]
    paris_station_statement_data_df.rename(
        columns={
            "numdocksavailable": "bicycle_docks_available",
            "numbikesavailable": "bicycle_available",
            "duedate": "last_statement_date",
        },
        inplace=True,
    )

    # Insert data into database
    con.execute(
        "INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;"
    )


def consolidate_station_statement_montpellier_data():
    """
    Consolidates station statement data for Montpellier by processing real-time station
    status data and inserting it into the CONSOLIDATE_STATION_STATEMENT table.
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    # Load data
    with open(
        f"data/raw_data/{today_date}/montpellier_realtime_bicycle_station_status_data.json"
    ) as fd:
        data = json.load(fd)
        data = data["data"]["stations"]

    montpellier_raw_data_df = pd.json_normalize(data)
    montpellier_raw_data_df["station_id"] = montpellier_raw_data_df["station_id"].apply(
        lambda x: f"{MONTPELLIER_CITY_CODE}-{x}"
    )

    # Format timestamp
    montpellier_raw_data_df["last_reported"] = pd.to_datetime(
        montpellier_raw_data_df["last_reported"], unit="s", origin="unix"
    )
    montpellier_raw_data_df["duedate"] = montpellier_raw_data_df[
        "last_reported"
    ].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    montpellier_raw_data_df["created_date"] = date.today()

    # Select and rename relevant columns
    montpellier_station_statement_data_df = montpellier_raw_data_df[
        [
            "station_id",
            "num_docks_available",
            "num_bikes_available",
            "duedate",
            "created_date",
        ]
    ]
    montpellier_station_statement_data_df.rename(
        columns={
            "num_docks_available": "bicycle_docks_available",
            "num_bikes_available": "bicycle_available",
            "duedate": "last_statement_date",
        },
        inplace=True,
    )

    # Insert data into database
    con.execute(
        "INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM montpellier_station_statement_data_df;"
    )


def consolidate_station_statement_nantes_toulouse_data(city="nantes"):
    """
    Consolidates station statement data for Nantes or Toulouse by processing real-time
    bicycle data and inserting it into the CONSOLIDATE_STATION_STATEMENT table.

    Args:
        city (str): The city name for consolidation ('nantes' or 'toulouse').
    """
    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb", read_only=False
    )

    # Load data
    with open(f"data/raw_data/{today_date}/{city}_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    city_raw_data_df = pd.json_normalize(data)
    city_raw_data_df = city_raw_data_df.astype({"number": str})

    # Add city-specific identifiers
    if city == "toulouse":
        city_raw_data_df["station_id"] = city_raw_data_df["number"].apply(
            lambda x: f"{TOULOUSE_CITY_CODE}-{x}"
        )
    elif city == "nantes":
        city_raw_data_df["station_id"] = city_raw_data_df["number"].apply(
            lambda x: f"{NANTES_CITY_CODE}-{x}"
        )
    city_raw_data_df["created_date"] = date.today()

    # Select and rename relevant columns
    city_station_statement_data_df = city_raw_data_df[
        [
            "station_id",
            "available_bike_stands",
            "available_bikes",
            "last_update",
            "created_date",
        ]
    ]
    city_station_statement_data_df.rename(
        columns={
            "available_bike_stands": "bicycle_docks_available",
            "available_bikes": "bicycle_available",
            "last_update": "last_statement_date",
        },
        inplace=True,
    )

    # Insert data into database
    con.execute(
        "INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM city_station_statement_data_df;"
    )


def consolidate_station_statement_data():
    """
    Consolidates station statement data for all cities: Paris, Nantes, Toulouse, and Montpellier.
    """
    consolidate_station_statement_paris_data()
    consolidate_station_statement_nantes_toulouse_data("nantes")
    consolidate_station_statement_nantes_toulouse_data("toulouse")
    consolidate_station_statement_montpellier_data()
