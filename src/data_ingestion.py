import os
from datetime import datetime

import requests


def get_realtime_bicycle_data():
    """
    Fetches real-time bicycle data from predefined URLs and serializes it to corresponding files.
    """
    datasets = [
        (
            "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json",
            "paris_realtime_bicycle_data.json",
        ),
        (
            "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json?lang=fr&timezone=Europe%2FBerlin",
            "nantes_realtime_bicycle_data.json",
        ),
        (
            "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis",
            "toulouse_realtime_bicycle_data.json",
        ),
        (
            "https://montpellier-fr.fifteen.site/gbfs/en/station_status.json",
            "montpellier_realtime_bicycle_station_status_data.json",
        ),
        (
            "https://montpellier-fr.fifteen.site/gbfs/en/station_information.json",
            "montpellier_realtime_bicycle_station_information_data.json",
        ),
    ]

    for url, file_name in datasets:
        response = requests.request("GET", url)
        response.raise_for_status()  # Raise an error for bad status codes
        serialize_data(response.text, file_name)


def get_commune_data():
    """
    Fetches commune data from a predefined URL and serializes it to a file.
    """
    url = "https://geo.api.gouv.fr/communes"
    file_name = "commune_data.json"
    response = requests.request("GET", url)
    response.raise_for_status()
    serialize_data(response.text, file_name)


def serialize_data(raw_json: str, file_name: str):
    """
    Saves raw JSON data to a file, creating directories as needed.

    Args:
        raw_json (str): The raw JSON data to save.
        file_name (str): The name of the file to save the data.
    """
    today_date = datetime.now().strftime("%Y-%m-%d")
    directory = f"data/raw_data/{today_date}"

    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(f"{directory}/{file_name}", "w") as fd:
        fd.write(raw_json)
