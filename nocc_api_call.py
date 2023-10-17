import requests
import time
import json
from datetime import datetime
# Fetch water level data from tidesandcurrents.noaa.gov
def fetch_water_level(station_id):
    url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    # Collect water level for one station
    payload = {
        "product": "water_level",
        "application": "Fabric EventStream",  # Replace with your application name
        "datum": "STND",
        "station": station_id,  # Set station ID
        "time_zone": "gmt",
        "units": "english",  # Use "english" for feet, "metric" for meters
        "format": "json",
        "date": "latest",
    }
    response = requests.get(url, params=payload)
    if response.status_code == 200:
        data = response.json()
        print("data is" + str(data))
        data_json = json.format(response.text)
        print("formated data is" + data_json)
        return data
    return None

fetch_water_level("9445958")