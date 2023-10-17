import time
import json
import asyncio
import requests
from datetime import datetime

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

# The Eventstream connection string and name parameters
# These are made available to you when you create a Eventstream in the Fabric Portal
# We will show you how in the next section.
EVENTSTREAM_CONNECTION_STR = "Endpoint=sb://esehwestcxdjlpxjow0c7c70.servicebus.windows.net/;SharedAccessKeyName=key_ce1b3efb-d2fe-2a6a-8c5f-6453af29752b;SharedAccessKey=G9SV2jXv8JaDxv4KTxkTWGMOeXBIJlXlW+AEhBbAkkM="
EVENTSTREAM_NAME = "es_465a9325-7208-4f7b-b58a-f86380a89e88"

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
        return data
    return None
async def run():
    # Create a producer client to send messages to the EventStream.
    # Specify a connection string to your EventStream namespace and the eventstream name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTSTREAM_CONNECTION_STR, eventhub_name=EVENTSTREAM_NAME
    )
    # Specify the station ID
    stationID = "9445958"  # Sample Bremerton, WA [9445958]
    async with producer:
        while True:
            # Create a batch.
            event_data_batch = await producer.create_batch()
            # Fetch current water level in feet
            station_data = fetch_water_level(stationID)
           
            if station_data is not None:
                # Fetch station location
                data = station_data["data"]
                if len(data) > 0:
                    water_level = data[0]["v"]
                station_latitude = station_data["metadata"]["lat"]
                station_longitude = station_data["metadata"]["lon"]
                # Create a data point with the time and value
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                # Generate the data structure
                water_point = {
                    "Current_time": current_time,
                    "Water Level Value": water_level,
                    "Station Latitude": station_latitude,
                    "Station Longitude": station_longitude,
                    "Station": "Water Station",
                }
                # Convert the curve data to JSON format
                json_water_data = json.dumps(water_point)
                event_data_batch.add(EventData(json_water_data))
                # Send the batch of events to the event hub.
                await producer.send_batch(event_data_batch)
            # Wait for 20secs before generating the next point
            time.sleep(20)

asyncio.run(run())