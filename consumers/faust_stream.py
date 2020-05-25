"""Defines trends calculations for stations"""
import logging

import faust
from dataclasses import asdict, dataclass

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

        
# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("com.udacity.cta.stations", value_type=Station)
# Define the output Kafka Topic
out_topic = app.topic("com.udacity.cta.transformed-stations", partitions=1)
# Define a Faust Table
table = app.Table(
   "transformedStations",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


#
#
# transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def stationevent(stations):
    #
    #  Group By station_id
    #
  
    async for se in stations:

        if se.red:
            line = 'red'
        elif se.green:
            line='green'
        elif se.blue:
            line='blue'
        else:
            line='no-color'
    
        transformed_station = TransformedStation(se.station_id, se.station_name, se.order, line)
        
        table[se.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
