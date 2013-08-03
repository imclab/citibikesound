#!/usr/bin/env python3

"""
a tone when a bike is checked out
a tone when a bike is checked in
tones vary per stations, via some map
313 current stations; 626 sounds

at each minute, we have a dict, station_id to tone
{72: -1, 72: 0, 72: 1}

"""

from housepy import config, log
import json, sys, model

filename = sys.argv[1]
log.info("Parsing %s" % filename)

try:
    with open(filename) as f:
        data = json.loads(f.read())
except Exception as e:
    log.error(log.exc(e))
    exit()

stations = model.fetch_stations()
for station in data['results']:
    print(json.dumps(station, indent=4))
    if station['id'] not in stations:
        model.insert_station(station['id'], station['longitude'], station['latitude'], station['availableBikes'], station['availableDocks'])
        continue
