#!/usr/bin/env python3

"""
a tone when a bike is checked out
a tone when a bike is checked in
tones vary per stations, via some map
313 current stations; 626 sounds

at each minute, we have a dict, station_id to tone
{72: -1, 73: 0, 74: 1}, etc

ok, so this can pull the endpoints directly


"""

from housepy import config, log, net
import json, sys, model, os, time

ENDPOINT = "http://appservices.citibikenyc.com/data2/stations.php"
LOOKUP = "http://api.geonames.org/findNearbyPostalCodesJSON"

def handle(t, data):
    stations = model.fetch_stations()
    events = {}
    for s in data['results']:
        try:
            if s['id'] not in stations:
                data = json.loads(net.read(LOOKUP, {'lat': s['latitude'], 'lng': s['longitude'], 'username': "h0use"}))
                zipcode = data['postalCodes'][0]['postalCode']
                model.insert_station(s['id'], s['longitude'], s['latitude'], zipcode, t, s['availableBikes'])
                continue
            station = stations[s['id']]
            if s['availableBikes'] != station['bikes'] and t > station['t']:
                json.dumps(s, indent=4)
                model.update_station(s['id'], t, s['availableBikes'])
                events[s['id']] = s['availableBikes'] - station['bikes']
        except Exception as e:
            log.error(log.exc(e))
    model.insert_beat(t, events)


if len(sys.argv) > 1:
    for filename in os.listdir("files"):
        log.info("Parsing %s" % filename)
        t = int(filename.split('.')[0])
        if model.check_t(t):
            log.info("skipping...")
            continue
        try:
            with open("files/" + filename) as f:
                data = json.loads(f.read())
        except Exception as e:
            log.error(log.exc(e))
            exit()
        handle(t, data)
else:
    log.info("Grabbing from endpoint...")
    try:
        t = time.time()
        data = json.loads(net.read(ENDPOINT))
    except Exception as e:
        log.error(log.exc(e))
        exit() 
    handle(t, data)

