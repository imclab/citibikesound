#!/usr/bin/env python3

import sqlite3, json, time, sys, os
from housepy import config, log

connection = sqlite3.connect("data.db")
connection.row_factory = sqlite3.Row
db = connection.cursor()

def init():
    try:
        db.execute("CREATE TABLE IF NOT EXISTS stations (station_id INTEGER, lon REAL, lat REAL, bikes INT, docks INT)")
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS stations_station_id ON stations(station_id)")
    except Exception as e:
        log.error(log.exc(e))
        return
    connection.commit()
init()

def fetch_stations():
    stations = {}
    db.execute("SELECT * FROM stations")
    # this should be dict comprehension!
    for station in db.fetchall():
        stations[station['station_id']] = dict(station)
    return stations

def insert_station(station_id, lon, lat, bikes, docks):
    db.execute("INSERT INTO stations (station_id, lon, lat, bikes, docks) VALUES (?, ?, ?, ?, ?)", (station_id, lon, lat, bikes, docks))
    log.info("added station_id %s" % station_id)
    connection.commit()

