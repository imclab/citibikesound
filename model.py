#!/usr/bin/env python3

import sqlite3, json, time, sys, os
from housepy import config, log

connection = sqlite3.connect("data.db")
connection.row_factory = sqlite3.Row
db = connection.cursor()

def init():
    try:
        db.execute("CREATE TABLE IF NOT EXISTS stations (station_id INTEGER, lon REAL, lat REAL, t INT, bikes INT)")
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS stations_station_id ON stations(station_id)")
        db.execute("CREATE TABLE IF NOT EXISTS beats (t INT, events TEXT)")
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS beats_t ON beats(t)")
    except Exception as e:
        log.error(log.exc(e))
        return
    connection.commit()
init()

def check_t(t):
    db.execute("SELECT t FROM beats WHERE t>=?", (t,))
    result = db.fetchone()
    return result is not None

def fetch_stations():
    stations = {}
    db.execute("SELECT * FROM stations")
    # this should be dict comprehension!
    for station in db.fetchall():
        stations[station['station_id']] = dict(station)
    return stations

def insert_station(station_id, t, lon, lat, bikes):
    db.execute("INSERT INTO stations (station_id, lon, lat, t, bikes) VALUES (?, ?, ?, ?, ?)", (station_id, lon, lat, t, bikes))
    log.info("added station_id %s" % station_id)
    connection.commit()

def update_station(station_id, t, bikes):
    db.execute("UPDATE stations SET t=?, bikes=? WHERE station_id=?", (t, bikes, station_id))
    log.info("updated station_id %s" % station_id)
    connection.commit()

def insert_beat(t, events):
    num = len(events)
    if num == 0:
        return
    events = json.dumps(events)
    db.execute("INSERT INTO beats (t, events) VALUES (?, ?)", (t, events))
    log.info("added events for %s stations" % num)
    connection.commit()

