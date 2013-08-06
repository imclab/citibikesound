#!/usr/bin/env python3

import model
from housepy import config, log, net, server, geo, util

class Home(server.Handler):

    def get(self, page):
        if page == "stations":
            stations = model.fetch_stations()
            s = {}
            for station_id, station in stations.items():
                s[station_id] = geo.project((station['lon'], station['lat']))
            max_x = s[max(s, key=lambda d: s[d][0])][0]
            min_x = s[min(s, key=lambda d: s[d][0])][0]
            max_y = s[max(s, key=lambda d: s[d][1])][1]
            min_y = s[min(s, key=lambda d: s[d][1])][1]
            for station_id, value in s.items():
                x, y = value
                s[station_id] = util.scale(x, min_x, max_x), util.scale(y, min_y, max_y)
            return self.json(s)
            

        return self.render("home.html")

handlers = [
    (r"/?([^/]*)", Home),
]    
server.start(handlers)

