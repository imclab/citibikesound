#!/usr/bin/env python
"""
http://appservices.citibikenyc.com/data2/stations.php

http://appservices.citibikenyc.com/data2/stations.php?updateOnly=true

http://appservices.citibikenyc.com/v1/helmet/list

http://appservices.citibikenyc.com/v1/branch/list

TODO: make bucket


"""

import time, os
from housepy import config, log, net, s3

ENDPOINT = "http://appservices.citibikenyc.com/data2/stations.php"

filename = "%s.txt" % str(time.time()).split('.')[0]

log.info("----- grab attempt")

try:
    net.grab(ENDPOINT, filename)
except Exception as e:
    log.error(log.exc(e))
    exit()

log.info("got %s, pushing..." % filename)    

try:
    s3.upload(filename)
except Exception as e:
    log.error(log.exc(e))
    exit()

log.info("upload complete")

os.remove(filename)

