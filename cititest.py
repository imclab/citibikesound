#!/usr/bin/env python3

import json, sys

data = json.loads(sys.argv[1])

print json.dumps(data, indent=4)