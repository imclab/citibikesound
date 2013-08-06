#!/usr/bin/env python3

from housepy import net

d = {'test': 2, 'gain': "high"}

result = net.urlencode(d)

print(result)

result = net.urldecode(result)

print(result)