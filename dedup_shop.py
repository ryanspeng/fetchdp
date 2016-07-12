#!/bin/env python

import sys
import json

sid = set([])
for l in sys.stdin:
    l = l.strip()
    x = json.loads(l)
    if x['id'] not in sid:
       sid.add(x['id'])
       print l
