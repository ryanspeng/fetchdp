#!/bin/env python

import json

ids = set([])
f = file('shop_ids', 'w')
for l in file('finall'):
    x = json.loads(l.strip())
    if x['id'] in ids:
        print "Dup:", x['id']
    else:
        ids.add(x['id'])
        print >>f, x['id']
f.close()
