#!/bin/env python

import json
import redis

def load(conn):
    for l in file('shops'):
        l = l.strip()
        info = json.loads(l)
        shop_id = info['id']
        conn.execute_command('GEOADD', 'shop_geo', info['lng'], info['lat'], info['id'])
        conn.hset('shop_info', info['id'], l)
        conn.execute()

if __name__ == '__main__':
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
    conn = redis.Redis(connection_pool=pool)
    pipeline = conn.pipeline()

    load(pipeline)
