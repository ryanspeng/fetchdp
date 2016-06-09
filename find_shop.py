#!/bin/env python
# -*- encoding: utf-8 -*-

import json
import redis
import traceback
import tornado.ioloop
import tornado.web

class GeoHandler(tornado.web.RequestHandler):
    def initialize(self, conn):
        self.conn = conn

    def get(self):
        try:
            self.set_header("Content-Type", "text/json")

            lng = self.get_argument('lng', None)
            lat = self.get_argument('lat', None)
            radius = self.get_argument('r', 500)
            if lng is None or lat is None:
                self.write(json.dumps({'code': 1, 'err_msg': "请求参数错误"}))

            print "GEORADIUS shop_geo %s %s %s m WITHDIST COUNT 20 ASC" % (lng, lat, radius)
            result = self.conn.execute_command("GEORADIUS shop_geo %s %s %s m WITHDIST COUNT 20 ASC" % (lng, lat, radius))

            ids = [x[0] for x in result]
            infos = self.conn.hmget('shop_info', ids)
            shop_infos = []
            for x, s in zip(result, infos):
                info = json.loads(s)
                info['avg_score'] = sum(info['score'])/len(info['score'])
                info['dist'] = x[1]
                shop_infos.append(info)
            self.write(json.dumps(shop_infos))
        except:
            self.write(json.dumps({'code': 1, 'err_msg': "请求异常"}))

def init_redis():
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
    conn = redis.Redis(connection_pool=pool)
    return conn

if __name__ == "__main__":
    conn = init_redis()
    application = tornado.web.Application([
        (r"/geo", GeoHandler, {'conn': conn}),
    ])

    application.listen(8080)
    tornado.ioloop.IOLoop.instance().start()
