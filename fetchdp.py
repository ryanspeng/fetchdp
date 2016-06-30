#!/bin/env python
# -*- encoding: utf-8 -*-

import os, os.path
import sys
import math
import time
import json
import socket
import urllib2
import traceback
import random
import logging
import logging.handlers

from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf-8')

PAGE_NUM = 15.0
categorys = {'g25842': 60, 'g2784': 70, 'g979': 80, 'g5672': 35, 'g3243': 10, 'g2884': 80, 'g246': 10, 'g131': 20, 'g25412': 55, 'g26119': 80, 'g159': 50, 'g158': 50, 'g157': 50, 'g156': 30, 'g155': 45, 'g154': 45, 'g153': 45, 'g152': 45, 'g151': 45, 'g248': 10, 'g6709': 45, 'g6708': 45, 'g6702': 45, 'g6701': 45, 'g6700': 50, 'g6706': 45, 'g2979': 80, 'g6828': 90, 'g33763': 65, 'g2882': 75, 'g20041': 25, 'g6693': 60, 'g6826': 90, 'g6710': 45, 'g33764': 65, 'g6712': 45, 'g6713': 45, 'g162': 55, 'g163': 55, 'g161': 70, 'g166': 55, 'g167': 55, 'g164': 55, 'g165': 55, 'g2874': 75, 'g2877': 75, 'g2876': 75, 'g2978': 80, 'g32705': 90, 'g2873': 75, 'g25410': 55, 'g2976': 80, 'g2872': 75, 'g2878': 75, 'g27769': 70, 'g27761': 70, 'g27763': 70, 'g27762': 70, 'g25462': 80, 'g27767': 70, 'g175': 65, 'g174': 60, 'g177': 65, 'g32704': 90, 'g171': 60, 'g173': 60, 'g172': 60, 'g179': 75, 'g178': 65, 'g3082': 80, 'g20026': 65, 'g493': 50, 'g182': 85, 'g260': 75, 'g980': 80, 'g25148': 95, 'g2901': 35, 'g2902': 35, 'g3014': 40, 'g612': 85, 'g3016': 40, 'g3018': 40, 'g150': 45, 'g4607': 80, 'g101': 10, 'g102': 10, 'g103': 10, 'g104': 10, 'g105': 10, 'g106': 10, 'g108': 10, 'g109': 10, 'g33803': 70, 'g33808': 70, 'g188': 70, 'g189': 70, 'g136': 25, 'g2790': 50, 'g33761': 50, 'g33762': 80, 'g183': 50, 'g184': 20, 'g185': 55, 'g186': 55, 'g187': 20, 'g27814': 70, 'g2714': 20, 'g192': 55, 'g20009': 70, 'g149': 45, 'g113': 10, 'g112': 10, 'g111': 10, 'g110': 10, 'g117': 10, 'g116': 10, 'g115': 10, 'g114': 10, 'g197': 80, 'g119': 20, 'g118': 10, 'g835': 80, 'g836': 80, 'g2926': 35, 'g26117': 80, 'g176': 65, 'g32732': 30, 'g2929': 80, 'g2928': 80, 'g147': 45, 'g3063': 80, 'g193': 70, 'g311': 10, 'g191': 55, 'g2840': 35, 'g195': 80, 'g259': 65, 'g20045': 35, 'g20038': 35, 'g20039': 30, 'g3020': 60, 'g3022': 60, 'g26490': 30, 'g26491': 80, 'g236': 65, 'g237': 80, 'g235': 20, 'g128': 20, 'g129': 20, 'g126': 20, 'g127': 20, 'g124': 20, 'g125': 20, 'g122': 20, 'g123': 50, 'g120': 20, 'g20040': 30, 'g2930': 80, 'g32722': 75, 'g2932': 80, 'g2934': 80, 'g6858': 60, 'g2838': 35, 'g3024': 60, 'g26085': 20, 'g2834': 35, 'g2836': 35, 'g180': 65, 'g2572': 50, 'g181': 80, 'g26483': 10, 'g26481': 10, 'g6694': 30, 'g32696': 20, 'g32702': 90, 'g2754': 30, 'g130': 20, 'g133': 30, 'g132': 10, 'g135': 15, 'g134': 30, 'g137': 30, 'g20042': 30, 'g26465': 80, 'g25475': 90, 'g26466': 80, 'g2914': 85, 'g2828': 65, 'g2827': 30, 'g2912': 85, 'g33792': 70, 'g33797': 70, 'g6844': 55, 'g2898': 50, 'g2894': 15, 'g258': 70, 'g25147': 95, 'g257': 85, 'g148': 45, 'g251': 10, 'g144': 30, 'g145': 45, 'g146': 45, 'g121': 20, 'g140': 30, 'g141': 30, 'g142': 30, 'g26101': 20, 'g32745': 35, 'g32744': 35, 'g508': 10, 'g32742': 80, 'g32749': 35, 'g1783': 10, 'g6714': 60, 'g27852': 35, 'g6823': 80}
regions = ['r14', 'r17', 'r15', 'r16', 'r20', 'r5952', 'r5950', 'r5951', 'r328', 'r9158', 'r27615', 'r9157', 'r27614', 'r27616', 'c434', 'c435']
sub_regions = {
               'r14': ['r2580', 'r1471', 'r2578', 'r1466', 'r1470', 'r1469', 'r2078', 'r2579', 'r2871', 'r1467', 'r1465', 'r2583', 'r22998', 'r1472', 'r2584', 'r1473', 'r22997', 'r1474', 'r1468', 'r23019', 'r2870', 'r2581', 'r2586', 'r23003', 'r23010', 'r23002', 'r2585', 'r7509', 'r23015', 'r23001', 'r22996', 'r23017', 'r23005', 'r12015', 'r23006', 'r12013', 'r22999', 'r23020', 'r23013', 'r23009', 'r23014', 'r23000', 'r23007', 'r23012', 'r23016', 'r23004', 'r23018', 'r12012', 'r23011', 'r23008', 'r14'],
               'r17': ['r1489', 'r1488', 'r1493', 'r2588', 'r1490', 'r1494', 'r1996', 'r1491', 'r1495', 'r2872', 'r2589', 'r2587', 'r1492', 'r1496', 'r1497', 'r1498', 'r23034', 'r7510', 'r23033', 'r23032', 'r23029', 'r23030', 'r23031', 'r23988', 'r23035', 'r23989', 'r17'],
               'r15': ['r1475', 'r1504', 'r2066', 'r1503', 'r23023', 'r23024', 'r1478', 'r1477', 'r2591', 'r1479', 'r1476', 'r2590', 'r1505', 'r23022', 'r2875', 'r23021', 'r2874', 'r23026', 'r23025', 'r15'],
               'r16': ['r1481', 'r1484', 'r2595', 'r1486', 'r1482', 'r1500', 'r1483', 'r1485', 'r2593', 'r2594', 'r1501', 'r1499', 'r2597', 'r1994', 'r2596', 'r2873', 'r2876', 'r23027', 'r23028', 'r16'],
               'r20': ['r1507', 'r1508', 'r1995', 'r2877', 'r23039', 'r2878', 'r2880', 'r7041', 'r7040', 'r7506', 'r2881', 'r2879', 'r2592', 'r1509', 'r7507', 'r23036', 'r7508', 'r23040', 'r23038', 'r25600', 'r23037', 'r20'],
               'r5952': ['r7043', 'r5959', 'r5961', 'r5960', 'r5952'],
               'r5950': ['r5953', 'r5955', 'r5954', 'r7042', 'r23043', 'r23044', 'r23042', 'r5950'],
               'r5951': ['r5957', 'r5958', 'r23045', 'r7521', 'r5956', 'r25907', 'r23990', 'r64881', 'r64883', 'r64882', 'r5951'],
               'r328': ['r1926', 'r1924', 'r1923', 'r1927', 'r2882', 'r328'],
               'r9158': ['r23041', 'r64877', 'r12016', 'r64878', 'r64880', 'r64879', 'r9158'],
               'r27615': ['r64866', 'r64947', 'r64858', 'r64859', 'r64860', 'r64865', 'r64862', 'r64861', 'r64863', 'r64864', 'r27615'],
               'r9157': ['r12011', 'r30781', 'r67342', 'r67346', 'r67349', 'r67350', 'r67374', 'r67376', 'r67384', 'r9157'],
               'c434': ['r27617', 'r65433', 'r65432', 'r65435', 'r65434', 'r65437', 'r65436', 'r65439', 'r65431', 'r65430', 'r65442', 'r65443', 'r65440', 'r65441', 'r65446', 'r65444', 'r65445', 'c434'],
               'c435': ['r27618', 'r65450', 'r65451', 'r65448', 'r65449', 'r65454', 'r65455', 'r65452', 'r65453', 'r65447', 'r65459', 'r65458', 'r65461', 'r65460', 'c435']
              }

def get_city(soup):
    for link in soup.find_all('a', class_='city'):
        return link.contents[0]
    return ''

def get_category(soup):
    for link in soup.find_all('a', class_='current-category'):
        return link.contents[0]
    return ''

def get_shop_name(soup):
    for h1 in soup.find_all('h1', class_='shop-name'):
        return h1.contents[0].strip()
    return ''

def get_shop_addr(soup):
    for div in soup.find_all('div', class_='address'):
        for span in div.find_all('span', class_='item'):
            return span.string.strip()
    return ''

def get_shop_tel(soup):
    for p in soup.find_all('p', class_='tel'):
        for span in p.find_all('span', class_='item'):
            return span.string.strip()
    return ''

def get_shop_num(soup):
    for span in soup.find_all(class_='num'):
        s = span.string.strip()
        if s.startswith('(') and s.endswith(')'):
            return int(s[1:-1])
    return 0

def get_shop_score(soup):
    for div in soup.find_all('div', class_='brief-info'):
        score = {}
        for span in div.find_all('span', class_='item'):
            s = span.string.strip()
            try:
                k, v = s.split('：')
                score[k] = float(v)
            except:
                pass
        return score
    return {}

def get_shop_center(soup):
    lng, lat = '', ''
    for scr in soup.find_all('script'):
        if scr.string and scr.string.find('apis.map.qq.com') > 0:
            i = scr.string.find('({')
            j = scr.string[i:].find('})')
            if i > 0 and j > 0:
                s = scr.string[i+2:i+j]
                for x in s.split(','):
                    if x.startswith('lng:'):
                        lng = x[4:]
                    elif x.startswith('lat:'):
                        lat = x[4:]
    return lng, lat

def fetch_url(url):
    try:
        headers = {
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Cache-Control': 'max-age=0',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:46.0) Gecko/20100101 Firefox/46.0',
                  }
        logging.getLogger().info("Request url: %s" % url)
        req = urllib2.Request(url, headers=headers)
        f = urllib2.urlopen(req)
        code = f.getcode()
        body = f.read()
        f.close()
        logging.getLogger().info("Request url: %s, response code: %s" % (url, code))
        return code, body
    except urllib2.HTTPError, e:
        logging.getLogger().info("Request url error: %s" % traceback.format_exc())
        return e.code, ''
    except:
        logging.getLogger().info("Request url error: %s" % traceback.format_exc())
        return 302, ''

def get_shop_info(shop_id):
    url = "http://www.dianping.com/shop/%s" % shop_id
    code, body = fetch_url(url)
    if code in (302, 404, 500, 502):
        return False
    elif code != 200:
        return None

    soup = BeautifulSoup(body, 'lxml')
    lng, lat = get_shop_center(soup)
    score_info = get_shop_score(soup)
    if score_info:
        logging.getLogger().info("shop %s score: %s" % (shop_id, ','.join(['%s|%s' % (k, v) for k, v in score_info.items()])))
    x = {'city': get_city, 'category': get_category, 'shop_name': get_shop_name,
         'shop_addr': get_shop_addr, 'shop_tel': get_shop_tel}
    info = dict([(k, f(soup)) for k, f in x.items()])
    info['lng'] = lng
    info['lat'] = lat
    info['score'] = score_info.values()
    return info

def get_ids(soup):
    num = get_shop_num(soup)
    ids = []
    for div in soup.find_all('div', class_='pic'):
        for href in div.find_all('a'):
            if href.get('href', '').startswith('/shop/'):
                ids.append(href.get('href')[6:])
    return num, ids

def write_result(info):
    f = file('result', 'a')
    print >>f, json.dumps(info)
    f.close()

def write_shop_id(sid):
    f = file('shop_id', 'a')
    print >>f, sid
    f.close()

def save_shop_info(sids, shop_id):
    info = get_shop_info(shop_id)
    if info and info.get('shop_name', ''):
        info['id'] = shop_id
        write_result(info)
        sids.add(shop_id)
        write_shop_id(shop_id)
    else:
        logging.getLogger().warning("Miss shop info (%s), result: %s" % (shop_id, str(info)))
    return info

def get_shops(sids, ids):
    try:
        for x in ids:
            if x in sids:
                logging.getLogger().info("Shop %s exist, ignore" % x)
                continue
            logging.getLogger().info("Request shop info, id: %s" % x)
            info = save_shop_info(sids, x)
            if info is None:
                logging.getLogger().error("Get shop info error (%s)" % x)
                return False
            elif not info:
                logging.getLogger().error("Get shop info fail (%s)" % x)
                continue
            t = random.randint(10, 20)
            logging.getLogger().info("Sleep %s seconds" % t)
            time.sleep(t)
        return True
    except:
        logging.getLogger().error("Get shops error: %s" % traceback.format_exc())
        return False

class Finder:
    def __init__(self, category):
        self.category = category
        self.mode = 1
        self.region_id = None
        self.sub_region_id = None
        self.page = 1
        self.error_num = 0

    def change_region(self):
        self.page = 1
        if self.region_id is not None:
            region = regions[self.region_id]
            if self.sub_region_id is not None:
                self.sub_region_id += 1
                if len(sub_regions[region]) <= self.sub_region_id:
                    self.sub_region_id = None
                    self.region_id += 1
            else:
                self.region_id += 1
        logging.getLogger().info("id:%s sub_id:%s page:%s" % (str(self.region_id), str(self.sub_region_id), self.page))

    def get_shop_ids(self, url, retry=False):
        code, body = fetch_url(url)
        if code == 200:
            self.error_num = 0
        if self.error_num > 10:
            logging.getLogger().info("Request fail num: %s" % self.error_num)
            self.change_region()
            self.page = 0
            return 0, []
        if code not in (200, 404, 403):
            time.sleep(8)
            code, body = fetch_url(url)
            self.error_num += 1
        if code in (302, 404, 500, 502):
            self.error_num += 1
            return 0, []
        elif code != 200:
            return 0, None

        soup = BeautifulSoup(body, 'lxml')
        num, ids = get_ids(soup)
        if not retry and num == 0:
            return self.get_shop_ids(url, True)
        logging.getLogger().info("Request shop ids, response code: %s, num: %s" % (code, num))
        return num, ids

    def check(self):
        if self.region_id is not None:
            if self.region_id >= len(regions):
                return None, False

        if self.mode == 1:
            url = "http://www.dianping.com/search/category/2/%s/%sp%s" % (categorys.get(self.category, 10), self.category, self.page)
            num, ids = self.get_shop_ids(url)
            if num > 50 * PAGE_NUM:
                self.mode = 2
                self.region_id = 0
                return [], True
            elif int(math.ceil(num/PAGE_NUM)) >= self.page:
                self.page += 1
                return ids, (int(math.ceil(num/PAGE_NUM)) >= self.page)
            else:
                return None, False
        elif self.region_id is not None and self.sub_region_id is not None:
            region = regions[self.region_id]
            if region in sub_regions:
                url = "http://www.dianping.com/search/category/2/%s/%s%sp%s" % (categorys.get(self.category, 10), self.category, sub_regions[region][self.sub_region_id], self.page)
            else:
                url = "http://www.dianping.com/search/category/2/%s/%s%sp%s" % (categorys.get(self.category, 10), self.category, region, self.page)
            num, ids = self.get_shop_ids(url)
            if (num > self.page * PAGE_NUM and self.page < 50) or num == 0:
                self.page += 1
            else:
                self.change_region()
            return ids, True
        elif self.region_id is not None:
            url = "http://www.dianping.com/search/category/2/%s/%s%sp%s" % (categorys.get(self.category, 10), self.category, regions[self.region_id], self.page)
            num, ids = self.get_shop_ids(url)
            if num > 50 * PAGE_NUM and regions[self.region_id] in sub_regions:
                self.sub_region_id = 0
                return [], True
            elif (num > self.page * PAGE_NUM) or num == 0:
                self.page += 1
            else:
                self.change_region()
            return ids, True

def load_shop_ids():
    sids = set()
    if os.path.isfile('shop_id'):
        for l in file('shop_id'):
            sids.add(l.strip())
    return sids

def set_logging():
    log_handler = logging.FileHandler('service.log')
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(process)d %(funcName)s:%(lineno)d %(message)s')
    log_handler.setFormatter(log_formatter)

    logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

if __name__ == '__main__':
    socket.setdefaulttimeout(60)
    set_logging()

    sids = load_shop_ids()

    if sys.argv[1][0] == 'g':
        category = sys.argv[1]
        finder = Finder(sys.argv[1])
        while True:
            ids, flag = finder.check()
            logging.getLogger().info("continue: %s, ids: %s" % (str(flag), str(ids)))
            time.sleep(5)
            if ids is None:
                break
            elif not get_shops(sids, ids):
                break

            if not flag:
                break
    else:
        if sys.argv[1] in sids:
            logging.getLogger().info("Shop %s exist, ignore" % sys.argv[2])
        else:
            save_shop_info(sids, sys.argv[1])

