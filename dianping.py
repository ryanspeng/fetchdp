#!/bin/env python
# -*- encoding: utf-8 -*-

import sys
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
    except:
        logging.getLogger().info("Request url error: %s" % traceback.format_exc())
        return 404, ''

def get_shop_info(shop_id):
    url = "http://www.dianping.com/shop/%s" % shop_id
    code, body = fetch_url(url)
    if code == 404:
        return None
    elif code != 200:
        return False

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
    for div in soup.find_all('div', class_='shop-list'):
        return [href.get('href')[6:] for href in div.find_all('a') if href.get('href', '').startswith('/shop/')]
    return []

def get_shop_ids(page=1):
    #2为北京，10为美食，如需其他信息需要调整这个参数
    url = "http://wap.dianping.com/shoplist/2/c/0/p%s" % page
    code, body = fetch_url(url)
    if code == 404:
        return None
    elif code != 200:
        return False

    soup = BeautifulSoup(body, 'lxml')
    ids = get_ids(soup)
    return ids

def write_result(info):
    f = file('result', 'a')
    print >>f, json.dumps(info)
    f.close()

def get_shop(page=1):
    try:
        logging.getLogger().info("Request shop list, page: %s" % page)
        ids = get_shop_ids(page)
        logging.getLogger().info("Get shop list: %s" % str(ids))
        if ids is None:
            logging.getLogger().error("Get shop list error (%s)" % page)
            return False
        elif not ids:
            logging.getLogger().error("Get shop list fail (%s)" % page)
            return True

        for x in ids:
            logging.getLogger().info("Request shop info, id: %s" % x)
            info = get_shop_info(x)
            if info is None:
                logging.getLogger().error("Get shop info error (%s)" % x)
                return False
            elif not info:
                logging.getLogger().error("Get shop info fail (%s)" % x)
                continue
            if info.get('shop_name', ''):
                info['id'] = x
                write_result(info)
            else:
                logging.getLogger().warning("Miss shop info (%s), result: %s" % (x, str(info)))
            t = random.randint(10, 20)
            logging.getLogger().info("Sleep %s seconds" % t)
            time.sleep(t)
        return True
    except:
        logging.getLogger().error("Get shops error: %s" % traceback.format_exc())
        return True

def set_logging():
    log_handler = logging.FileHandler('service.log')
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(process)d %(funcName)s:%(lineno)d %(message)s')
    log_handler.setFormatter(log_formatter)

    logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "%s start end" % sys.argv[0]
        sys.exit()
    start = int(sys.argv[1])
    end = int(sys.argv[2]) + 1

    socket.setdefaulttimeout(60)
    set_logging()

    for page in range(start, end):
        if not get_shop(page):
            break
        if page % 1000 == 0:
            time.sleep(600)
