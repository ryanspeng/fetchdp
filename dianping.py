#!/bin/env python
# -*- encoding: utf-8 -*-

import time
import urllib2
import traceback

from bs4 import BeautifulSoup

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
    headers = {
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:46.0) Gecko/20100101 Firefox/46.0',
              }
    req = urllib2.Request(url, headers=headers)
    print "req:", url
    f = urllib2.urlopen(req)
    code = f.getcode()
    body = f.read()
    f.close()
    print url, code
    return code, body

def get_shop_info(shop_id):
    url = "http://www.dianping.com/shop/%s" % shop_id
    code, body = fetch_url(url)
    if code == 404:
        return None
    elif code != 200:
        return False

    soup = BeautifulSoup(body, 'lxml')
    lng, lat = get_shop_center(soup)
    x = {'city': get_city, 'category': get_category, 'shop_name': get_shop_name,
         'shop_addr': get_shop_addr, 'shop_tel': get_shop_tel}
    info = dict([(k, f(soup)) for k, f in x.items()])
    info['lng'] = lng
    info['lat'] = lat
    return info

def get_ids(soup):
    for div in soup.find_all('div', class_='shop-list'):
        return [href.get('href')[6:] for href in div.find_all('a') if href.get('href', '').startswith('/shop/')]
    return []

def get_shop_ids(page=1):
    #2为北京，10为美食，如需其他信息需要调整这个参数
    url = "http://wap.dianping.com/shoplist/2/c/10/p%s" % page
    code, body = fetch_url(url)
    if code == 404:
        return None
    elif code != 200:
        return False

    soup = BeautifulSoup(body, 'lxml')
    ids = get_ids(soup)
    return ids

def get_shop(page=1):
    try:
        ids = get_shop_ids(page)
        if ids is None:
            print "Request shop list invaild"
            return
        elif not ids:
            print "Ignore"
            return

        for x in ids:
            info = get_shop_info(x)
            if info is None:
                print "Request shop info invaild"
                return
            elif not info:
                print "Ignore shop %s" % x
                continue
            print "INSERT INTO shop (id, city, category, shop_name, shop_addr, shop_tel, lng, lat) VALUES (%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (x, info['city'], info['category'], info['shop_name'], info['shop_addr'], info['shop_tel'], info['lng'], info['lat'])
            time.sleep(6)
    except:
        print traceback.format_exc()

get_shop(page=1)
