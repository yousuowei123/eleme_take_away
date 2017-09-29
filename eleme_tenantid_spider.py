# __author__ = "c tob"

import config
import requests
import pymysql
import json
import time
import random

# 代理服务器
proxyHost = "proxy.abuyun.com"
proxyPort = "9020"

# 代理隧道验证信息
proxyUser = config.proxyUser
proxyPass = config.proxyPass

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host": proxyHost,
    "port": proxyPort,
    "user": proxyUser,
    "pass": proxyPass,
    }

proxies = {
    "http": proxyMeta,
    "https": proxyMeta,
    }

session = requests.Session()  # 创建一个会话接口
requests.adapters.DEFAULT_RETRIES = 5
session.keep_alive = False  # 访问完后关闭会话


def create_table():
    try:
        print('开始创建id表')
        conn = pymysql.connect(host=config.ip, user=config.user, passwd=config.passwd, db=config.db, charset='utf8')
        cur = conn.cursor()
        cur.execute('set names utf8')

        sql = '''CREATE TABLE IF NOT EXISTS eleme_tenantid(
             id BIGINT PRIMARY  key  AUTO_INCREMENT,
             tenant_id varchar(20),
             business_name varchar(100),
             address varchar(200),
             unique(tenant_id), index(tenant_id)
             )'''
        cur.execute(sql)

    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()


def get_valid_longitude_latitude():
    try:
        conn = pymysql.connect(host=config.ip, user=config.user, passwd=config.passwd, db=config.db, charset='utf8')
        cur = conn.cursor()
        cur.execute('set names utf8')

        sql = "select latitude,longitude from eleme_validlnglat where province='广东省' and city='深圳市'"
        # sql += " limit 10"
        print('\033[31;1m开始获取经纬度列表\033[0m')
        cur.execute(sql)
        rows = cur.fetchall()
        id_list = []
        for r in rows:
            latitude = '{:.5f}'.format(float(r[0]))
            longitude = '{:.6f}'.format(float(r[1]))
            id_list.append([latitude, longitude])
        return id_list
    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()


def get_tenantid(latitude, longitude, offset):
    url = "https://mainsite-restapi.ele.me/shopping/restaurants"
    params = {'latitude': str(latitude),
              'longitude': str(longitude),
              'offset': 20*offset,
              'limit': 20,
              'extras[]': 'activities',
              'terminal': 'h5'}
    headers = {'Accept-Encoding': 'gzip, deflate, sdch, br',
               'Accept-Language': 'zh-CN,zh;q=0.8',
               'User-Agent': 'Mozilla/5.0'}

    r = session.get(url, params=params, headers=headers, proxies=proxies, timeout=60)
    print(r.url)
    t = random.randint(1, 3)
    time.sleep(t)

    return r.text


def get_id_info(html):
    if html:
        results = json.loads(html)
        items_list = []
        for item in results:
            items = {}
            items['tenant_id'] = item['id']
            items['business_name'] = item['name'].replace('（', '(').replace('）', ')')
            items['address'] = item['address']
            items_list.append(items)
        return items_list


def store_id(latitude, longitude):
    business_id = []
    i = 1
    while True:
        try:
            html = get_tenantid(latitude, longitude, i)
            id_list = get_id_info(html)
            # print(id_list)
            if id_list:
                business_id += id_list
                i += 1
            else:
                break
        except Exception as e:
            print(e)
            time.sleep(1)
            continue
    return business_id


def save_to_mysql(dic):
    try:
        if dic:
            conn = pymysql.connect(host=config.ip, user=config.user, passwd=config.passwd, db=config.db, charset='utf8')
            cur = conn.cursor()
            cur.execute('set names utf8')

            sql = "insert into eleme_tenantid (tenant_id, business_name, address) value ('{0}', '{1}', '{2}')".format(dic['tenant_id'], dic['business_name'], dic['address'])
            print('\033[31;1m{}\033[0m'.format(sql))
            cur.execute(sql)
            cur.connection.commit()
    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()


def insert_per_lnglat(dic_list):
    if dic_list:
        for item in dic_list:
            try:
                save_to_mysql(item)
            except Exception as e:
                print(e)
                continue


def main():
    create_table()

    for lnglat in get_valid_longitude_latitude():
        try:
            dic_list = store_id(lnglat[0], lnglat[1])
            insert_per_lnglat(dic_list)
            print('\033[33;1m经纬度为{}的页面下载完成\033[0m'.format(lnglat))
        except Exception as e:
            print(e)
            continue

if __name__ == "__main__":
    main()
    # create_table()
    # dic_list = store_id(22.72000, 113.781400)
    # insert_per_lnglat(dic_list)
