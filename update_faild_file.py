# __author__ = "c tob"

import config
import requests
import json
import pymysql
import log
import geohash
import coord_transform
import hashlib
import time
import random
from multiprocessing.dummy import Pool as ThreadPool

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


def get_one_html(tenant_id):
    try:
        url = 'https://mainsite-restapi.ele.me/shopping/restaurant/{}?extras[]=activities&'.format(tenant_id)
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) \
                  Chrome/57.0.2987.133 Safari/537.36'}
        isproxy = config.isproxy
        if isproxy:
            r = session.get(url, headers=header, proxies=proxies, timeout=20)
        else:
            r = session.get(url, headers=header, timeout=20)
        # r = session.get(url, headers=header, timeout=60)
        print('\033[31;1m正在爬取店铺>>>\033[0m', url)
        if r.status_code == 200:
            t = random.randint(1, 7)
            time.sleep(t)
            return r.text
        elif r.status_code == 404:
            save_invaid_id(tenant_id)
            t = random.randint(1, 7)
            time.sleep(t)
            print('\033[31;1m这家id为{}店铺地址已经失效'.format(tenant_id))
        else:
            t = random.randint(1, 7)
            time.sleep(t)
            save_faild_id(tenant_id)
            print('又封ip了，先等一会再说')

    except Exception as e:
        print('获取网页失败', e)
        log.write_log('获取这id{}的店铺网页失败'.format(tenant_id))


def save_faild_id(tenant_id):
    with open('faild_id_6_13.txt', 'a', encoding='utf-8') as f:
        f.write(str(tenant_id) + '\n')
        f.close()


def save_invaid_id(tenant_id):
    with open('invaid_tenant_id.txt', 'a', encoding='utf-8') as f:
        f.write(str(tenant_id) + '\n')
        f.close()


def get_tenant_info(html):
    try:
        items = {}
        if html:
            results = json.loads(html)
            business_id = results['id']
            items['business_id'] = business_id
            name = results['name'].replace('（', '(').replace('）', ')')  # 替换括号
            items['name'] = name
            items['address'] = results['address'].replace('（', '(').replace('）', ')')
            items['telephone'] = results['phone']
            items['month_saled'] = results['recent_order_num']
            items['shop_announcement'] = results['promotion_info'].replace('%', '分之百')

            gd_lng = results['longitude']  # 高德经纬度转化为百度经纬度
            gd_lat = results['latitude']
            bd_latlng = coord_transform.gcj02_to_bd09(gd_lng, gd_lat)
            items['latitude'] = bd_latlng[1]
            items['longitude'] = bd_latlng[0]
            items['geohash'] = geohash.encode(items['latitude'], items['longitude'])

            items['avg_rating'] = results['rating']
            items['business_url'] = 'https://h5.ele.me/shop/#id=' + str(business_id)
            image_path = results['image_path']
            s1 = image_path[0]
            s2 = image_path[1:3]
            if image_path[-2] == 'i':
                items['photo_url'] = 'https://fuss10.elemecdn.com/' + s1 + '/' + s2 + '/' + image_path[3:] + '.gif'
            elif image_path[-2] == 'n':
                items['photo_url'] = 'https://fuss10.elemecdn.com/' + s1 + '/' + s2 + '/' + image_path[3:] + '.png'
            else:
                items['photo_url'] = 'https://fuss10.elemecdn.com/' + s1 + '/' + s2 + '/' + image_path[3:] + '.jpeg'

            items['float_minimum_order_amount'] = results['float_minimum_order_amount']
            items['float_delivery_fee'] = results['float_delivery_fee']

            if results['activities']:  # 优惠活动
                minus = ''
                for each in results['activities']:
                    description = each.get('description') + ';'
                    minus += description
                items['minus'] = minus
            else:
                items['minus'] = ''
            items['delivery_consume_time'] = results['order_lead_time']

            if len(results['opening_hours']) > 1:
                items['work_time'] = results['opening_hours'][0] + ',' + results['opening_hours'][1]
            else:
                items['work_time'] = results['opening_hours'][0]

        else:
            pass
        #
        # if html[1]:
        #     content = json.loads(html[1])
        #     items_list = []
        #     for i in range(0, len(content)):
        #         item = {}
        #         item['name'] = content[i]['name']
        #         items_list.append(item)
        #     # print(items_list)
        #     items['food_category'] = items_list
        # else:
        #     items['food_category'] = ''

        # 增加md5值
        string = str(items['business_id']) + ',' + str(items['name']) + ',' + str(
            items['address']) + ',' + str(items['telephone']) + ',' + str(
            items['month_saled']) + ',' + str(items['shop_announcement']) + ',' + str(
            items['latitude']) + ',' + str(items['longitude']) + ',' + str(
            items['geohash']) + ',' + str(items['avg_rating']) + ',' + str(
            items['business_url']) + ',' + str(items['photo_url']) + ',' + str(
            items['float_minimum_order_amount']) + ',' + str(items['float_delivery_fee']) + ',' + str(
            items['minus']) + ',' + str(items['delivery_consume_time']) + ',' + str(items['work_time'])
        md5 = get_md5_value(string)
        items['md5'] = md5

        return items
    except Exception as e:
        print('\033[33;1m商铺id{}的信息抓取失败\033[0m', e)
        log.write_log('商铺id的信息抓取失败{}'.format(e))


def get_md5_value(string):
    '''
    把字段名生成md5值确保数据库中不插入重复的值
    :param string:
    :return:
    '''
    my_md5 = hashlib.md5()
    string = string.encode('utf-8')
    my_md5.update(string)
    my_md5_digest = my_md5.hexdigest()
    return my_md5_digest


def save_to_mysql(items):
    if items:
        try:
            ROWstr = ''  # 行字段
            COLstr = ''  # 列字段
            conn = pymysql.connect(config.ip, user=config.user, passwd=config.passwd, db=config.db, charset='utf8')
            cur = conn.cursor()
            cur.execute("set names utf8")
            for key in items.keys():
                COLstr = (COLstr + '"%s"' + ',') % (key)
                ROWstr = (ROWstr + '"%s"' + ',') % (items[key])
            COLstr = COLstr.replace("\"", "")
            cur.execute("INSERT INTO %s(%s) VALUES (%s)" % ("eleme_tenantinfo", COLstr[:-1], ROWstr[:-1]))
            cur.connection.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print('保存到mysql出错', e)
            log.write_log('保存到mysql出错{},{}'.format(e, items))
    else:
        log.write_log('元素为空')


# def store(items, business_id):
#     conn = pymysql.connect(config.ip, user=config.user, passwd=config.passwd, db=config.db, charset='utf8')
#     cur = conn.cursor()
#     cur.execute("set names utf8")
#     repeat_sql = "select md5 from eleme_new_tenant_info where tenantid="+str(business_id)
#     cur.execute(repeat_sql)
#     if items['md5'] != cur.fetchall():
#     if len(cur.fetchall()) == 0:
#         cur.execute("INSERT INTO eleme_tenantid (tenantid,province,city,region,sign) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",0)",(tendant_id,province,city,region))
#         cur.connection.commit()
#     cur.close()
#     conn.close()

'''
def get_tenant_id():
    try:
        print('通过商品id表获取商品id开始')
        try:
            start_tenant_id = int(config.tenant_info_start_id)
        except:

            start_tenant_id = 0
        conn = pymysql.connect(config.ip, user=config.user, passwd=config.passwd, db=config.db, charset='utf8')
        cur = conn.cursor()
        cur.execute('set names utf8')
        sql = "SELECT tenantid FROM eleme_tenantid WHERE sign=0 and tenantid>=" + str(start_tenant_id) + \
              " order by tenantid asc "  # + "limit 1,50"
        cur.execute(sql)
        for tenant_id in cur.fetchall():
            yield tenant_id

    except Exception as e:
        print('获取失败', e)
        log.write_log('获取id失败')
    finally:
        cur.close()
        conn.close()
'''


def get_tenant_id():  # 获取文件的下载失败id列表
    faild_list = []
    f = open('tenant_fail_id.txt', 'r')
    for tenant_id in f:
        tenant_id = tenant_id.strip()
        faild_list.append(tenant_id)
    f.close()
    return faild_list


def run(tenant_id):
    '''
    构造多线程调用的函数
    :param tenant_id:
    :return:
    '''
    try:
        html = get_one_html(tenant_id)
        items = get_tenant_info(html)
        print(items)
        save_to_mysql(items)
        print('\033[32;1m商铺id{}的信息爬取成功\033[0m'.format(tenant_id))
    except:
        print('\033[33;1m运行这个{}店铺出错\033[0m'.format(tenant_id))
        log.write_log('运行这个{}店铺出错'.format(tenant_id))


def main():
    log.log_config('eleme_tenantinfo6_13')  # 记录日志文件
    # create_table()  # 创建数据库表
    # file = open('tenant_success_5_8.txt', 'r')  # 打开记录文件
    # success_id_list = []
    # for line in file:
    #     line = int(line.strip())
    #     success_id_list.append(line)
    # try:
    #     for tenant_id in get_tenant_id():  # 单线程爬取
    #         html = get_one_html(tenant_id[0])
    #         items = get_tenant_info(html)
    #         save_to_mysql(items)
    #         save_success_id(tenant_id)
    #         print('\033[32;1m商铺id{}的信息爬取成功\033[0m'.format(tenant_id))
    #
    # except:
    #     print('\033[33;1m运行这个{}店铺出错\033[0m'.format(tenant_id))
    #     log.write_log('运行这个{}店铺出错'.format(tenant_id))
    #

    '''
    shop_id_list = []  # 断点续爬
    for tenant_id in get_tenant_id():
        try:
            if tenant_id[0] not in success_id_list:
                shop_id_list.append(tenant_id[0])
            else:
                print('\033[31;1m这个店铺ID{}已经爬过了,你还要我怎样。\033[0m'.format(tenant_id[0]))
        except:
            log.write_log('ID{}已经存在'.format(tenant_id[0]))
            continue
    '''
    shop_id_list = get_tenant_id()
    pool = ThreadPool(2)   # 调用多线程的线程池
    pool.map(run, shop_id_list)
    pool.close()
    pool.join()
    # file.close()

if __name__ == "__main__":
    main()
    # run(19920)

