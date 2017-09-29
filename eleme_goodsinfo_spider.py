# __author__ = "c tob"
import requests
import json
import pymysql
import config
import log
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
sess = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=10)
sess.mount('https://', adapter)


def create_table():
    conn = pymysql.connect(config.ip, user=config.user, passwd=config.passwd, db=config.db, charset="utf8")
    cur = conn.cursor()
    cur.execute("set names utf8")
    cur.execute('CREATE TABLE IF NOT EXISTS eleme_new_goods_info \
               (id BIGINT PRIMARY KEY AUTO_INCREMENT, food_id VARCHAR(30),business_id VARCHAR(30),price VARCHAR(20),\
               name VARCHAR(150),image_path VARCHAR(150),month_saled int(10) DEFAULT 0,\
               description VARCHAR(300),specs_value VARCHAR(300),specs_features VARCHAR(1500),md5 VARCHAR(50),\
               UNIQUE(food_id), INDEX(food_id),INDEX(business_id), INDEX(name))')
    cur.close()
    conn.close()


def get_html_text(tenant_id):
    try:
        url = 'https://mainsite-restapi.ele.me/pizza/v1/restaurants/{}/menu'.format(tenant_id)
        header = {'user-agent': 'Mozilla/5.0'}
        isproxy = config.isproxy
        if isproxy:
            r = sess.get(url, headers=header, proxies=proxies, timeout=20)
        # else:
        #     r = sess.get(url, headers=header, timeout=20)

        print('\033[31;1m正在爬取商家ID{}的商品信息\033[0m>>>{}'.format(tenant_id, url))
        t = random.randint(1, 7)
        time.sleep(t)
        return r.text

    except Exception as e:
        print('\033[31;1m获取网页出错>>>\033[0m{}'.format(tenant_id), e)
        log.write_log('\033[31;1m获取网页出错>>>\033[0m{}'.format(tenant_id))


def save_success_id(tenant_id):
    '''
    把已经抓取的url保存到txt文件中
    :param url:
    :return:
    '''
    with open('goods_success.txt', 'a', encoding='utf-8') as f:
        f.write(str(tenant_id) + '\n')
        f.close()


def save_goods_fail_id(items):
    with open('goods_fail.txt', 'a', encoding='utf-8') as f:
        f.write(str(items) + '\n')
        f.close()


def get_goods_info(html):
    try:
        results = json.loads(html)
        items_list = []
        for contents in results:
            if contents:
                for item in contents['foods']:
                    items = {}
                    items['food_id'] = item['item_id']
                    items['business_id'] = item['restaurant_id']
                    items['price'] = item['specfoods'][0]['price']
                    # original_price = item['specfoods'][0]['original_price']
                    # if original_price:
                    #     items['original_price'] = original_price
                    # else:
                    #     items['original_price'] = ''

                    items['name'] = item['name'].replace('（', '(').replace('）', ')')
                    items['image_path'] = item['image_path']
                    items['month_saled'] = item['month_sales']
                    items['description'] = item['description'].replace('%', '分之百')
                    # items['activity_image_text'] = contents['description']

                    if item['attrs']:  # 获取特殊规格属性的食品
                        content_list = []
                        for each in item['attrs']:
                            content_dict = {}
                            name = each['name']
                            values = each['values']
                            content_dict[name] = values
                            content_list.append(content_dict)
                        items["specs_value"] = json.dumps(content_list, ensure_ascii=False)
                    else:
                        items["specs_value"] = ""

                    if item['specfoods']:  # 获取有饮料规格的食品
                        specs_features = []
                        for each in item['specfoods']:
                            if each['specs']:
                                    each_dict = {"name": each['name'], "price": (each['price']),
                                              "specs_value": each['specs'][0]['value']}
                                    specs_features.append(each_dict)
                            else:
                                continue
                        if specs_features:
                            items['specs_features'] = json.dumps(specs_features, ensure_ascii=False)  # dump
                        else:
                            items['specs_features'] = ""
                    else:
                        items['specs_features'] = ""

                    # items['category_id'] = item['category_id']
                    # items['food_appraise'] = item['tips']
                    # items['sort'] = contents['name']

                    string = str(items['food_id']) + ',' + str(items['business_id']) + ',' + str(
                        items['price']) + ',' + str(items['name']) + ',' + str(items['image_path']) + ',' + str(
                        items['month_saled']) + ',' + str(items['description']) + ',' + str(
                        items['specs_value']) + ',' + str(items['specs_features'])
                    md5 = get_md5_value(string)
                    items['md5'] = md5
                    items_list.append(items)
            else:
                continue
        return items_list
    except Exception as e:
        print('获取商品信息出错', e)


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
    try:
        if items:
            try:
                ROWstr = ''  # 行字段
                COLstr = ''  # 列字段
                conn = pymysql.connect(config.ip, user=config.user, passwd=config.passwd, db=config.db, charset='utf8')
                cur = conn.cursor()
                cur.execute("set names utf8")
                for key in items.keys():
                    COLstr = (COLstr + '"%s"' + ',') % (key)
                    ROWstr = (ROWstr + "'%s'" + ",") % (items[key])
                COLstr = COLstr.replace("\"", "")
                cur.execute("INSERT INTO %s(%s) VALUES (%s)" % ("eleme_new_goods_info", COLstr[:-1], ROWstr[:-1]))
                cur.connection.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print('保存到mysql出错', e)
                log.write_log('保存到mysql出错{}'.format(items))
                save_goods_fail_id(items)
        else:
            pass

    except Exception as e:
        print('保存到mysql失败', e)
        log.write_log('保存到mysql失败{}'.format(e))


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
              " order by tenantid asc "  # + "limit 1,20"
        cur.execute(sql)
        for tenant_id in cur.fetchall():
            yield tenant_id

    except Exception as e:
        print('获取失败', e)
        log.write_log('获取id失败')

    finally:
        cur.close()
        conn.close()


def run(tenant_id):
    '''
    构造多线程调用函数
    :param tenant_id:
    :return:
    '''
    html = get_html_text(tenant_id)
    results = get_goods_info(html)
    if results:
        for items in results:
            try:
                save_to_mysql(items)
            except:
                continue
    else:
        pass
    print('\033[32;1m店铺ID{}的商品信息抓取完成\033[0m'.format(tenant_id))
    save_success_id(tenant_id)


def main():
    log.log_config('eleme_new_goods_info')
    create_table()
    file = open('goods_success.txt', 'r')
    success_id_list = []
    for line in file:
        line = int(line.strip())
        success_id_list.append(line)

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

    # shop_id_list = []    # 多线程爬虫
    # for tenant_id in get_tenant_id():
    #     shop_id_list.append(tenant_id[0])

    pool = ThreadPool(2)
    pool.map(run, shop_id_list)
    pool.close()
    pool.join()
    file.close()

    # for tenant_id in get_tenant_id():  # 单线程爬取
    #     try:
    #         html = get_html_text(tenant_id[0])
    #         for items in get_goods_info(html):
    #             save_to_mysql(items)
    #     except Exception as e:
    #         print('获取id异常', e)
    #         log.write_log(e)
    #         continue

if __name__ == '__main__':
    main()
    # run(920216)
