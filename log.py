# __author__ = "c tob"
import logging
import time


def log_config(filename):
    logger = logging.getLogger()

    filename1 = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    logging.basicConfig(
                        level=logging.INFO,              # 定义输出到文件的log级别，
                        format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',    # 定义输出log的格式
                        datefmt='%Y-%m-%d %A %H:%M:%S',                                     # 时间
                        filename=filename1 + ' ' + filename +" error.log",                # log文件名
                        filemode='a')                        # 写入模式“w”或“a”
    console = logging.StreamHandler()                  # 定义console handler
    console.setLevel(logging.INFO)                     # 定义该handler级别
    formatter = logging.Formatter('%(asctime)s  %(filename)s : %(levelname)s  %(message)s')  #定义该handler格式
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)


def write_log(message):
    logging.info(message)


def write_shop_log(message):
    logging.info(message)

