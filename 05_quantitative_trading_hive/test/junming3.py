# coding=utf-8

import sys
from pyhive import hive
import json
import requests


class HiveUtil(object):
    def __init__(self, con_dict):
        retry_times = 3  # 重试3次
        for i in range(retry_times):
            try:
                self.db_connect = hive.Connection(host=con_dict.get('host'),
                                                  port=con_dict.get('port'),
                                                  username=con_dict.get('username'),
                                                  database=con_dict.get('database'))
                self.db_cursor = self.db_connect.cursor()

                break
            except Exception as e:
                continue

    def __enter__(self):
        return self.db_cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db_connect.commit()
        self.db_cursor.close()
        self.db_connect.close()


hive_dict = {
    'host': '10.10.40.143',
    'port': 10000,
    'username': 'hive',
    'database': 'ads_xcc'
}
dt = '20221020'

# --（1）每日爬虫数据量，组装提取数据量，清洗校验的数据量，校验正常数据量，校验异常数据量，入库的数据量
sql1 = """
        select
            original_flag,normal_flag,err_flag,normal_total_flag,err_total_flag
            from (select if(package_original_total = clean_normal_original_total + clean_err_original_total, 0,1)  original_flag,
                         if(package_normal_total = clean_normal_normal_total + clean_err_normal_total, 0, 1) normal_flag,
                         if(package_err_total = clean_normal_err_total + clean_err_err_total, 0, 1)  err_flag,
                         if(clean_normal_total =clean_normal_original_total + clean_normal_normal_total + clean_normal_err_total, 0, 1)  normal_total_flag,
                         if(clean_err_total = clean_err_original_total + clean_err_normal_total + clean_err_err_total, 0,1) err_total_flag
                  from ads_xcc.ads_xcc_ware_detail_etl_total_incr_d
                  where dt = {0}) a
        where original_flag=1 or normal_flag=1 or err_flag=1 or normal_total_flag=1 or err_total_flag=1
""".format(dt)

key_tuple1 = ("original_flag", "normal_flag", "err_flag", "normal_total_flag", "err_total_flag")

sql2 = """
    select id from dwd_xcc.dwd_xcc_ware_detail_clean_and_check_after_err_incr_d where dt={0} and err_flag like '%组装规则有误%'
""".format(dt)

sql3 = """
    select id from dwd_xcc.dwd_xcc_ware_detail_etl_check_field where dt='{0}'
""".format(dt)


def getRes(data1, data2, data3):
    res = '\n' + '监控1: 每日爬虫数据量，组装提取数据量，清洗校验的数据量，校验正常数据量，校验异常数据量，入库的数据量' \
                 '\n' + 'original_flag: ' + str(data1.get('original_flag')) + \
          '\n' + 'normal_flag: ' + str(data1.get('normal_flag')) + \
          '\n' + 'err_flag: ' + str(data1.get('err_flag')) + \
          '\n' + 'normal_total_flag: ' + str(data1.get('normal_total_flag')) + \
          '\n' + 'err_total_flag: ' + str(data1.get('err_total_flag')) + \
          '\n' + '监控2：有all_json但是没有etl_json，要么组装规则有问题' \
                 '\n' + '数量: ' + str(len(data2)) + \
          '\n' + '监控3：重点字段监控：最大温度和最小温度，为空的需要查询原始字段是否为空，如果不为空，则证明有bug' + \
          '\n' + '数量：' + str(len(data3)) + '\n'

    return res


with HiveUtil(hive_dict) as db:
    db.execute(sql1)
    data_list1 = db.fetchall()

    db.execute(sql2)
    data_list2 = db.fetchall()
    # 这里数据较多 显示数据量

    db.execute(sql3)
    data_list3 = db.fetchall()

    content = getRes(dict(zip(key_tuple1, data_list1[0] if len(data_list1) > 0 else {})),
                     data_list2 if len(data_list2) > 0 else [],
                     data_list3 if len(data_list2) > 0 else [])

    headers = {
        'Content-type': 'application/json;charset=utf-8',
    }

    body = "{\"msgtype\": \"text\",\"text\": {\"content\": \"" + str(content) + "\"}}"
    print(body)
    # res = requests.post(
    #     "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=655a93ce-296e-477d-bf91-0b1c471e3db9",
    #     data=body.encode(), headers=headers)
    # print(res)
    # print(res.text)
