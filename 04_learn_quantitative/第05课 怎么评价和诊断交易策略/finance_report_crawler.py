#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""

"""
抓取财报数据，主要关注EPS、公告日期、报告期
"""

import json
import urllib3

from pymongo import UpdateOne

from database import DB_CONN
from stock_util import get_all_codes

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'


def crawl_finance_report():
    # 先获取所有的股票列表
    codes = get_all_codes()

    # 创建连接池
    conn_pool = urllib3.PoolManager()

    # 抓取的财务地址，scode为股票代码
    url = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?' \
          'type=YJBB20_YJBB&token=70f12f2f4f091e459a279469fe49eca5&st=reportdate&sr=-1' \
          '&filter=(scode={0})&p={page}&ps={pageSize}&js={"pages":(tp),"data":%20(x)}'

    # 循环抓取所有股票的财务信息
    for code in codes:
        # 替换股票代码，抓取该只股票的财务数据
        response = conn_pool.request('GET', url.replace('{0}', code))

        # 解析抓取结果
        result = json.loads(response.data.decode('UTF-8'))

        # 取出数据
        reports = result['data']

        # 更新数据库的请求列表
        update_requests = []
        # 循环处理所有报告数据
        for report in reports:
            doc = {
                # 报告期
                'report_date': report['reportdate'][0:10],
                # 公告日期
                'announced_date': report['latestnoticedate'][0:10],
                # 每股收益
                'eps': report['basiceps'],
                'code': code
            }

            # 将更新请求添加到列表中，更新时的查询条件为code、report_date，为了快速保存数据，需要增加索引
            # db.finance_report.createIndex({'code':1, 'report_date':1})
            update_requests.append(
                UpdateOne(
                    {'code': code, 'report_date': doc['report_date']},
                    # upsert=True保证了如果查不到数据，则插入一条新数据
                    {'$set': doc}, upsert=True))

        # 如果更新数据的请求列表不为空，则写入数据库
        if len(update_requests) > 0:
            # 采用批量写入的方式，加快保存速度
            update_result = DB_CONN['finance_report'].bulk_write(update_requests, ordered=False)
            print('股票 %s, 财报，更新 %d, 插入 %d' %
                  (code, update_result.modified_count, update_result.upserted_count))


if __name__ == "__main__":
    crawl_finance_report()
