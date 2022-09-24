#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""

"""
compute_pe：计算市盈率
"""

from pymongo import DESCENDING, UpdateOne

from database import DB_CONN
from stock_util import get_all_codes

finance_report_collection = DB_CONN['finance_report']
daily_collection = DB_CONN['daily']


def compute_pe():
    """
    计算股票在某只的市盈率
    """

    # 获取所有股票
    codes = get_all_codes()

    for code in codes:
        print('计算市盈率, %s' % code)
        daily_cursor = daily_collection.find(
            {'code': code},
            projection={'close': True, 'date': True})

        update_requests = []
        for daily in daily_cursor:
            _date = daily['date']
            # 找到该股票距离当前日期最近的年报，通过公告日期查询，防止未来函数
            finance_report = finance_report_collection.find_one(
                {'code': code, 'report_date': {'$regex': '\d{4}-12-31'}, 'announced_date': {'$lte': _date}},
                sort=[('announced_date', DESCENDING)]
            )

            if finance_report is None:
                continue

            # 计算滚动市盈率并保存到daily_k中
            eps = 0
            if finance_report['eps'] != '-':
                eps = finance_report['eps']

            # 计算PE
            if eps != 0:
                update_requests.append(UpdateOne(
                    {'code': code, 'date': _date},
                    {'$set': {'pe': round(daily['close'] / eps, 4)}}))

        if len(update_requests) > 0:
            update_result = daily_collection.bulk_write(update_requests, ordered=False)
            print('更新PE, %s, 更新：%d' % (code, update_result.modified_count))


if __name__ == "__main__":
    compute_pe()
