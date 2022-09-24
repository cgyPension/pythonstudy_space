#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""


from datetime import datetime, timedelta

import tushare as ts
from pymongo import UpdateOne

from database import DB_CONN
from stock_util import get_trading_dates

"""
从tushare获取股票基础数据，保存到本地的MongoDB数据库中
"""


def crawl_basic(begin_date=None, end_date=None):
    """
    抓取指定时间范围内的股票基础信息
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """

    # 如果没有指定开始日期，则默认为前一日
    if begin_date is None:
        begin_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # 如果没有指定结束日期，则默认为前一日
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # 获取指定日期范围的所有交易日列表
    all_dates = get_trading_dates(begin_date, end_date)

    # 按照每个交易日抓取
    for date in all_dates:
        try:
            # 抓取当日的基本信息
            crawl_basic_at_date(date)
        except:
            print('抓取股票基本信息时出错，日期：%s' % date, flush=True)


def crawl_basic_at_date(date):
    """
    从Tushare抓取指定日期的股票基本信息
    :param date: 日期
    """
    # 从TuShare获取基本信息，index是股票代码列表
    df_basics = ts.get_stock_basics(date)

    # 如果当日没有基础信息，在不做操作
    if df_basics is None:
        return

    # 初始化更新请求列表
    update_requests = []
    # 获取所有股票代码集合
    codes = set(df_basics.index)
    # 按照股票代码提取所有数据
    for code in codes:
        # 获取一只股票的数据
        doc = dict(df_basics.loc[code])
        try:
            # 将上市日期，20180101转换为2018-01-01的形式
            time_to_market = datetime \
                .strptime(str(doc['timeToMarket']), '%Y%m%d') \
                .strftime('%Y-%m-%d')

            # 将总股本和流通股本转为数字
            totals = float(doc['totals'])
            outstanding = float(doc['outstanding'])

            # 组合成基本信息文档
            doc.update({
                # 股票代码
                'code': code,
                # 日期
                'date': date,
                # 上市日期
                'timeToMarket': time_to_market,
                # 流通股本
                'outstanding': outstanding,
                # 总股本
                'totals': totals
            })

            # 生成更新请求，需要按照code和date创建索引
            update_requests.append(
                UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': doc}, upsert=True))
        except:
            print('发生异常，股票代码：%s，日期：%s' % (code, date), flush=True)
            print(doc, flush=True)

    # 如果抓到了数据
    if len(update_requests) > 0:
        update_result = DB_CONN['basic'].bulk_write(update_requests, ordered=False)

        print('抓取股票基本信息，日期：%s, 插入：%4d条，更新：%4d条' %
              (date, update_result.upserted_count, update_result.modified_count), flush=True)


if __name__ == '__main__':
    crawl_basic('2017-01-01', '2017-12-31')
