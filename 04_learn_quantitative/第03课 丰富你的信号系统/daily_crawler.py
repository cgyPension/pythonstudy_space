#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""

from pymongo import UpdateOne
from database import DB_CONN
import tushare as ts
from datetime import datetime

"""
从tushare获取日K数据，保存到本地的MongoDB数据库中
"""


class DailyCrawler:
    def __init__(self):
        """
        初始化
        """

        # 创建daily数据集
        self.daily = DB_CONN['daily']
        # 创建daily_hfq数据集
        self.daily_hfq = DB_CONN['daily_hfq']

    def crawl_index(self, begin_date=None, end_date=None):
        """
        抓取指数的日K数据。
        指数行情的主要作用：
        1. 用来生成交易日历
        2. 回测时做为收益的对比基准

        :param begin_date: 开始日期
        :param end_date: 结束日期
        """

        # 指定抓取的指数列表，可以增加和改变列表里的值
        index_codes = ['000001', '000300', '399001', '399005', '399006']

        # 当前日期
        now = datetime.now().strftime('%Y-%m-%d')
        # 如果没有指定开始，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日，则默认为当前日期
        if end_date is None:
            end_date = now

        # 按照指数的代码循环，抓取所有指数信息
        for code in index_codes:
            # 抓取一个指数的在时间区间的数据
            df_daily = ts.get_k_data(code, index=True, start=begin_date, end=end_date)
            # 保存数据
            self.save_data(code, df_daily, self.daily, {'index': True})

    def save_data(self, code, df_daily, collection, extra_fields=None):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param code: 股票代码
        :param df_daily: 包含日线数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
        """

        # 数据更新的请求列表
        update_requests = []

        # 将DataFrame中的行情数据，生成更新数据的请求
        for df_index in df_daily.index:
            # 将DataFrame中的一行数据转dict
            doc = dict(df_daily.loc[df_index])
            # 设置股票代码
            doc['code'] = code

            # 如果指定了其他字段，则更新dict
            if extra_fields is not None:
                doc.update(extra_fields)

            # 生成一条数据库的更新请求
            # 注意：
            # 需要在code、date、index三个字段上增加索引，否则随着数据量的增加，
            # 写入速度会变慢，创建索引的命令式：
            # db.daily.createIndex({'code':1,'date':1,'index':1})
            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': doc['index']},
                    {'$set': doc},
                    upsert=True)
            )

        # 如果写入的请求列表不为空，则保存都数据库中
        if len(update_requests) > 0:
            # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('保存日线数据，代码： %s, 插入：%4d条, 更新：%4d条' %
                  (code, update_result.upserted_count, update_result.modified_count),
                  flush=True)

    def crawl(self, begin_date=None, end_date=None):
        """
        抓取股票的日K数据，主要包含了不复权和后复权两种

        :param begin_date: 开始日期
        :param end_date: 结束日期
        """

        # 通过tushare的基本信息API，获取所有股票的基本信息
        stock_df = ts.get_stock_basics()
        # 将基本信息的索引列表转化为股票代码列表
        codes = list(stock_df.index)

        # 当前日期
        now = datetime.now().strftime('%Y-%m-%d')

        # 如果没有指定开始日期，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日期，则默认为当前日期
        if end_date is None:
            end_date = now

        for code in codes:
            # 抓取不复权的价格
            df_daily = ts.get_k_data(code, autype=None, start=begin_date, end=end_date)
            self.save_data(code, df_daily, self.daily, {'index': False})

            # 抓取后复权的价格
            df_daily_hfq = ts.get_k_data(code, autype='hfq', start=begin_date, end=end_date)
            self.save_data(code, df_daily_hfq, self.daily_hfq, {'index': False})


# 抓取程序的入口函数
if __name__ == '__main__':
    dc = DailyCrawler()
    # 抓取指定日期范围的指数日行情
    # 这两个参数可以根据需求改变，时间范围越长，抓取时花费的时间就会越长
    dc.crawl_index('2015-01-01', '2015-12-31')
    # 抓取指定日期范围的股票日行情
    # 这两个参数可以根据需求改变，时间范围越长，抓取时花费的时间就会越长
    dc.crawl('2015-01-01', '2015-12-31')
