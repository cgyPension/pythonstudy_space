#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""


import traceback

from pandas import DataFrame
from pymongo import UpdateOne, ASCENDING

from database import DB_CONN
from stock_util import get_all_codes


def compute(begin_date, end_date):
    """
    计算指定日期内的Boll突破上轨和突破下轨信号，并保存到数据库中，
    方便查询使用
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """

    # 获取所有股票代码
    all_codes = get_all_codes()

    # 计算每一只股票的Boll信号
    for code in all_codes:
        try:
            # 获取后复权的价格，使用后复权的价格计算BOLL
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id': False}
            )

            df_daily = DataFrame([daily for daily in daily_cursor])

            # 计算MB，盘后计算，这里用当日的Close
            df_daily['MB'] = df_daily['close'].rolling(20).mean()
            # 计算STD20，计算20日的标准差
            df_daily['std'] = df_daily['close'].rolling(20).std()

            print(df_daily, flush=True)
            # 计算UP，上轨
            df_daily['UP'] = df_daily['MB'] + 2 * df_daily['std']
            # 计算down，下轨
            df_daily['DOWN'] = df_daily['MB'] - 2 * df_daily['std']

            print(df_daily, flush=True)

            # 将日期作为索引
            df_daily.set_index(['date'], inplace=True)

            # 将close移动一个位置，变为当前索引位置的前收
            last_close = df_daily['close'].shift(1)

            # 将上轨移一位，前一日的上轨和前一日的收盘价都在当日了
            shifted_up = df_daily['UP'].shift(1)
            # 突破上轨，是向上突破，条件是前一日收盘价小于前一日上轨，当日收盘价大于当日上轨
            df_daily['up_mask'] = (last_close <= shifted_up) & (df_daily['close'] > shifted_up)

            # 将下轨移一位，前一日的下轨和前一日的收盘价都在当日了
            shifted_down = df_daily['DOWN'].shift(1)
            # 突破下轨，是向下突破，条件是前一日收盘价大于前一日下轨，当日收盘价小于当日下轨
            df_daily['down_mask'] = (last_close >= shifted_down) & (df_daily['close'] < shifted_down)

            # 对结果进行过滤，只保留向上突破或者向上突破的数据
            df_daily = df_daily[df_daily['up_mask'] | df_daily['down_mask']]
            # 从DataFrame中扔掉不用的数据
            df_daily.drop(['close', 'std', 'MB', 'UP', 'DOWN'], 1, inplace=True)

            # 将信号保存到数据库
            update_requests = []
            # DataFrame的索引是日期
            for date in df_daily.index:
                # 保存的数据包括股票代码、日期和信号类型，结合数据集的名字，就表示某只股票在某日
                doc = {
                    'code': code,
                    'date': date,
                    # 方向，向上突破 up，向下突破 down
                    'direction': 'up' if df_daily.loc[date]['up_mask'] else 'down'
                }
                update_requests.append(
                    UpdateOne(doc, {'$set': doc}, upsert=True))

            # 如果有信号数据，则保存到数据库中
            if len(update_requests) > 0:
                # 批量写入到boll数据集中
                update_result = DB_CONN['boll'].bulk_write(update_requests, ordered=False)
                print('%s, upserted: %4d, modified: %4d' %
                      (code, update_result.upserted_count, update_result.modified_count),
                      flush=True)
        except:
            traceback.print_exc()


def is_boll_break_up(code, date):
    """
    查询某只股票是否在某日出现了突破上轨信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 出现了突破上轨信号，False - 没有出现突破上轨信号
    """
    count = DB_CONN['boll'].count({'code': code, 'date': date, 'direction': 'up'})
    return count == 1


def is_boll_break_down(code, date):
    """
    查询某只股票是否在某日出现了突破下轨信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 出现了突破下轨信号，False - 没有出现突破下轨信号
    """
    count = DB_CONN['boll'].count({'code': code, 'date': date, 'direction': 'down'})
    return count == 1


if __name__ == '__main__':
    # 计算指定时间内的boll信号
    compute(begin_date='2015-01-01', end_date='2015-12-31')