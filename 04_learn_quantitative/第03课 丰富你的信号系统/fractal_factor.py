#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""


from database import DB_CONN
from stock_util import get_all_codes
from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame
import traceback


def compute_fractal(begin_date, end_date):
    # 获取所有股票代码
    codes = get_all_codes()

    # 计算每个股票的信号
    for code in codes:
        try:
            # 获取后复权的价格，使用后复权的价格计算分型信号
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'high': True, 'low': True, '_id': False}
            )

            df_daily = DataFrame([daily for daily in daily_cursor])

            # 设置日期作为索引
            df_daily.set_index(['date'], 1, inplace=True)

            # 通过shift，将前两天和后两天对齐到中间一天
            df_daily_shift_1 = df_daily.shift(1)
            df_daily_shift_2 = df_daily.shift(2)
            df_daily_shift_3 = df_daily.shift(3)
            df_daily_shift_4 = df_daily.shift(4)

            # 顶分型，中间日的最高价既大于前两天的最高价，也大于后两天的最高价
            df_daily['up'] = (df_daily_shift_3['high'] > df_daily_shift_1['high']) & \
                             (df_daily_shift_3['high'] > df_daily_shift_2['high']) & \
                             (df_daily_shift_3['high'] > df_daily_shift_4['high']) & \
                             (df_daily_shift_3['high'] > df_daily['high'])

            # 底分型，中间日的最低价既小于前两天的最低价，也小于后两天的最低价
            df_daily['down'] = (df_daily_shift_3['low'] < df_daily_shift_1['low']) & \
                               (df_daily_shift_3['low'] < df_daily_shift_2['low']) & \
                               (df_daily_shift_3['low'] < df_daily_shift_4['low']) & \
                               (df_daily_shift_3['low'] < df_daily['low'])

            # 只保留了出现顶分型和低分型信号的日期
            df_daily = df_daily[(df_daily['up'] | df_daily['down'])]

            # 抛掉不用的数据
            df_daily.drop(['high', 'low'], 1, inplace=True)

            print(df_daily)
            # 将信号保存到数据库 ,
            update_requests = []
            # 保存的数据结果时，code、date和信号的方向
            for date in df_daily.index:
                doc = {
                    'code': code,
                    'date': date,
                    # up: 顶分型， down：底分型
                    'direction': 'up' if df_daily.loc[date]['up'] else 'down'
                }

                # 保存时以code、date和direction做条件，那么就需要在这三个字段上建立索引
                # db.fractal_signal.createIndex({'code': 1, 'date': 1, 'direction': 1})
                update_requests.append(
                    UpdateOne(doc, {'$set': doc}, upsert=True))

            if len(update_requests) > 0:
                update_result = DB_CONN['fractal_signal'].bulk_write(update_requests, ordered=False)
                print('%s, upserted: %4d, modified: %4d' %
                      (code, update_result.upserted_count, update_result.modified_count),
                      flush=True)
        except:
            print('错误发生： %s' % code, flush=True)
            traceback.print_exc()


def is_fractal_up(code, date):
    """
    查询某只股票在某个日期是否出现顶分型信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 出现顶分型信号，False - 没有出现顶分型信号
    """
    count = DB_CONN['fractal_signal'].count({'code': code, 'date': date, 'direction': 'up'})
    return count == 1


def is_fractal_down(code, date):
    """
    查询某只股票在某个日期是否出现底分型信号
    :param code: 股票代码
    :param date: 日期
    :return: True - 出现底分型信号，False - 没有出现底分型信号
    """
    count = DB_CONN['fractal_signal'].count({'code': code, 'date': date, 'direction': 'down'})
    return count == 1


if __name__ == '__main__':
    compute_fractal('2015-01-01', '2015-12-31')

