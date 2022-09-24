#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""


from datetime import datetime, timedelta

from pymongo import UpdateOne, ASCENDING

from database import DB_CONN
from stock_util import get_trading_dates, get_all_codes

"""
对日行情数据做进一步的处理：
1. 填充is_trading字段，is_trading用来区分某只股票在某个交易日是否为停牌
2. 填充停牌日的行情数据
3. 填充复权因子和前收
"""


def fill_is_trading_between(begin_date=None, end_date=None):
    """
    填充指定时间段内的is_trading字段
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """

    # 获取指定日期范围的所有交易日列表，按日期正序排列
    all_dates = get_trading_dates(begin_date, end_date)

    # 循环填充所有交易日的is_trading字段
    for date in all_dates:
        # 填充daily数据集
        fill_single_date_is_trading(date, 'daily')
        # 填充daily_hfq数据集
        fill_single_date_is_trading(date, 'daily_hfq')


def fill_is_trading(date=None):
    """
    为日线数据增加is_trading字段，表示是否交易的状态，True - 交易  False - 停牌
    从Tushare来的数据不包含交易状态，也不包含停牌的日K数据，为了系统中使用的方便，我们需要填充停牌是的K数据。
    一旦填充了停牌的数据，那么数据库中就同时包含了停牌和交易的数据，为了区分这两种数据，就需要增加这个字段。

    在填充该字段时，要考虑到是否最坏的情况，也就是数据库中可能已经包含了停牌和交易的数据，但是却没有is_trading
    字段。这个方法通过交易量是否为0，来判断是否停牌
    """

    if date is None:
        all_dates = get_trading_dates()
    else:
        all_dates = [date]

    for date in all_dates:
        fill_single_date_is_trading(date, 'daily')
        fill_single_date_is_trading(date, 'daily_hfq')


def fill_single_date_is_trading(date, collection_name):
    """
    填充某一个日行情的数据集的is_trading
    :param date: 日期
    :param collection_name: 集合名称
    """
    print('填充字段， 字段名: is_trading，日期：%s，数据集：%s' %
          (date, collection_name), flush=True)
    daily_cursor = DB_CONN[collection_name].find(
        {'date': date},
        projection={'code': True, 'volume': True, 'index': True, '_id': False},
        batch_size=1000)

    update_requests = []
    for daily in daily_cursor:
        # 当日成交量大于0，则为交易状态
        is_trading = daily['volume'] > 0

        update_requests.append(
            UpdateOne(
                {'code': daily['code'], 'date': date, 'index': daily['index']},
                {'$set': {'is_trading': is_trading}}))

    if len(update_requests) > 0:
        update_result = DB_CONN[collection_name].bulk_write(update_requests, ordered=False)
        print('填充字段， 字段名: is_trading，日期：%s，数据集：%s，更新：%4d条' %
              (date, collection_name, update_result.modified_count), flush=True)


def fill_daily_k_at_suspension_days(begin_date=None, end_date=None):
    """
    填充指定日期范围内，股票停牌日的行情数据。
    填充时，停牌的开盘价、最高价、最低价和收盘价都为最近一个交易日的收盘价，成交量为0，
    is_trading是False

    :param begin_date: 开始日期
    :param end_date: 结束日期
    """

    # 当前日期的前一天
    before = datetime.now() - timedelta(days=1)
    # 找到据当前最近一个交易日的所有股票的基本信息
    basics = []
    while 1:
        # 转化为str
        last_trading_date = before.strftime('%Y-%m-%d')
        # 因为TuShare的基本信息最早知道2016-08-09，所以如果日期早于2016-08-09
        # 则结束查找
        if last_trading_date < '2016-08-09':
            break

        # 找到当日的基本信息
        basic_cursor = DB_CONN['basic'].find(
            {'date': last_trading_date},
            # 填充时需要用到两个字段股票代码code和上市日期timeToMarket，
            # 上市日期用来判断
            projection={'code': True, 'timeToMarket': True, '_id': False},
            # 一次返回5000条，可以降低网络IO开销，提高速度
            batch_size=5000)

        # 将数据放到basics列表中
        basics = [basic for basic in basic_cursor]

        # 如果查询到了数据，在跳出循环
        if len(basics) > 0:
            break

        # 如果没有找到数据，则继续向前一天
        before -= timedelta(days=1)

    # 获取指定日期范围内所有交易日列表
    all_dates = get_trading_dates(begin_date, end_date)

    # 填充daily数据集中的停牌日数据
    fill_daily_k_at_suspension_days_at_date_one_collection(
        basics, all_dates, 'daily')
    # 填充daily_hfq数据中的停牌日数据
    fill_daily_k_at_suspension_days_at_date_one_collection(
        basics, all_dates, 'daily_hfq')


def fill_daily_k_at_suspension_days_at_date_one_collection(
        basics, all_dates, collection):
    """
    更新单个数据集的单个日期的数据
    :param basics:
    :param all_dates:
    :param collection:
    :return:
    """
    code_last_trading_daily_dict = dict()
    for date in all_dates:
        update_requests = []
        last_daily_code_set = set(code_last_trading_daily_dict.keys())
        for basic in basics:
            code = basic['code']
            # 如果循环日期小于
            if date < basic['timeToMarket']:
                print('日期：%s, %s 还没上市，上市日期: %s' % (date, code, basic['timeToMarket']), flush=True)
            else:
                # 找到当日数据
                daily = DB_CONN[collection].find_one({'code': code, 'date': date, 'index': False})
                if daily is not None:
                    code_last_trading_daily_dict[code] = daily
                    last_daily_code_set.add(code)
                else:
                    if code in last_daily_code_set:
                        last_trading_daily = code_last_trading_daily_dict[code]
                        suspension_daily_doc = {
                            'code': code,
                            'date': date,
                            'close': last_trading_daily['close'],
                            'open': last_trading_daily['close'],
                            'high': last_trading_daily['close'],
                            'low': last_trading_daily['close'],
                            'volume': 0,
                            'is_trading': False
                        }
                        update_requests.append(
                            UpdateOne(
                                {'code': code, 'date': date, 'index': False},
                                {'$set': suspension_daily_doc},
                                upsert=True))
        if len(update_requests) > 0:
            update_result = DB_CONN[collection].bulk_write(update_requests, ordered=False)
            print('填充停牌数据，日期：%s，数据集：%s，插入：%4d条，更新：%4d条' %
                  (date, collection, update_result.upserted_count, update_result.modified_count), flush=True)


def fill_au_factor_pre_close(begin_date, end_date):
    """
    为daily数据集填充：
    1. 复权因子au_factor，复权的因子计算方式：au_factor = hfq_close/close
    2. pre_close = close(-1) * au_factor(-1)/au_factor
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """
    all_codes = get_all_codes()

    for code in all_codes:
        hfq_daily_cursor = DB_CONN['daily_hfq'].find(
            {'code': code, 'date': {'$lte': end_date, '$gte': begin_date}, 'index': False},
            sort=[('date', ASCENDING)],
            projection={'date': True, 'close': True})

        date_hfq_close_dict = dict([(x['date'], x['close']) for x in hfq_daily_cursor])

        daily_cursor = DB_CONN['daily'].find(
            {'code': code, 'date': {'$lte': end_date, '$gte': begin_date}, 'index': False},
            sort=[('date', ASCENDING)],
            projection={'date': True, 'close': True}
        )

        last_close = -1
        last_au_factor = -1

        update_requests = []
        for daily in daily_cursor:
            date = daily['date']
            try:
                close = daily['close']

                doc = dict()

                # 复权因子 = 当日后复权价格 / 当日实际价格
                au_factor = round(date_hfq_close_dict[date] / close, 2)
                doc['au_factor'] = au_factor
                # 当日前收价 = 前一日实际收盘价 * 前一日复权因子 / 当日复权因子
                if last_close != -1 and last_au_factor != -1:
                    pre_close = last_close * last_au_factor / au_factor
                    doc['pre_close'] = round(pre_close, 2)

                last_au_factor = au_factor
                last_close = close

                update_requests.append(
                    UpdateOne(
                        {'code': code, 'date': date, 'index': False},
                        {'$set': doc}))
            except:
                print('计算复权因子时发生错误，股票代码：%s，日期：%s' % (code, date), flush=True)
                # 恢复成初始值，防止用错
                last_close = -1
                last_au_factor = -1

        if len(update_requests) > 0:
            update_result = DB_CONN['daily'].bulk_write(update_requests, ordered=False)
            print('填充复权因子和前收，股票：%s，更新：%4d条' %
                  (code, update_result.modified_count), flush=True)


if __name__ == '__main__':
    fill_au_factor_pre_close('2015-01-01', '2015-12-31')
    fill_is_trading_between('2015-01-01', '2015-12-31')
    fill_daily_k_at_suspension_days('2015-01-01', '2015-12-31')
