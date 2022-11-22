import time
from datetime import datetime

import pandas as pd
import warnings
import numpy as np
from sqlalchemy import create_engine
import akshare as ak
warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows',None)
pd.set_option('max_columns',None)
pd.set_option('expand_frame_repr',False)
pd.set_option('display.unicode.ambiguous_as_wide',True)
pd.set_option('display.unicode.east_asian_width',True)



"""
选股策略
择时策略
"""
# class StockSelectionStrategy(object):

# 可用日线数量约束
# g_available_days_limit = 250
# # 日线数据少于g_available_days_limit，则不创建
# if df.shape[0] < g_available_days_limit:
#     continue

def extend_factor(df):
    """
    计算扩展因子

    :param df: 待计算扩展因子的DataFrame
    :return: 包含扩展因子的DataFrame
    """

    # pipe 可以将一个函数的处理结果传递给另外一个函数，使得代码简洁、便于阅读
    # 使用pipe计算涨停因子
    # df = df.pipe(zt)
    df = df.pipe(zt).pipe(ss, delta_days=30).pipe(candidate)
    return df

def zt(df):
    """
    计算涨停因子

    若涨停，则因子为True，否则为False
    以当日收盘价较前一日收盘价上涨9.8%及以上作为涨停判断标准

    :param df: 待计算扩展因子的DataFrame
    :return: 包含扩展因子的DataFrame
    """

    df['zt'] = np.where((df['收盘价'].values >= 1.098 * df['前收盘价'].values), True, False)

    return df

def shift_i(df, factor_list, i, fill_value=0, suffix='a'):
    """
    计算移动因子，用于获取前i日或者后i日的因子

    :param df: 待计算扩展因子的DataFrame
    :param factor_list: 待移动的因子列表
    :param i: 移动的步数
    :param fill_value: 用于填充NA的值，默认为0
    :param suffix: 值为a(ago)时表示移动获得历史数据，用于计算指标；值为l(later)时表示获得未来数据，用于计算收益
    :return: 包含扩展因子的DataFrame
    """

    # 选取需要shift的列构成新的DataFrame，进行shift操作
    shift_df = df[factor_list].shift(i, fill_value=fill_value)

    # 对新的DataFrame列进行重命名
    shift_df.rename(columns={x: '{}_{}{}'.format(x, i, suffix) for x in factor_list}, inplace=True)

    # 将重命名后的DataFrame合并到原始DataFrame中
    df = pd.concat([df, shift_df], axis=1)

    return df


def shift_till_n(df, factor_list, n, fill_value=0, suffix='a'):
    """
    计算范围移动因子

    用于获取前/后n日内的相关因子，内部调用了shift_i

    :param df: 待计算扩展因子的DataFrame
    :param factor_list: 待移动的因子列表
    :param n: 移动的步数范围
    :param fill_value: 用于填充NA的值，默认为0
    :param suffix: 值为a(ago)时表示移动获得历史数据，用于计算指标；值为l(later)时表示获得未来数据，用于计算收益
    :return: 包含扩展因子的DataFrame
    """

    for i in range(n):
        df = shift_i(df, factor_list, i + 1, fill_value, suffix)
    return df


def ss(df, delta_days=30):
    """
    计算双神因子，即间隔的两个涨停

    若当日形成双神，则因子为True，否则为False

    :param df: 待计算扩展因子的DataFrame
    :param delta_days: 两根涨停间隔的时间不能超过该值，否则不判定为双神，默认值为30
    :return: 包含扩展因子的DataFrame
    """

    # 移动涨停因子，求取近delta_days天内的涨停情况，保存在一个临时DataFrame中
    temp_df = shift_till_n(df, ['zt'], delta_days, fill_value=False)

    # 生成列表，用于后续检索第2天前至第delta_days天前是否有涨停出现
    col_list = ['zt_{}a'.format(x) for x in range(2, delta_days + 1)]

    # 计算双神，需同时满足3个条件：
    # 1、第2天前至第delta_days天前，至少有1个涨停
    # 2、1天前不是涨停（否则就是连续涨停，不是间隔的涨停）
    # 3、当天是涨停
    df['ss'] = temp_df[col_list].any(axis=1) & ~temp_df['zt_1a'] & temp_df['zt']

    return df

def ma(df, n=5, factor='收盘'):
    """
    计算均线因子

    :param df: 待计算扩展因子的DataFrame
    :param n: 待计算均线的周期，默认计算5日均线
    :param factor: 待计算均线的因子，默认为收盘价
    :return: 包含扩展因子的DataFrame
    """

    # 均线名称，例如，收盘价的5日均线名称为ma_5，成交量的5日均线名称为volume_ma_5
    name = '{}ma_{}'.format('' if '收盘' == factor else factor + '_', n+'d')

    # 取待计算均线的因子列
    s = pd.Series(df[factor], name=name, index=df.index)

    # 利用rolling和mean计算均线数据
    s = s.rolling(center=False, window=n).mean()

    # 将均线数据添加到原始的DataFrame中
    df = df.join(s)

    # 均线数值保留两位小数
    df[name] = df[name].apply(lambda x: round(x + 0.001, 2))

    return df

def mas(df, ma_list=None, factor='close'):
    """
    计算多条均线因子，内部调用ma计算单条均线

    :param df: 待计算扩展因子的DataFrame
    :param ma_list: 待计算均线的周期列表，默认为None
    :param factor: 待计算均线的因子，默认为收盘价
    :return: 包含扩展因子的DataFrame
    """

    if ma_list is None:
        ma_list = []
    for i in ma_list:
        df = ma(df, i, factor)
    return df

def cross_mas(df, ma_list=None):
    """
    计算穿均线因子

    若当日最低价不高于均线价格
    且当日收盘价不低于均线价格
    则当日穿均线因子值为True，否则为False

    :param df: 待计算扩展因子的DataFrame
    :param ma_list: 均线的周期列表，默认为None
    :return: 包含扩展因子的DataFrame
    """

    if ma_list is None:
        ma_list = []
    for i in ma_list:
        df['cross_{}'.format(i)] = (df['low'] <= df['ma_{}'.format(i)]) & (
                df['ma_{}'.format(i)] <= df['close'])
    return df


def candidate(df):
    """
    计算是否为候选

    若当日日线同时穿过5、10、20、30日均线
    且30日均线在60日均线上方
    且当日形成双神
    则当日作为候选，该因子值为True，否则为False

    :param df: 待计算扩展因子的DataFrame
    :return: 包含扩展因子的DataFrame
    """

    # 均线周期列表
    ma_list = [5, 10, 20, 30, 60]

    # 计算均线的因子，保存到临时的DataFrame中
    temp_df = mas(df, ma_list)

    # 计算穿多线的因子，保存到临时的DataFrame中
    temp_df = cross_mas(temp_df, ma_list)

    # 穿多线因子的列名列表
    column_list = ['cross_{}'.format(x) for x in ma_list[:-1]]

    # 计算是否为候选
    df['candidate'] = temp_df[column_list].all(axis=1) & (temp_df['ma_30'] >= temp_df['ma_60']) & df['ss']

    return df