import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import numpy as np
import pandas as pd
from sklearn import linear_model
from factor_config import *


def mad(df_factor):
    """3倍中位数去极值"""
    # 求出因子值的中位数
    median = np.median(df_factor)
    # 求出因子值与中位数的差值, 进行绝对值
    mad = np.median(abs(df_factor - median))
    # 定义几倍的中位数上下限
    high = median + (3 * 1.4826 * mad)
    low = median - (3 * 1.4826 * mad)
    # 替换上下限
    df_factor = np.where(df_factor > high, high, df_factor)
    df_factor = np.where(df_factor < low, low, df_factor)
    return df_factor


def percentile(df_factor, min=0.01, max=0.99):
    """固定比例去极值"""
    # 得到上下限的值
    q = df_factor.quantile([min, max])
    # 超出上下限的值，赋值为上下限
    return np.clip(df_factor, q.iloc[0], q.iloc[-1])


def stand(df_factor):
    """z-score 标准化"""
    mean = df_factor.mean()
    std = df_factor.std()
    return (df_factor - mean) / std


# input是某个行业的因子所有数据, 去均值中性化
def ind_func(df_factor):
    ind_mean = np.mean(df_factor)
    df_factor = df_factor - ind_mean
    return df_factor


def neutralization_industy(df, factor, industy):
    # 行业以外就统一归一化。
    df[industy].fillna(value="其他行业", inplace=True)

    # temp_df = pd.DataFrame()
    # temp_df[f'{factor}_行业中性化'] = df.groupby(industy)[factor].apply(lambda x: ind_func(x))

    df[factor] = df.groupby(industy)[factor].apply(lambda x: ind_func(x))

    return df


def neutralization_market(df, factor, market):
    # 做线性回归取残差
    #  建立回归方程
    x = df[market].to_frame()  # 将某列单独做一个df
    y = df[factor]

    lr = linear_model.LinearRegression()
    lr.fit(x, y)
    # 系数矩阵
    # print(lr.coef_)
    # 截距矩阵
    # print(lr.intercept_)

    # 计算线性模型预测值
    y_hat = lr.predict(x)  # 预测
    # 残差
    y_error = y - y_hat  # 去残差
    df[factor] = y_error
    return df


# 中性化需要传入单个因子值和总市值
# 回归取残差时，内存直接爆了。所以用 df[因子]-df[因子].行业平均来当行业中性化
# 先做市值，再做行业
def neutralization(df, factor, industy='申万一级行业名称', market='总市值'):
    """行业市值中性化"""
    print_diff_time('enter_fmt_neut_mkt')
    df = neutralization_market(df, factor, market=market)
    print_diff_time('enter_fmt_neut_ind')
    df = neutralization_industy(df, factor, industy=industy)
    return df


def factor_format(df, options_format, factors):
    print_diff_time('enter_fmt_1')
    df.dropna(subset=['下周期涨跌幅'], inplace=True)
    # 删除新股
    df = df[df['上市至今交易天数'] > 250]
    print_diff_time('enter_fmt_2')
    # ===删除下个交易日不交易、开盘涨停的股票，因为这些股票在下个交易日开盘时不能买入。
    df = df[df['下日_是否交易'] == 1]
    # df = df[df['下日_开盘涨停'] == False] # 因子分析时，可以使用下一日开盘涨停的股票。
    df = df[df['下日_是否ST'] == False]
    df = df[df['下日_是否退市'] == False]
    print_diff_time('enter_fmt_3')
    # df = df.sort_values(by=['股票代码', '交易日期'], ascending=True)
    print_diff_time('enter_fmt_4')

    if '科创创业板' not in options_format:
        df = df[~df['股票代码'].str.contains('sh68|sz30|bj')]

    print_diff_time('enter_fmt_5')

    for factor in factors:
        df[f'{factor}_原始'] = df[factor]
        # temp_df = pd.DataFrame()
        print_diff_time('enter_fmt_stand')

        df[factor] = stand(df[factor])

        if '去极值' in options_format:
            print_diff_time('enter_fmt_mad')
            df[factor] = percentile(df[factor])

        # if '行业市值中性化' in options_format:
        #     print_diff_time('enter_fmt_neut')
        #     df['总市值'] = stand(df['总市值'])
        #     df = neutralization(df, factor, industy='申万一级行业名称', market='总市值')

        if '市值中性化' in options_format:
            print_diff_time('enter_fmt_neut_mkt')
            df = neutralization_market(df, factor, market='总市值')

        if '行业中性化' in options_format:
            print_diff_time('enter_fmt_neut_ind')
            df = neutralization_industy(df, factor, industy='申万一级行业名称')

    print_diff_time('enter_fmt_sort')
    df = df.sort_values(by=['交易日期', '股票代码'], ascending=True)
    print_diff_time('enter_fmt_done')

    return df


def drop_na_factor(df, factor):
    # temp_df = df.copy()
    temp_df = df
    temp_df.dropna(subset=[factor], inplace=True)
    # temp_df = temp_df.sort_values(by=['交易日期', '股票代码'], ascending=True)
    return temp_df
