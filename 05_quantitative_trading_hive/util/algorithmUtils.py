import math
import os
import sys
import pandas as pd
import numpy as np
import talib as ta
import MyTT as mt
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)


def max_drawdown(df):
    '''计算最大回撤
        df[name] 传入收盘价
    '''
    md=((df.cummax()-df)/df.cummax()).max()
    return round(md*100,2)

def get_holding_yield_qc(df):
    '''得到收益走势：持股收益
        一个月22天交易日
        3个月66
        一年250
        今日收益率
    '''
    df['yield_1d'] = (df.close / df.close.shift(1) - 1)*100
    df['yield_1d'] = df['yield_1d'].fillna(0)
    df['yield_7d'] = df['yield_1d'].rolling(window=7, min_periods=1).sum()
    df['yield_22d'] = df['yield_1d'].rolling(window=22, min_periods=1).sum()
    df['yield_66d'] = df['yield_1d'].rolling(window=66, min_periods=1).sum()
    df['yield_td'] = (df.close / df.close.iloc[0] - 1)*100
    # df['yield_td']=df.close.cumprod()
    # df['yield_td']=(df.close+1).cumprod()-1
    return df

def get_holding_yield_tj(df):
    '''得到统计收益'''
    dict = {}
    # 收益统计表
    dict['yield_td'] = round(df['yield_td'].iloc[-1], 2)
    dict['yield_1d'] = round(df['yield_1d'].iloc[-1], 2)
    dict['yield_7d'] = round(df['yield_7d'].iloc[-1], 2)
    dict['yield_22d'] = round(df['yield_22d'].iloc[-1], 2)
    dict['yield_66d'] = round(df['yield_66d'].iloc[-1], 2)
    # 年化收益率
    # dict['annual_ret'] = round((pow(1 + df['yield_td'].iloc[-1], 250 / len(df)) - 1), 2)
    dict['annual_ret'] = round((pow(1 + df['yield_td'].iloc[-1]/100, 250 / len(df)) - 1)*100, 2)
    dict['max_drawdown']= max_drawdown(df.close)
    # 夏普比率 表示每承受一单位总风险，会产生多少的超额报酬，可以同时对策略的收益与风险进行综合考虑。可以理解为经过风险调整后的收益率。计算公式如下，该值越大越好
    # 超额收益率以无风险收益率为基准
    # 公认默认无风险收益率为年化3%
    # exReturn = df['yield_1d']/100 - 0.03 / 250
    exReturn = df['yield_1d'] - 0.03 / 250
    dict['sharp_ratio'] = round(np.sqrt(len(exReturn)) * exReturn.mean() / exReturn.std(), 2)
    return dict


def mt_rsi(close:pd.Series,n):
    dif = close - mt.REF(close, 1)
    return mt.RD(mt.SMA(mt.MAX(dif, 0), n) / mt.SMA(mt.ABS(dif), n) * 100)

def ta_rsi(close:pd.Series,n):
    return ta.RSI(close, timeperiod=n)

def dwd_factor(df):

    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

if __name__ == '__main__':
     test_xxx()