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

'''ICIR 因子清洗工具'''


def filter_extreme_MAD(series,n=3):
    """3倍中位数去极值"""
    median = series.median()
    new_median = ((series - median).abs()).median()
    return series.clip(median - n*new_median,median + n*new_median)

def percentile(df_factor, min=0.01, max=0.99):
    """固定比例去极值"""
    # 得到上下限的值
    q = df_factor.quantile([min, max])
    # 超出上下限的值，赋值为上下限
    return np.clip(df_factor, q.iloc[0], q.iloc[-1])

def winsorize_percentile(series, left=0.025, right=0.975):
    lv, rv = np.percentile(series, [left*100, right*100])
    return series.clip(lv, rv)

def stand(series):
    """z-score 标准化"""
    mean = series.mean()
    std = series.std()
    return (series - mean) / std

def nonlinear_transform(series, quantile_to_subtract):
    # 转换后因子值 = -（因子值 - 对应分位数）^2，此时因子值离该分位数越近，转换后因子值越大
    return -(series.sub(series.quantile(quantile_to_subtract, axis=1), axis=0) ** 2)