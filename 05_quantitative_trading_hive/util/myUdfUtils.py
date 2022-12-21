import os
import sys
from pyspark.sql.types import *
import pandas as pd
# import talib as ta
# import MyTT as mt
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

'''环境有问题单独识别不了这个包'''

def registerUDF(spark):
    '''批量注册udf'''
    spark_udf = spark.udf
    spark_udf.register('pow1', pow1, returnType=DoubleType())
    spark_udf.register('cj_func', cj_func, returnType=DecimalType(precision=20,scale=4))
    # spark_udf.register('ta_rsi', ta_rsi, returnType=DecimalType(precision=20,scale=4))
    # spark_udf.register('mt_rsi', mt_rsi, returnType=DecimalType(precision=20,scale=4))


def pow1(m, n):
    return float(m) ** float(n)

def cj_func(a:pd.Series,b:pd.Series):
    return a * b

# def ta_rsi(close:pd.Series,n) -> pd.Series:
#     return ta.RSI(close, timeperiod=n)

# def mt_rsi(close:pd.Series,n) -> pd.Series:
#     dif = close - mt.REF(close, 1)
#     return mt.RD(mt.SMA(mt.MAX(dif, 0), n) / mt.SMA(mt.ABS(dif), n) * 100)