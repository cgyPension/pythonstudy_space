import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from pyspark.sql.types import *
import pandas as pd
import talib as ta
from MyTT import *
import akshare as ak
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


def cj_func(a:pd.Series,b:pd.Series):
    return a * b

def mt_rsi(close:pd.Series,n):
    dif = close - REF(close, 1)
    return RD(SMA(MAX(dif, 0), n) / SMA(ABS(dif), n) * 100)

'''环境有问题单独识别不了别的文件'''
def registerUDF(spark):
    '''批量注册udf'''
    spark_udf = spark.udf
    spark_udf.register('cj_func', cj_func, returnType=DecimalType(precision=20,scale=4))
    # spark_udf.register('mt_rsi', mt_rsi, returnType=DecimalType(precision=20, scale=4))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/test_talib.py
# nohup python /opt/code/05_quantitative_trading_hive/bak/test_spark.py >> my.log 2>&1 &
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/test_talib.py
# 这个要用远程调试执行 不然本地读不了linux
if __name__ == '__main__':
    appName = os.path.basename(__file__)
    df = ak.stock_zh_a_hist(symbol='002811', period='daily', start_date='20221201', end_date='20221219',
                            adjust='qfq')

    CLOSE = df['收盘'].values
    OPEN = df['开盘'].values  # 基础数据定义，只要传入的是序列都可以
    HIGH = df['最高'].values
    LOW = df['最低'].values  # 例如 CLOSE=list(df.close) 都是一样


    MA5 = MA(CLOSE, 5)  # 获取5日均线序列
    MA10 = MA(CLOSE, 10)  # 获取10日均线序列
    df['ma5'] = MA5
    df['ma10'] = MA10
    df['ta_rsi_6d']=ta.RSI(CLOSE, timeperiod=6)
    df['mt_rsi_6d']=mt_rsi(CLOSE,6)
    print(df)

    print('BTC5日均线', MA5[-1])  # 只取最后一个数
    print('BTC10日均线', RET(MA10))  # RET(MA10) == MA10[-1]
    print('今天5日线是否上穿10日线', RET(CROSS(MA5, MA10)))
    print('最近5天收盘价全都大于10日线吗？', EVERY(CLOSE > MA10, 5))
    print('{}：执行完毕！！！'.format(appName))

