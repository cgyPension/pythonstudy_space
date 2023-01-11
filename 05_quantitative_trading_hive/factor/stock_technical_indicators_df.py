import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import time
import warnings
from datetime import date,datetime
import akshare as ak
import numpy as np
import pandas as pd
import talib as ta
import MyTT as mt
from util.algorithmUtils import mt_rsi, ta_rsi
from util.CommonUtils import get_spark

# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

def get_data():
    '''技术面很多是通达信那种移动平均的，省事直接全量'''
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    spark_df = spark.sql("""
        select trade_date,
               stock_code,
               stock_name,
               close_price as close
        from stock.ods_dc_stock_quotes_di
        order by trade_date
        """)

    pd_df = spark_df.toPandas()
    pd_df['rsi_6d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.RSI(x['close'],6))
    pd_df['rsi_12d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.RSI(x['close'],12))

    pd_df['ma_5d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close'],timeperiod=5))
    pd_df['ma_10d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close'],timeperiod=10))
    pd_df['ma_20d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close'],timeperiod=20))
    pd_df['ma_50d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close'],timeperiod=50))
    pd_df['ma_120d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close'],timeperiod=120))
    pd_df['ma_200d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close'],timeperiod=200))
    pd_df['ma_250d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close'],timeperiod=250))

    pd_df['update_time'] = datetime.now()
    pd_df['td'] = pd_df['trade_date']
    pd_df = pd_df[['trade_date', 'stock_code', 'stock_name', 'rsi_6d', 'rsi_12d','ma_5d','ma_10d', 'ma_20d', 'ma_50d', 'ma_120d','ma_200d','ma_250d','update_time', 'td']]
    spark_df = spark.createDataFrame(pd_df)
    spark_df.repartition(1).write.insertInto('factor.stock_technical_indicators_df', overwrite=True)
    spark.stop()
    print('{}：执行完毕！！！'.format(appName))



# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/stock_technical_indicators_df.py
if __name__ == '__main__':
    # if len(sys.argv) == 1:
    #     print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    # elif len(sys.argv) == 2:
    #     run_type = sys.argv[1]
    #     if run_type == 'all':
    #         start_date = '20210101'
    #         end_date = date.today().strftime('%Y%m%d')
    #     else:
    #         start_date = date.today().strftime('%Y%m%d')
    #         end_date = start_date
    # elif len(sys.argv) == 4:
    #     run_type = sys.argv[1]
    #     start_date = sys.argv[2]
    #     end_date = sys.argv[3]

    start_time = time.time()
    get_data()
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))