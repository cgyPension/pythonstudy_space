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
from util.algorithmUtils import rps
from util.CommonUtils import get_spark

# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

def get_data():
    '''技术面很多是通达信那种移动平均的，
       rsi历史数据越多越准，所以全量
    '''
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    spark_df = spark.sql("""
with tmp_t1 as (
             select trade_date,
                   stock_code,
                   stock_name,
                   close_price,
                   max(high_price)over(partition by stock_code order by trade_date rows between 249 preceding and current row) as high_price_250d,
                   min(low_price)over(partition by stock_code order by trade_date rows between 249 preceding and current row) as low_price_250d,
                    case when substr(stock_code,3,1)='6' then '000001'
                         when substr(stock_code,3,3)='002' then '399005'
                         when substr(stock_code,3,3)='300' then '399006'
                         else '399005' end as index_code
            from stock.ods_dc_stock_quotes_di
),
tmp_t2 as (
    select tmp_t1.trade_date,
       tmp_t1.stock_code,
       tmp_t1.stock_name,
       tmp_t1.close_price,
       high_price_250d,
       low_price_250d,
       tmp_t1.close_price/t2.close_price as rs_1
from tmp_t1
left join stock.ods_dc_index_di t2
        on tmp_t1.trade_date = t2.trade_date
        and tmp_t1.index_code = t2.index_code
)
select trade_date,
       stock_code,
       stock_name,
       close_price,
       high_price_250d,
       low_price_250d,
       (rs_1/avg(rs_1)over(partition by stock_code order by trade_date rows between 49 preceding and current row)-1)*10 as rs
from tmp_t2
order by trade_date
        """)
    pd_df = spark_df.toPandas()

    pd_df['rps_5d'] = rps(pd_df, 5)
    pd_df['rps_10d'] = rps(pd_df, 10)
    pd_df['rps_20d'] = rps(pd_df, 20)
    pd_df['rps_50d'] = rps(pd_df, 50)
    pd_df['rps_120d'] = rps(pd_df, 120)
    pd_df['rps_250d'] = rps(pd_df, 250)

    pd_df['rsi_6d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.RSI(x['close_price'],6))
    pd_df['rsi_12d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.RSI(x['close_price'],12))

    pd_df['ma_5d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=5))
    pd_df['ma_10d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=10))
    pd_df['ma_20d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=20))
    pd_df['ma_50d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=50))
    pd_df['ma_120d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=120))
    pd_df['ma_150d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=150))
    pd_df['ma_200d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=200))
    pd_df['ma_250d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=250))

    pd_df['update_time'] = datetime.now()
    pd_df['td'] = pd_df['trade_date']
    pd_df = pd_df[['trade_date', 'stock_code', 'stock_name', 'rps_5d','rps_10d','rps_20d','rps_50d','rps_120d','rps_250d','rs','rsi_6d', 'rsi_12d','ma_5d','ma_10d', 'ma_20d', 'ma_50d', 'ma_120d','ma_150d','ma_200d','ma_250d','high_price_250d','low_price_250d','update_time', 'td']]
    spark_df = spark.createDataFrame(pd_df)
    spark_df.repartition(1).write.insertInto('stock.dwd_stock_technical_indicators_df', overwrite=True)
    spark.stop()
    print('{}：执行完毕！！！'.format(appName))



# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_technical_indicators_df.py
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