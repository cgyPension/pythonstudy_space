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
from util.algorithmUtils import rps, plate_rps
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
            with t1 as (
                        select trade_date,
                               industry_plate_code as plate_code,
                               industry_plate as plate_name,
                               open_price,
                               close_price,
                               high_price,
                               low_price,
                               change_percent,
                               change_amount,
                               volume,
                               turnover,
                               amplitude,
                               turnover_rate,
                               max(high_price)over(partition by industry_plate order by trade_date rows between 249 preceding and current row) as high_price_250d,
                               min(low_price)over(partition by industry_plate order by trade_date rows between 249 preceding and current row) as low_price_250d
                        from stock.ods_dc_stock_industry_plate_hist_di
                        union all
                        select trade_date,
                               concept_plate_code as plate_code,
                               concept_plate as plate_name,
                               open_price,
                               close_price,
                               high_price,
                               low_price,
                               change_percent,
                               change_amount,
                               volume,
                               turnover,
                               amplitude,
                               turnover_rate,
                               max(high_price)over(partition by concept_plate order by trade_date rows between 249 preceding and current row) as high_price_250d,
                               min(low_price)over(partition by concept_plate order by trade_date rows between 249 preceding and current row) as low_price_250d
                        from stock.ods_dc_stock_concept_plate_hist_di
            )
            select * from t1 order by trade_date
        """)
    pd_df = spark_df.toPandas()

    pd_df['rps_5d'] = plate_rps(pd_df, 5)
    pd_df['rps_10d'] = plate_rps(pd_df, 10)
    pd_df['rps_15d'] = plate_rps(pd_df, 15)
    pd_df['rps_20d'] = plate_rps(pd_df, 20)
    pd_df['rps_50d'] = plate_rps(pd_df, 50)

    pd_df['ma_5d'] = pd_df.groupby('plate_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=5))
    pd_df['ma_10d'] = pd_df.groupby('plate_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=10))
    pd_df['ma_20d'] = pd_df.groupby('plate_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=20))
    pd_df['ma_50d'] = pd_df.groupby('plate_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=50))
    pd_df['ma_120d'] = pd_df.groupby('plate_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=120))
    pd_df['ma_150d'] = pd_df.groupby('plate_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=150))
    pd_df['ma_200d'] = pd_df.groupby('plate_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=200))
    pd_df['ma_250d'] = pd_df.groupby('plate_code',group_keys=False).apply(lambda x: ta.MA(x['close_price'],timeperiod=250))

    spark.createDataFrame(pd_df).createOrReplaceTempView('tmp_dim_plate')
    r_df = spark.sql("""
            with t1 as (
             select *,
                    if(sort_array(array(rps_5d,rps_10d,rps_15d,rps_20d))[1]>=90,1,0) as rps_red
             from tmp_dim_plate
             ),
             t2 as (
             select *,
                    --20日内rps首次三线翻红
                   if(rps_red=1 and sum(rps_red)over(partition by plate_code order by trade_date rows between 19 preceding and current row)=1,1,0) as is_rps_red
             from t1
             )
             select trade_date,
                    plate_code,
                    plate_name,
                    open_price,
                    close_price,
                    high_price,
                    low_price,
                    change_percent,
                    change_amount,
                    volume,
                    turnover,
                    amplitude,
                    turnover_rate,
                    rps_5d,
                    rps_10d,
                    rps_15d,
                    rps_20d,
                    rps_50d,
                    is_rps_red,
                    ma_5d,
                    ma_10d,
                    ma_20d,
                    ma_50d,
                    ma_120d,
                    ma_150d,
                    ma_200d,
                    ma_250d,
                    high_price_250d,
                    low_price_250d,
                    current_timestamp() as update_time,
                    trade_date as td
             from t2
             order by trade_date,is_rps_red desc,(rps_5d+rps_10d+rps_15d+rps_20d) desc,change_percent desc
        """)
    r_df.repartition(1).write.insertInto('stock.dim_plate_df', overwrite=True)
    spark.stop()
    print('{}：执行完毕！！！'.format(appName))



# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dim/dim_plate_df.py
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