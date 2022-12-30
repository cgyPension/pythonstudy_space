import os
import sys
import time
import warnings
from datetime import date,datetime
import akshare as ak
import numpy as np
import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
from util.algorithmUtils import mt_rsi, ta_rsi

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import get_spark

# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

def get_data():
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    spark_df = spark.sql("""
        select trade_date,
           stock_code,
           stock_name,
           close_price as close,
           sum(if(change_amount>0,change_amount,0))over(partition by stock_code order by trade_date rows between 5 preceding and current row)/6/(
           sum(if(change_amount>0,change_amount,0))over(partition by stock_code order by trade_date rows between 5 preceding and current row)/6+
           abs(sum(if(change_amount<0,change_amount,0))over(partition by stock_code order by trade_date rows between 5 preceding and current row))/6)*100 as rsi_6d
        from stock.ods_dc_stock_quotes_di
        where td >= '2022-12-01'
        """)

    pd_df = spark_df.toPandas()
    pd_df['at_rsi'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta_rsi(x['close'],6))
    pd_df['mt_rsi'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: mt_rsi(x['close'],6))

    print(pd_df)
    # spark_df = spark.createDataFrame(pd_df)
    # spark_df.repartition(1).write.insertInto('stock.ods_dc_stock_tfp_di', overwrite=True)
    print('{}：执行完毕！！！'.format(appName))


# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_dc_stock_tfp_di.py all
# nohup python ods_dc_stock_tfp_di.py update 20221010 20221010 >> my.log 2>&1 &
# python ods_dc_stock_tfp_di.py all
if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        run_type = sys.argv[1]
        if run_type == 'all':
            start_date = '20210101'
            end_date = date.today().strftime('%Y%m%d')
        else:
            start_date = date.today().strftime('%Y%m%d')
            end_date = start_date
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]

    start_time = time.time()
    get_data()
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))