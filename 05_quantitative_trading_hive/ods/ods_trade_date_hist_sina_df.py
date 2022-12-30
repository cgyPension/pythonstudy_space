import os
import sys
import time
import warnings
from datetime import date,datetime
import akshare as ak
import numpy as np
import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
from pyspark.sql.types import *

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import str_pre, get_spark
from util.DBUtils import sqlalchemyUtil
warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

def get_data():
    appName = os.path.basename(__file__)
    spark = get_spark(appName)

    for i in range(1):
        try:
            # 新浪财经的股票交易日历数据
            df = ak.tool_trade_date_hist_sina()
            df = df[df['trade_date'] > pd.to_datetime('2020-01-01').date()]

            if df.empty:
                continue

            df['date_id'] = df.index
            df['update_time'] = pd.to_datetime(datetime.now())

            df = df[['date_id','trade_date','update_time']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})

            spark_df = spark.createDataFrame(df)
            spark_df.repartition(1).write.insertInto('stock.ods_trade_date_hist_sina_df', overwrite=True)
        except Exception as e:
            print(e)

    print('{}：执行完毕！！！'.format(appName))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_trade_date_hist_sina_df.py
if __name__ == '__main__':
    start_time = time.time()
    get_data()
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))
