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

def get_data(start_date, end_date):
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    pd_df = pd.DataFrame()

    index_dic = {'000001': '上证指数', '399001': '深证成数', '399005': '中小100', '399006': '创业板指'}
    for index_code,index_name in index_dic.items():
        try:
            df = ak.index_zh_a_hist(symbol=index_code, period="daily", start_date=start_date,end_date=end_date)
            # print('ods_dc_index_di：正在处理{}...'.format(single_date))

            if df.empty:
                continue
            df.drop_duplicates(subset=['日期'], keep='last', inplace=True)
            df['index_code'] = index_code
            df['index_name'] = index_name
            df['日期'] = df['日期'].apply(lambda x: pd.to_datetime(x).date())
            df['td'] = df['日期']
            df['update_time'] = pd.to_datetime(datetime.now())

            df.rename(columns={'日期': 'trade_date', '开盘': 'open_price', '收盘': 'close_price', '最高': 'high_price',
                               '最低': 'low_price', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
                               '涨跌幅': 'change_percent',
                               '涨跌额': 'change_amount', '换手率': 'turnover_rate'}, inplace=True)
            df = df[['trade_date', 'index_code', 'index_name', 'open_price', 'close_price', 'high_price', 'low_price',
                     'volume',
                     'turnover', 'amplitude', 'change_percent', 'change_amount', 'turnover_rate','update_time','td']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})
            pd_df = pd_df.append(df)
        except Exception as e:
            print(e)


    spark_df = spark.createDataFrame(pd_df)
    spark_df.repartition(1).write.insertInto('stock.ods_dc_index_di', overwrite=True)
    spark.stop
    print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_dc_index_di.py all
# nohup python ods_dc_index_di.py update 20221010 20221010 >> my.log 2>&1 &
# python ods_dc_index_di.py all
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
    get_data(start_date, end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))
