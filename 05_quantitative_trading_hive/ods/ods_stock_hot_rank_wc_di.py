import os
import sys
import time
import warnings
from datetime import date,datetime
import random
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

# 接口问题 全量的时候要分开一个月一个月跑
def get_data(start_date, end_date):
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    pd_df = pd.DataFrame()

    daterange = pd.date_range(start_date, end_date)
    # df = ak.tool_trade_date_hist_sina()
    # df = df[(df['trade_date'] >= start_date)&(df['trade_date'] <= end_date)]
    # daterange = df['trade_date']


    for single_date in daterange:
        # time.sleep(random.randint(1,3))
        try:
            # print('single_date：',single_date.strftime("%Y%m%d"))
            #  东方财富网-行情中心-涨停板行情-涨停股池
            df = ak.stock_hot_rank_wc(date=single_date.strftime("%Y%m%d"))
            # print('ods_dc_stock_tfp_di：正在处理{}...'.format(single_date))
            # print("xxxxdf:", df)
            if df.empty:
                continue
            df.drop_duplicates(subset=['股票代码'], keep='last', inplace=True)
            df['stock_code'] = df['股票代码'].apply(str_pre)
            df['trade_date'] = pd.to_datetime(single_date).date()
            df['td'] = df['trade_date']
            df['update_time'] = pd.to_datetime(datetime.now())

            df.rename(columns={'股票简称':'stock_name','现价':'new_price','涨跌幅':'change_percent','个股热度':'hot','序号':'hot_rank'}, inplace=True)
            df = df[['trade_date','stock_code','stock_name','new_price','change_percent','hot','hot_rank','update_time','td']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})
            # print("xxxxdf:",df)
            pd_df = pd_df.append(df)
        except Exception as e:
            print(e)
    # print("pd_df:", pd_df)
    spark_df = spark.createDataFrame(pd_df)
    spark_df.repartition(1).write.insertInto('stock.ods_stock_hot_rank_wc_di', overwrite=True)
    print('{}：执行完毕！！！'.format(appName))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_stock_hot_rank_wc_di.py all
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_stock_hot_rank_wc_di.py update 20210101 20211231
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_stock_hot_rank_wc_di.py update 20210101 20210630
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_stock_hot_rank_wc_di.py update 20210701 20211231
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_stock_hot_rank_wc_di.py update 20220101 20221230
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_stock_hot_rank_wc_di.py update 20220101 20221230
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
    get_data(start_date, end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))
