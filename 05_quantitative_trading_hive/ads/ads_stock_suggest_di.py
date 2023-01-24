import datetime
import multiprocessing
import os
import sys
import time
import warnings
from datetime import date
import akshare as ak
import numpy as np
import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
from util.CommonUtils import get_process_num, get_spark


# bigquant经常是持股两天 第一天集合竞价开盘买，第二天收盘卖
def get_data(start_date, end_date):
   appName = os.path.basename(__file__)
   spark = get_spark(appName)

   s_date = '20210101'
   start_date,end_date = pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()
   # 增量 因为持股5日收益 要提前6个交易日 这里是下一交易日开盘买入 持股两天 在第二天的收盘卖出
   td_df = ak.tool_trade_date_hist_sina()
   daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < start_date)]
   daterange_df = daterange_df.iloc[-7:, 0].reset_index(drop=True)
   if daterange_df.empty:
       start_date = pd.to_datetime(start_date).date()
   else:
       start_date = pd.to_datetime(daterange_df[0]).date()

   # todo ====================================================================  f小市值+市盈率TTM+换手率+主观因子  ==================================================================
   spark.sql(
       """

          """ % (start_date, end_date)
   ).createOrReplaceTempView('fxsz_pettm_hsl_zg')

   result_sql = '''
   select * from fxsz_pettm_hsl_zg
   '''
   result_df = spark.sql(result_sql)

   try:
       # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
       result_df.repartition(1).write.insertInto('stock.ads_stock_suggest_di', overwrite=True)  # 如果执行不存在这个表，会报错
       spark.stop
   except Exception as e:
       print(e)
   print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_stock_suggest_di.py all
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_stock_suggest_di.py update 20221101 20221226
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_stock_suggest_di.py update 20221110
# nohup ads_stock_suggest_di.py update 20221010 20221010 >> my.log 2>&1 &
# nohup spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_stock_suggest_di.py all >> my.log 2>&1 &
# python ads_stock_suggest_di.py update 20221110 20221110
if __name__ == '__main__':
    process_num = get_process_num()
    start_date = date.today().strftime('%Y%m%d')
    end_date = start_date
    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        run_type = sys.argv[1]
        if run_type == 'all':
            start_date = '20210101'
            end_date
        else:
            start_date
            end_date
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]

    start_time = time.time()
    get_data(start_date, end_date)
    end_time = time.time()
    print('程序运行时间：{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))

