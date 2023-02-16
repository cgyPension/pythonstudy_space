import datetime
import os
import sys
import time
import warnings
from datetime import date,datetime
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
from util.CommonUtils import get_process_num, get_spark, get_trade_date_nd
from util.DBUtils import clickhouseUtil

def get_data(start_date, end_date):
   appName = os.path.basename(__file__)
   spark = get_spark(appName)
   start_date,end_date = pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()
   # 增量 因为持股5日收益 要提前6个交易日 这里是下一交易日开盘买入 持股两天 在第二天的收盘卖出
   start_date_7 = get_trade_date_nd(start_date,-7)
   ck = clickhouseUtil()
   try:
       ods_trade_date_hist_sina_df = spark.sql('''select * from stock.ods_trade_date_hist_sina_df''')
       ods_trade_date_df = ods_trade_date_hist_sina_df.toPandas()
       ods_trade_date_df['update_time'] = pd.to_datetime(datetime.now())
       ck.execute('''truncate table ods_trade_date_hist_sina_df''')
       ck.execute_insert('ods_trade_date_hist_sina_df', ods_trade_date_df)

       dim_plate_df = spark.sql('''select * from stock.dim_plate_df where td  between '%s' and '%s' ''' % (start_date, end_date))
       plate_df = dim_plate_df.toPandas().drop('td', axis=1)
       plate_df['update_time'] = pd.to_datetime(datetime.now())
       partition_plate_df=ck.execute_query('''select partition from system.parts where table='dim_plate_df' and partition between '%s' and '%s' '''%(start_date, end_date))
       for p in partition_plate_df['partition']:
           ck.execute('''alter table dim_plate_df drop partition '%s' ''' % p)
       ck.execute_insert('dim_plate_df', plate_df)

       # 不能跑全量 要半年跑
       dwd_stock_quotes_stand_di = spark.sql('''select * from stock.dwd_stock_quotes_stand_di where td  between '%s' and '%s' ''' % (start_date_7, end_date))
       dwd_stock_stand_df = dwd_stock_quotes_stand_di.toPandas().drop('td', axis=1)
       dwd_stock_stand_df['update_time'] = pd.to_datetime(datetime.now())
       partition_dwd_stock_stand_df=ck.execute_query('''select partition from system.parts where table='dwd_stock_quotes_stand_di' and partition between '%s' and '%s' '''%(start_date_7, end_date))
       for p in partition_dwd_stock_stand_df['partition']:
           ck.execute('''alter table dwd_stock_quotes_stand_di drop partition '%s' ''' % p)
       ck.execute_insert('dwd_stock_quotes_stand_di', dwd_stock_stand_df)

       spark.stop
   except Exception as e:
       print(e)
   print('{}：执行完毕！！！'.format(appName))


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/export_clickhouse.py all
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/export_clickhouse.py update 20221101 20221226
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/export_clickhouse.py update 20210101 20210201
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

