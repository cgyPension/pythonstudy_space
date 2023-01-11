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


def get_data():
   appName = os.path.basename(__file__)
   spark = get_spark(appName)

   spark_df = spark.sql(
       """
with t1_2_3 as (
select trade_date,
       stock_strategy_name||'_'||2||'_'||3 as strategy_id,
       stock_strategy_name,
       2 as hold_day,
       3 as hold_n,
       sum(holding_yield_2d) as yield_1d,
       count(1) as ct,
       sum(if(holding_yield_2d>0,1,0)) as sl
from stock.ads_stock_suggest_di
where stock_strategy_ranking <=3
group by trade_date,stock_strategy_name
    ),
t1_5_3 as (
select trade_date,
       stock_strategy_name||'_'||5||'_'||3 as strategy_id,
       stock_strategy_name,
       5 as hold_day,
       3 as hold_n,
       sum(holding_yield_5d) as yield_1d,
       count(1) as ct,
       sum(if(holding_yield_5d>0,1,0)) as sl
from stock.ads_stock_suggest_di
where stock_strategy_ranking <=3
group by trade_date,stock_strategy_name
    )
select trade_date,
       strategy_id,
       stock_strategy_name,
       hold_day,
       hold_n,
       sum(yield_1d)over(partition by stock_strategy_name order by trade_date) as yield_td,
       round((pow(1 + sum(yield_1d)over(partition by stock_strategy_name order by trade_date)/100, 250 / sum(ct)over(partition by stock_strategy_name order by trade_date)) - 1)*100, 2) as annual_yield,
       sum(sl)over(partition by stock_strategy_name order by trade_date)/sum(ct)over(partition by stock_strategy_name order by trade_date)*100 as wp,
       yield_1d,
       sum(yield_1d)over(partition by stock_strategy_name order by trade_date rows between 6 preceding and current row) as yield_7d,
       sum(yield_1d)over(partition by stock_strategy_name order by trade_date rows between 21 preceding and current row) as yield_22d,
       sum(yield_1d)over(partition by stock_strategy_name order by trade_date rows between 65 preceding and current row) as yield_66d,
       current_timestamp() as update_time,
       trade_date as td
from t1_2_3
union all
select trade_date,
       strategy_id,
       stock_strategy_name,
       hold_day,
       hold_n,
       sum(yield_1d)over(partition by stock_strategy_name order by trade_date) as yield_td,
       round((pow(1 + sum(yield_1d)over(partition by stock_strategy_name order by trade_date)/100, 250 / sum(ct)over(partition by stock_strategy_name order by trade_date)) - 1)*100, 2) as annual_yield,
       sum(sl)over(partition by stock_strategy_name order by trade_date)/sum(ct)over(partition by stock_strategy_name order by trade_date)*100 as wp,
       yield_1d,
       sum(yield_1d)over(partition by stock_strategy_name order by trade_date rows between 6 preceding and current row) as yield_7d,
       sum(yield_1d)over(partition by stock_strategy_name order by trade_date rows between 21 preceding and current row) as yield_22d,
       sum(yield_1d)over(partition by stock_strategy_name order by trade_date rows between 65 preceding and current row) as yield_66d,
       current_timestamp() as update_time,
       trade_date as td
from t1_5_3
       """
   )

   try:
       # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
       spark_df.repartition(1).write.insertInto('stock.ads_strategy_yield_di', overwrite=True)  # 如果执行不存在这个表，会报错
       spark.stop
   except Exception as e:
       print(e)
   print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_strategy_yield_di.py all
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_strategy_yield_di.py update 20221101 20221226
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_strategy_yield_di.py update 20221110
# nohup ads_strategy_yield_di.py update 20221010 20221010 >> my.log 2>&1 &
# nohup spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_strategy_yield_di.py all >> my.log 2>&1 &
# python ads_strategy_yield_di.py update 20221110 20221110
if __name__ == '__main__':
    start_time = time.time()
    get_data()
    end_time = time.time()
    print('程序运行时间：{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))

