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
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
from util.CommonUtils import get_process_num, get_spark


# bigquant经常是持股两天 第一天集合竞价开盘买，第二天收盘卖
def get_data(start_date, end_date):
   appName = os.path.basename(__file__)
   # 本地模式
   spark = get_spark(appName)

    # 如果开始日期等于20210101  则start_date = 今天
   start_date = date.today().strftime('%Y%m%d') if start_date == '20210101' else start_date

   s_date = '20210101'
   end_date = pd.to_datetime(end_date).date()

   td_df = ak.tool_trade_date_hist_sina()
   daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
   daterange_df = daterange_df.iloc[-5:, 0].reset_index(drop=True)
   # 增量 要前5个交易日 但是要预够假期 不够取最靠近前5的交易日
   if daterange_df.empty:
       start_date = pd.to_datetime(start_date).date()
   else:
       start_date = pd.to_datetime(daterange_df[0]).date()

   spark_df = spark.sql(
   """
with tmp_ads_01 as (
select *,
       dense_rank()over(partition by td order by total_market_value) as dr_tmv,
       dense_rank()over(partition by td order by turnover_rate) as dr_turnover_rate,
       dense_rank()over(partition by td order by pe_ttm) as dr_pe_ttm
from stock.dwd_stock_quotes_di
where td between '%s' and '%s'
        -- 剔除京股
        and substr(stock_code,1,2) != 'bj'
        -- 剔除涨停 涨幅<5
        and change_percent <5
        and turnover_rate between 1 and 30
        and stock_label_names rlike '小市值'
),
tmp_ads_02 as (
               select *,
                      '小市值+换手率+市盈率TTM' as stock_strategy_name,
                      dense_rank()over(partition by td order by dr_tmv+dr_turnover_rate+dr_pe_ttm) as stock_strategy_ranking
               from tmp_ads_01
               where suspension_time is null
                       or estimated_resumption_time < date_add('%s',1)
                       or pe_ttm is null
                       or pe_ttm <=30
               order by stock_strategy_ranking
)
select trade_date,
       stock_code,
       stock_name,
       open_price,
       close_price,
       high_price,
       low_price,
       volume,
       volume_ratio_1d,
       volume_ratio_5d,
       turnover,
       amplitude,
       change_percent,
       change_amount,
       turnover_rate,
       turnover_rate_5d,
       turnover_rate_10d,
       total_market_value,
       industry_plate,
       concept_plates,
       pe,
       pe_ttm,
       pb,
       ps,
       ps_ttm,
       dv_ratio,
       dv_ttm,
       net_profit,
       net_profit_yr,
       total_business_income,
       total_business_income_yr,
       business_fee,
       sales_fee,
       management_fee,
       finance_fee,
       total_business_fee,
       business_profit,
       total_profit,
       ps_business_cash_flow,
       return_on_equity,
       npadnrgal,
       net_profit_growth_rate,
       interprets,
       reason_for_lhbs,
       lhb_num_5d,
       lhb_num_10d,
       lhb_num_30d,
       lhb_num_60d,
       ma_5d,
       ma_10d,
       ma_20d,
       ma_30d,
       ma_60d,
       stock_label_names,
       stock_label_num,
       sub_factor_names,
       sub_factor_score,
       stock_strategy_name,
       stock_strategy_ranking,
       holding_yield_2d,
       holding_yield_5d,
       current_timestamp() as update_time,
       trade_date as td
from tmp_ads_02
where stock_strategy_ranking <=10
   """ % (start_date, end_date, end_date))


   try:
       # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
       spark_df.repartition(1).write.insertInto('stock.ads_stock_suggest_di', overwrite=True)  # 如果执行不存在这个表，会报错
   except Exception as e:
       print(e)
   print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_stock_suggest_di.py all
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_stock_suggest_di.py update
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ads/ads_stock_suggest_di.py update 20221110
# nohup ads_stock_suggest_di.py update 20221010 20221010 >> my.log 2>&1 &
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

