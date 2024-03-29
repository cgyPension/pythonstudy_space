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
   # 本地模式
   spark = get_spark(appName)

    # 如果开始日期等于20210101  则start_date = 今天
   # start_date = date.today().strftime('%Y%m%d') if start_date == '20210101' else start_date

   s_date = '20210101'
   end_date = pd.to_datetime(end_date).date()

   # 增量 因为持股5日收益 要提前6个交易日 这里是下一交易日开盘买入 持股两天 在第二天的收盘卖出
   td_df = ak.tool_trade_date_hist_sina()
   daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
   daterange_df = daterange_df.iloc[-7:, 0].reset_index(drop=True)
   if daterange_df.empty:
       start_date = pd.to_datetime(start_date).date()
   else:
       start_date = pd.to_datetime(daterange_df[0]).date()

   # todo ====================================================================  f小市值+市盈率TTM+换手率+主观因子  ==================================================================
   spark.sql(
       """
       with tmp_ads_01 as (
       select *
       from stock.dwd_stock_quotes_stand_di
       where td between '%s' and '%s'
               and stock_name not rlike 'ST'
               --rlike语句匹配正则表达式 like rlike会自动把null的数据去掉 要转换
               and nvl(concept_plates,'保留null') not rlike '次新股'
               and nvl(stock_label_names,'保留null') not rlike '行业板块涨跌幅前10%%-'
               and stock_label_names rlike 'f小市值'
               and change_percent <5
               and turnover_rate between 1 and 30
               and pe_ttm between 0 and 30
       ),
       tmp_ads_02 as (
       --去除or 停复牌
       select a.*
       from tmp_ads_01 a
       left join (select trade_date,lead(trade_date,1)over(order by trade_date) as next_trade_date from stock.ods_trade_date_hist_sina_df) b
            on a.trade_date = b.trade_date
       where a.suspension_time is null
             or a.estimated_resumption_time < b.next_trade_date
       ),
       --要剔除玩所有不要股票再排序 否则排名会变动
       tmp_ads_03 as (
       select *,
              dense_rank()over(partition by td order by f_total_market_value) as dr_f_total_market_value,
              dense_rank()over(partition by td order by pe_ttm,pe) as dr_pe_ttm,
              dense_rank()over(partition by td order by turnover_rate) as dr_turnover_rate,
              dense_rank()over(partition by td order by sub_factor_score desc) as dr_sub_factor_score
       from tmp_ads_02
       ),
       tmp_ads_04 as (
                      select *,
                             'f小市值+市盈率TTM+换手率+主观因子' as stock_strategy_name,
                             -- 加上量比排序 避免排名重复
                             -- dense_rank()over(partition by td order by dr_tmv+dr_turnover_rate+dr_pe_ttm,volume_ratio_1d) as stock_strategy_ranking
                             -- 权重 
                             dense_rank()over(partition by td order by dr_f_total_market_value+dr_pe_ttm+dr_turnover_rate+dr_sub_factor_score,volume_ratio_1d) as stock_strategy_ranking
                      from tmp_ads_03
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
              rps_5d,
              rps_10d,
              rps_20d,
              rps_50d,
              rs,
              rsi_6d,
              rsi_12d,
              ma_5d,
              ma_10d,
              ma_20d,
              ma_50d,
              high_price_250d,
              low_price_250d,
              stock_label_names,
              stock_label_num,
              sub_factor_names,
              sub_factor_score,
              stock_strategy_name,
              stock_strategy_ranking,
              holding_yield_2d,
              holding_yield_5d,
              hot_rank,
              interprets,
              reason_for_lhbs,
              lhb_num_5d,
              lhb_num_10d,
              lhb_num_30d,
              lhb_num_60d,
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
              f_volume,
              f_volume_ratio_1d,
              f_volume_ratio_5d,
              f_turnover,
              f_turnover_rate,
              f_turnover_rate_5d,
              f_turnover_rate_10d,
              f_total_market_value,
              f_pe,
              f_pe_ttm,
              current_timestamp() as update_time,
              trade_date as td
       from tmp_ads_04
       where stock_strategy_ranking <=10
       order by stock_strategy_ranking
          """ % (start_date, end_date)
   ).createOrReplaceTempView('fxsz_pettm_hsl_zg')

   # todo ====================================================================  行业rps+f小市值+换手率  ==================================================================
   spark.sql(
       """
       with tmp_ads_01 as (
       select *
       from stock.dwd_stock_quotes_stand_di
       where td between '%s' and '%s'
               and stock_name not rlike 'ST'
               --rlike语句匹配正则表达式 like rlike会自动把null的数据去掉 要转换
               and nvl(concept_plates,'保留null') not rlike '次新股'
               and stock_label_names rlike '行业rps>=87'
               -- and stock_label_names rlike '概念rps>=87'
               and nvl(stock_label_names,'保留null') not rlike '行业板块涨跌幅前10%%-'
               and turnover_rate between 1 and 30
               and pe_ttm between 0 and 30
       ),
       tmp_ads_02 as (
       --去除or 停复牌
       select a.*
       from tmp_ads_01 a
       left join (select trade_date,lead(trade_date,1)over(order by trade_date) as next_trade_date from stock.ods_trade_date_hist_sina_df) b
            on a.trade_date = b.trade_date
       where a.suspension_time is null
             or a.estimated_resumption_time < b.next_trade_date
       ),
       --要剔除玩所有不要股票再排序 否则排名会变动
       tmp_ads_03 as (
       select *,
              dense_rank()over(partition by td order by f_total_market_value) as dr_f_total_market_value,
              dense_rank()over(partition by td order by turnover_rate) as dr_turnover_rate
       from tmp_ads_02
       ),
       tmp_ads_04 as (
                      select *,
                             '行业rps+f小市值+换手率' as stock_strategy_name,
                             dense_rank()over(partition by td order by dr_f_total_market_value+dr_turnover_rate,volume_ratio_1d) as stock_strategy_ranking
                      from tmp_ads_03
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
              rps_5d,
              rps_10d,
              rps_20d,
              rps_50d,
              rs,
              rsi_6d,
              rsi_12d,
              ma_5d,
              ma_10d,
              ma_20d,
              ma_50d,
              high_price_250d,
              low_price_250d,
              stock_label_names,
              stock_label_num,
              sub_factor_names,
              sub_factor_score,
              stock_strategy_name,
              stock_strategy_ranking,
              holding_yield_2d,
              holding_yield_5d,
              hot_rank,
              interprets,
              reason_for_lhbs,
              lhb_num_5d,
              lhb_num_10d,
              lhb_num_30d,
              lhb_num_60d,
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
              f_volume,
              f_volume_ratio_1d,
              f_volume_ratio_5d,
              f_turnover,
              f_turnover_rate,
              f_turnover_rate_5d,
              f_turnover_rate_10d,
              f_total_market_value,
              f_pe,
              f_pe_ttm,
              current_timestamp() as update_time,
              trade_date as td
       from tmp_ads_04
       where stock_strategy_ranking <=10
       order by stock_strategy_ranking
          """ % (start_date, end_date)
   ).createOrReplaceTempView('hyrps_fxsz_hsl')


   # todo ====================================================================  涨停+2连板+量比+换手率  ==================================================================
   spark.sql(
       """
       with tmp_ads_01 as (
       select t2.*
       from stock.dwd_stock_zt_di t1
       left join stock.dwd_stock_quotes_stand_di t2
            on t1.trade_date = t2.trade_date
            and t1.stock_code = t2.stock_code
            and t2.td between '%s' and '%s'
       where t1.td between '%s' and '%s'
               and t1.lx_sealing_nums = '2'
       ),
       tmp_ads_02 as (
       --去除or 停复牌
       select a.*
       from tmp_ads_01 a
       left join (select trade_date,lead(trade_date,1)over(order by trade_date) as next_trade_date from stock.ods_trade_date_hist_sina_df) b
            on a.trade_date = b.trade_date
       where a.suspension_time is null
             or a.estimated_resumption_time < b.next_trade_date
       ),
       tmp_ads_03 as (
                      select *,
                             dense_rank()over(partition by td order by volume_ratio_1d) as dr_volume_ratio_1d,
                             dense_rank()over(partition by td order by turnover_rate) as dr_turnover_rate
                      from tmp_ads_02
       ),
       tmp_ads_04 as (
                      select *,
                             '涨停+2连板+量比+换手率' as stock_strategy_name,
                             dense_rank()over(partition by td order by dr_volume_ratio_1d+dr_turnover_rate,dr_turnover_rate) as stock_strategy_ranking
                      from tmp_ads_03
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
              rps_5d,
              rps_10d,
              rps_20d,
              rps_50d,
              rs,
              rsi_6d,
              rsi_12d,
              ma_5d,
              ma_10d,
              ma_20d,
              ma_50d,
              high_price_250d,
              low_price_250d,
              stock_label_names,
              stock_label_num,
              sub_factor_names,
              sub_factor_score,
              stock_strategy_name,
              stock_strategy_ranking,
              holding_yield_2d,
              holding_yield_5d,
              hot_rank,
              interprets,
              reason_for_lhbs,
              lhb_num_5d,
              lhb_num_10d,
              lhb_num_30d,
              lhb_num_60d,
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
              f_volume,
              f_volume_ratio_1d,
              f_volume_ratio_5d,
              f_turnover,
              f_turnover_rate,
              f_turnover_rate_5d,
              f_turnover_rate_10d,
              f_total_market_value,
              f_pe,
              f_pe_ttm,
              current_timestamp() as update_time,
              trade_date as td
       from tmp_ads_04
       where stock_strategy_ranking <=10
       order by stock_strategy_ranking
          """ % (start_date, end_date,start_date, end_date)
   ).createOrReplaceTempView('zt_2lbl_lb_hsl')

   result_sql = '''
   select * from fxsz_pettm_hsl_zg
   union all
   select * from hyrps_fxsz_hsl
   union all
   select * from zt_2lbl_lb_hsl
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

