import os
import sys
import time
import warnings
from datetime import date
import akshare as ak
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

def get_data(start_date, end_date):
    try:
        appName = os.path.basename(__file__)
        # 本地模式
        spark = get_spark(appName)
        s_date = '20210101'
        end_date = pd.to_datetime(end_date).date()

        # 增量 因为ma_60d 要提前60个交易日 那个持股5日后收益要覆盖前5日
        td_df = ak.tool_trade_date_hist_sina()
        daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
        daterange_75 = daterange_df.iloc[-75:, 0].reset_index(drop=True)
        if daterange_75.empty:
            start_date_75 = pd.to_datetime(start_date).date()
        else:
            start_date_75 = pd.to_datetime(daterange_75[0]).date()

        daterange_10 = daterange_df.iloc[-10:, 0].reset_index(drop=True)
        if daterange_10.empty:
            start_date_10 = pd.to_datetime(start_date).date()
        else:
            start_date_10 = pd.to_datetime(daterange_10[0]).date()
        

        spark.sql("""
with t3 as (
select *,
      if(lead(announcement_date,1)over(partition by stock_code order by announcement_date) is null,'9999-12-01',lead(announcement_date,1)over(partition by stock_code order by announcement_date)) as end_date
from stock.ods_stock_lrb_em_di
),
t4 as (
select *,
      if(lead(announcement_date,1)over(partition by stock_code order by announcement_date) is null,'9999-12-01',lead(announcement_date,1)over(partition by stock_code order by announcement_date)) as end_date
from stock.ods_financial_analysis_indicator_di
),
t5 as (
-- 有同一天上榜两次的情况,把原因凭借其他金额类字段去掉 ；拼接字段名加s
select trade_date,
       stock_code,
       concat_ws(';',collect_list(interpret)) as interprets,
       concat_ws(';',collect_list(reason_for_lhb)) as reason_for_lhbs
from stock.ods_stock_lhb_detail_em_di
where td between '%s' and '%s'
      group by td,trade_date,stock_code
),
plate_1 as (
select *
from (
       select stock_code,
              industry_plate,
              row_number()over(partition by stock_code order by trade_date desc) as rk
       from stock.dim_dc_stock_plate_di
         )
where rk = 1
),
tmp_dim_industry_rps as (
select trade_date,
       industry_plate,
       close_price,
       pr_industry_cp,
       if((rps_5d >=87 and rps_10d >=87 and rps_20d >=87) or 
          (rps_5d >=87 and rps_10d >=87 and rps_50d >=87) or 
          (rps_5d >=87 and rps_20d >=87 and rps_50d >=87) or 
          (rps_10d >=87 and rps_20d >=87 and rps_50d >=87),1,0) as is_industry_rps
from(
     select trade_date,
            industry_plate,
            close_price,
            pr_industry_cp,
           percent_rank()over(partition by trade_date order by rps_5d asc)*100 as rps_5d,
           percent_rank()over(partition by trade_date order by rps_10d asc)*100 as rps_10d,
           percent_rank()over(partition by trade_date order by rps_20d asc)*100 as rps_20d,
           percent_rank()over(partition by trade_date order by rps_50d asc)*100 as rps_50d
     from (select trade_date,
            industry_plate,
            close_price,
            percent_rank()over(partition by trade_date order by change_percent desc)*100 as pr_industry_cp,
            if(lag(close_price,4)over(partition by industry_plate order by trade_date) is null,null,(close_price/lag(close_price,4)over(partition by industry_plate order by trade_date)-1)*100) as rps_5d,
            if(lag(close_price,9)over(partition by industry_plate order by trade_date) is null,null,(close_price/lag(close_price,9)over(partition by industry_plate order by trade_date)-1)*100) as rps_10d,
            if(lag(close_price,19)over(partition by industry_plate order by trade_date) is null,null,(close_price/lag(close_price,19)over(partition by industry_plate order by trade_date)-1)*100) as rps_20d,
            if(lag(close_price,59)over(partition by industry_plate order by trade_date) is null,null,(close_price/lag(close_price,49)over(partition by industry_plate order by trade_date)-1)*100) as rps_50d
     from stock.ods_dc_stock_industry_plate_hist_di
     where td between '%s' and '%s')
    )
)
select t1.trade_date,
       t1.stock_code,
       t1.stock_name,
       t1.open_price,
       t1.close_price,
       t1.high_price,
       t1.low_price,
       t1.volume,
       if(lag(t1.volume,1)over(partition by t1.stock_code order by t1.trade_date) is null or t1.volume is null,null,t1.volume/lag(t1.volume,1,null)over(partition by t1.stock_code order by t1.trade_date)) as volume_ratio_1d,
       if(lag(t1.volume,4)over(partition by t1.stock_code order by t1.trade_date) is null or t1.volume is null,null,t1.volume/avg(t1.volume)over(partition by t1.stock_code order by t1.trade_date rows between 4 preceding and current row)) as volume_ratio_5d,
       t1.turnover,
       t1.amplitude,
       t1.change_percent,
       t1.change_amount,
       t1.turnover_rate,
       if(lag(t1.close_price,4)over(partition by t1.stock_code order by t1.trade_date) is null,null,avg(t1.turnover_rate)over(partition by t1.stock_code order by t1.trade_date rows between 4 preceding and current row)) as turnover_rate_5d,
       if(lag(t1.close_price,9)over(partition by t1.stock_code order by t1.trade_date) is null,null,avg(t1.turnover_rate)over(partition by t1.stock_code order by t1.trade_date rows between 9 preceding and current row)) as turnover_rate_10d,
       t2.total_market_value,
       plate_1.industry_plate,
       plate_2.concept_plates,
       tmp_dim_industry_rps.pr_industry_cp,
       tmp_dim_industry_rps.is_industry_rps,
       plate_2.is_concept_rps,
       t2.pe,
       t2.pe_ttm,
       t2.pb,
       t2.ps,
       t2.ps_ttm,
       t2.dv_ratio,
       t2.dv_ttm,
       t3.net_profit,
       t3.net_profit_yr,
       t3.total_business_income,
       t3.total_business_income_yr,
       t3.business_fee,
       t3.sales_fee,
       t3.management_fee,
       t3.finance_fee,
       t3.total_business_fee,
       t3.business_profit,
       t3.total_profit,
       t4.ps_business_cash_flow,
       t4.return_on_equity,
       t4.npadnrgal,
       t4.net_profit_growth_rate,
       t5.interprets,
       t5.reason_for_lhbs,
       sum(if(t5.reason_for_lhbs is not null,1,0))over(partition by t1.stock_code order by t1.trade_date rows between 4 preceding and current row) as lhb_num_5d,
       sum(if(t5.reason_for_lhbs is not null,1,0))over(partition by t1.stock_code order by t1.trade_date rows between 9 preceding and current row) as lhb_num_10d,
       sum(if(t5.reason_for_lhbs is not null,1,0))over(partition by t1.stock_code order by t1.trade_date rows between 29 preceding and current row) as lhb_num_30d,
       sum(if(t5.reason_for_lhbs is not null,1,0))over(partition by t1.stock_code order by t1.trade_date rows between 59 preceding and current row) as lhb_num_60d,
       t8.hot_rank,
       -- 这里是下一交易日开盘买入 持股两天 在第二天的收盘卖出
       -- 收益率公式优化 (后/前)-1
       -- if(lead(t1.close_price,2)over(partition by t1.stock_code order by t1.trade_date) is null or t1.open_price = 0,null,(lead(t1.close_price,2)over(partition by t1.stock_code order by t1.trade_date)-lead(t1.open_price,1)over(partition by t1.stock_code order by t1.trade_date))/lead(t1.open_price,1)over(partition by t1.stock_code order by t1.trade_date))*100 as holding_yield_2d,
       if(lead(t1.close_price,2)over(partition by t1.stock_code order by t1.trade_date) is null or t1.open_price = 0,null,(lead(t1.close_price,2)over(partition by t1.stock_code order by t1.trade_date)/lead(t1.open_price,1)over(partition by t1.stock_code order by t1.trade_date)-1)*100) as holding_yield_2d,
       if(lead(t1.close_price,5)over(partition by t1.stock_code order by t1.trade_date) is null or t1.open_price = 0,null,(lead(t1.close_price,5)over(partition by t1.stock_code order by t1.trade_date)/lead(t1.open_price,1)over(partition by t1.stock_code order by t1.trade_date)-1)*100) as holding_yield_5d,
       t6.lx_sealing_nums,
       t7.selection_reason,
       t9.rps_5d,
       t9.rps_10d,
       t9.rps_20d,
       t9.rps_50d,
       t9.rs,
       t9.rsi_6d,
       t9.rsi_12d,
       t9.ma_5d,
       t9.ma_10d,
       t9.ma_20d,
       t9.ma_50d,
       t9.ma_120d,
       t9.ma_150d,
       t9.ma_200d,
       t9.ma_250d,
       t9.high_price_250d,
       t9.low_price_250d
from stock.ods_dc_stock_quotes_di t1
left join stock.ods_lg_indicator_di t2
        on t1.trade_date = t2.trade_date
            and t1.stock_code = t2.stock_code
            and t2.td between '%s' and '%s'
-- 区间关联左闭右开
left join t3
        on t1.trade_date >= t3.announcement_date and t1.trade_date <t3.end_date
            and t1.stock_code = t3.stock_code
-- 区间关联左闭右开
left join  t4
       on t1.trade_date >= t4.announcement_date and t1.trade_date <t4.end_date
         and t1.stock_code = t4.stock_code
-- 有同一天上榜两次的情况
left join t5
       on t1.trade_date = t5.trade_date
            and t1.stock_code = t5.stock_code
left join stock.ods_stock_zt_pool_di t6
       on t1.trade_date = t6.trade_date
            and t1.stock_code = t6.stock_code
            and t6.td between '%s' and '%s'
left join stock.ods_stock_strong_pool_di t7
       on t1.trade_date = t7.trade_date
            and t1.stock_code = t7.stock_code
            and t7.td between '%s' and '%s'
left join stock.ods_stock_hot_rank_wc_di t8
       on t1.trade_date = t8.trade_date
            and t1.stock_code = t8.stock_code
            and t8.td between '%s' and '%s'
left join stock.dwd_stock_technical_indicators_df t9
       on t1.trade_date = t9.trade_date
            and t1.stock_code = t9.stock_code
            and t9.td between '%s' and '%s'
left join plate_1
     on t1.stock_code = plate_1.stock_code
left join stock.dim_dc_stock_plate_di plate_2
        on t1.trade_date = plate_2.trade_date
        and t1.stock_code = plate_2.stock_code
        and plate_2.td between '%s' and '%s'
left join tmp_dim_industry_rps
     on t1.trade_date = tmp_dim_industry_rps.trade_date
        and plate_1.industry_plate = tmp_dim_industry_rps.industry_plate
where t1.td between '%s' and '%s'
        """% (start_date_75,end_date,start_date_75,end_date,start_date_75,end_date,start_date_75,end_date,start_date_75,end_date,start_date_75,end_date,start_date_75,end_date,start_date_75,end_date,start_date_75,end_date)).createOrReplaceTempView('tmp_dwd_01')

        spark.sql("""
--因为股票交易日不连续，不能用日期等差的方式算连续
with tmp_lx_ljqs_01 as (
select trade_date,
       stock_code,
       date_id
from(
      select t1.trade_date,
       t1.stock_code,
       t1.stock_name,
       t1.change_percent,
       if(lag(t1.volume,1)over(partition by t1.stock_code order by t1.trade_date) is null or t1.volume is null,null,t1.volume/lag(t1.volume,1,null)over(partition by t1.stock_code order by t1.trade_date)) as volume_ratio_1d,
       t2.date_id
from stock.ods_dc_stock_quotes_di t1
left join stock.ods_trade_date_hist_sina_df t2
        on t1.trade_date = t2.trade_date
        )
where volume_ratio_1d>1
    and change_percent>0
),
tmp_lx_ljqs_02 as (
select *,
       date_id-rank()over(partition by stock_code order by date_id) as flag
from tmp_lx_ljqs_01
),
tmp_lx_ljqs_03 as (
select *,
       '连续'||count(1)over(partition by stock_code,flag order by trade_date)||'天量价齐升' as lx_ljqs_days
from tmp_lx_ljqs_02),
tmp_lx_ljqd_01 as (
select trade_date,
       stock_code,
       date_id
from(
      select t1.trade_date,
       t1.stock_code,
       t1.change_percent,
       if(lag(t1.volume,1)over(partition by t1.stock_code order by t1.trade_date) is null or t1.volume is null,null,t1.volume/lag(t1.volume,1,null)over(partition by t1.stock_code order by t1.trade_date)) as volume_ratio_1d,
       t2.date_id
from stock.ods_dc_stock_quotes_di t1
left join stock.ods_trade_date_hist_sina_df t2
        on t1.trade_date = t2.trade_date
        )
where volume_ratio_1d<1
    and change_percent<0
),
tmp_lx_ljqd_02 as (
select trade_date,
       stock_code,
       date_id-rank()over(partition by stock_code order by date_id) as flag
from tmp_lx_ljqd_01)
,
tmp_lx_ljqd_03 as (
select *,
       '连续'||count(1)over(partition by stock_code,flag order by trade_date)||'天量价齐跌' as lx_ljqd_days
from tmp_lx_ljqd_02
)
select t1.*,
       if((t1.rps_5d >=87 and t1.rps_10d >=87 and t1.rps_20d >=87) or 
          (t1.rps_5d >=87 and t1.rps_10d >=87 and t1.rps_50d >=87) or 
          (t1.rps_5d >=87 and t1.rps_20d >=87 and t1.rps_50d >=87) or 
          (t1.rps_10d >=87 and t1.rps_20d >=87 and t1.rps_50d >=87),1,0) as is_stock_rps,
       if(t1.pr_industry_cp <=10,1,0) as is_pr_industry_cp_10,
       if(t1.interprets rlike '买',1,0) as is_lhb_buy,
       if(t1.interprets rlike '卖',1,0) as is_lhb_sell,
       if(t1.lhb_num_60d>0,1,0) is_lhb_60d,
       if(percent_rank()over(partition by t1.trade_date order by t1.total_market_value)<0.333,1,0) as is_min_market_value,
       if(percent_rank()over(partition by t1.trade_date order by t1.total_market_value) >=0.333 and percent_rank()over(partition by t1.trade_date order by t1.total_market_value) <0.666,1,0) as is_mid_market_value,
       if(percent_rank()over(partition by t1.trade_date order by t1.total_market_value) >=0.666,1,0) as is_max_market_value,
       if(t1.concept_plates rlike '预盈预增',1,0) as is_pre_profit_increase,
       if(t1.concept_plates rlike '预亏预减',1,0) as is_pre_deficiency_decrease,
       if(t1.lx_sealing_nums is not null,1,0) as is_zt,
       if(t1.selection_reason is not null,1,0) as is_strong,
       t2.lx_ljqs_days,
       t3.lx_ljqd_days,
       if(t1.rsi_6d<=20,1,0) as is_rsi_6d_over_sell,
       if(t1.rsi_6d>=80,1,0) as is_rsi_6d_over_buy,
       if(t1.rsi_6d>t1.rsi_12d and lag(t1.rsi_6d,1)over(partition by t1.stock_code order by t1.trade_date)<lag(t1.rsi_12d,1)over(partition by t1.stock_code order by t1.trade_date),1,0) as is_rsi_6_bigger_12,
       if(t1.rsi_6d<t1.rsi_12d and lag(t1.rsi_6d,1)over(partition by t1.stock_code order by t1.trade_date)>lag(t1.rsi_12d,1)over(partition by t1.stock_code order by t1.trade_date),1,0) as is_rsi_6_smaller_12
from tmp_dwd_01 t1
left join tmp_lx_ljqs_03 t2
        on t1.trade_date = t2.trade_date
            and t1.stock_code = t2.stock_code
left join tmp_lx_ljqd_03 t3
        on t1.trade_date = t3.trade_date
            and t1.stock_code = t3.stock_code
        """).createOrReplaceTempView('tmp_dwd_02')

        spark_df = spark.sql("""
select t1.trade_date,
       t1.stock_code,
       t1.stock_name,
       t1.open_price,
       t1.close_price,
       t1.high_price,
       t1.low_price,
       t1.volume,
       t1.volume_ratio_1d,
       t1.volume_ratio_5d,
       t1.turnover,
       t1.amplitude,
       t1.change_percent,
       t1.change_amount,
       t1.turnover_rate,
       t1.turnover_rate_5d,
       t1.turnover_rate_10d,
       t1.total_market_value,
       t1.industry_plate,
       t1.concept_plates,
       t1.rps_5d,
       t1.rps_10d,
       t1.rps_20d,
       t1.rps_50d,
       t1.rs,
       t1.rsi_6d,
       t1.rsi_12d,
       t1.ma_5d,
       t1.ma_10d,
       t1.ma_20d,
       t1.ma_50d,
       t1.ma_120d,
       t1.ma_150d,
       t1.ma_200d,
       t1.ma_250d,
       t1.high_price_250d,
       t1.low_price_250d,
       concat_ws(',',if(t1.is_min_market_value=1,'小市值',null),
                     if(t1.is_mid_market_value=1,'中市值',null),
                     if(t1.is_max_market_value=1,'大市值',null),
                     if(t1.is_stock_rps=1,'个股rps>=87',null),
                     if(t1.is_industry_rps=1,'行业rps>=87',null),
                     if(t1.is_concept_rps=1,'概念rps>=87',null),
                     if(t1.is_pr_industry_cp_10=1,'行业板块涨跌幅前10%%-',null),
                     if(t1.is_lhb_buy=1,'当天龙虎榜_买',null),
                     if(t1.is_lhb_sell=1,'当天龙虎榜_卖-',null),
                     if(t1.is_lhb_60d=1,'最近60天龙虎榜',null),
                     if(t1.is_pre_profit_increase=1,'预盈预增',null),
                     if(t1.is_pre_deficiency_decrease=1,'预亏预减-',null),
                     if(t1.is_zt=1,'当天涨停',null),
                     if(t1.is_strong=1,'当天强势股',null),
                     t1.lx_ljqs_days,
                     t1.lx_ljqd_days,
                     if(t1.is_rsi_6d_over_sell=1,'rsi_6d超卖',null),
                     if(t1.is_rsi_6d_over_buy=1,'rsi_6d超买-',null),
                     if(t1.is_rsi_6_bigger_12=1,'rsi_6_12金叉',null),
                     if(t1.is_rsi_6_smaller_12=1,'rsi_6_12死叉-',null)
       ) as stock_label_names,
       (
        nvl(t1.is_min_market_value,0)+
        nvl(t1.is_mid_market_value,0)+
        nvl(t1.is_max_market_value,0)+
        nvl(t1.is_stock_rps,0)+
        nvl(t1.is_industry_rps,0)+
        nvl(t1.is_concept_rps,0)+
        nvl(t1.is_pr_industry_cp_10,0)+
        nvl(t1.is_lhb_buy,0)+
        nvl(t1.is_lhb_sell,0)+
        nvl(t1.is_lhb_60d,0)+
        nvl(t1.is_pre_profit_increase,0)+
        nvl(t1.is_pre_deficiency_decrease,0)+
        nvl(t1.is_zt,0)+
        nvl(t1.is_strong,0)+
        if(t1.lx_ljqs_days is not null,1,0)+
        if(t1.lx_ljqd_days is not null,1,0)+
        nvl(t1.is_rsi_6d_over_sell,0)+
        nvl(t1.is_rsi_6d_over_buy,0)+
        nvl(t1.is_rsi_6_bigger_12,0)+
        nvl(t1.is_rsi_6_smaller_12,0)
        ) as stock_label_num,
       concat_ws(',',if(t1.is_industry_rps=1,'行业rps>=87',null),
                     if(t1.is_stock_rps=1,'个股rps>=87',null),
                     if(t1.is_concept_rps=1,'概念rps>=87',null),
                     if(t1.is_pr_industry_cp_10=1,'行业板块涨跌幅前10%%-',null),
                     if(t1.is_lhb_buy=1,'当天龙虎榜_买',null),
                     if(t1.is_lhb_sell=1,'当天龙虎榜_卖-',null),
                     if(t1.is_pre_profit_increase=1,'预盈预增',null),
                     if(t1.is_pre_deficiency_decrease=1,'预亏预减-',null),
                     if(t1.is_rsi_6d_over_sell=1,'rsi_6d超卖',null),
                     if(t1.is_rsi_6d_over_buy=1,'rsi_6d超买-',null),
                     if(t1.is_rsi_6_bigger_12=1,'rsi_6_12金叉',null),
                     if(t1.is_rsi_6_smaller_12=1,'rsi_6_12死叉-',null)
       ) as sub_factor_names,
       (
        nvl(t1.is_industry_rps,0)+
        nvl(t1.is_stock_rps,0)+
        nvl(t1.is_concept_rps,0)-
        nvl(t1.is_pr_industry_cp_10,0)+
        nvl(t1.is_lhb_buy,0)-
        nvl(t1.is_lhb_sell,0)-
        nvl(t1.is_pre_profit_increase,0)-
        nvl(t1.is_pre_deficiency_decrease,0)+
        nvl(t1.is_rsi_6d_over_sell,0)-
        nvl(t1.is_rsi_6d_over_buy,0)+
        nvl(t1.is_rsi_6_bigger_12,0)-
        nvl(t1.is_rsi_6_smaller_12,0)
        ) as sub_factor_score,
       t1.holding_yield_2d,
       t1.holding_yield_5d,
       t1.hot_rank,
       t1.interprets,
       t1.reason_for_lhbs,
       t1.lhb_num_5d,
       t1.lhb_num_10d,
       t1.lhb_num_30d,
       t1.lhb_num_60d,
       t1.pe,
       t1.pe_ttm,
       t1.pb,
       t1.ps,
       t1.ps_ttm,
       t1.dv_ratio,
       t1.dv_ttm,
       t1.net_profit,
       t1.net_profit_yr,
       t1.total_business_income,
       t1.total_business_income_yr,
       t1.business_fee,
       t1.sales_fee,
       t1.management_fee,
       t1.finance_fee,
       t1.total_business_fee,
       t1.business_profit,
       t1.total_profit,
       t1.ps_business_cash_flow,
       t1.return_on_equity,
       t1.npadnrgal,
       t1.net_profit_growth_rate,
       
       tfp.suspension_time,
       tfp.suspension_deadline,
       tfp.suspension_period,
       tfp.suspension_reason,
       tfp.belongs_market,
       tfp.estimated_resumption_time,
       current_timestamp() as update_time,
       t1.trade_date as td
from tmp_dwd_02 t1
left join stock.ods_dc_stock_tfp_di tfp
    on t1.trade_date = tfp.trade_date
        and t1.stock_code = tfp.stock_code
        and tfp.td between '%s' and '%s'
where t1.trade_date between '%s' and '%s'
        """% (start_date_10,end_date,start_date_10,end_date))
         # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
        spark_df.repartition(1).write.insertInto('stock.dwd_stock_quotes_di', overwrite=True)  # 如果执行不存在这个表，会报错
        spark.stop
    except Exception as e:
        print(e)
    print('{}：执行完毕！！！'.format(appName))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_quotes_di.py all
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_quotes_di.py all
# nohup tmp_ods.py update 20221101 >> my.log 2>&1 &
# python tmp_ods.py all
# python tmp_ods.py update 20210101 20211231
if __name__ == '__main__':
    # between and 两边都包含
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
    print('程序运行时间：耗时{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))

