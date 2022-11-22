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
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
from util.CommonUtils import get_process_num, get_spark


# 太多任务一起执行 mysql不释放内存 要分开断开连接重连
def get_data(start_date, end_date):
    # if start_date == '20210101':
    #     # 全量
    #     delete_sql = '''truncate table dwd_stock_quotes_di'''
    #     spark.sql(delete_sql)
    # else:
    #     s_date = '20210101'
    #     td_df = ak.tool_trade_date_hist_sina()
    #     daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
    #     # 增量 前5个交易日 但是要预够假期
    #     start_date = daterange_df.iloc[-5,0]
    #     end_date = pd.to_datetime(end_date).date()
    #     delete_sql = '''alter table dwd_stock_quotes_di drop if exists partition (td >= '%s',td <='%s')'''% (start_date,end_date)
    #     hive_engine.execute(delete_sql)

    try:
        appName = os.path.basename(__file__)
        # 本地模式
        spark = get_spark(appName)
        s_date = '20210101'
        td_df = ak.tool_trade_date_hist_sina()
        daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
        # 增量 前5个交易日 但是要预够假期
        start_date = daterange_df.iloc[-5,0]
        end_date = pd.to_datetime(end_date).date()


        spark.sql("""
with t3 as (
select *,
      if(lead(announcement_date,1)over(partition by stock_code order by announcement_date) is null,'9999-12-01',lead(announcement_date,1)over(partition by stock_code order by announcement_date)) as end_date
from ods_stock_lrb_em_di
),
t4 as (
select *,
      if(lead(announcement_date,1)over(partition by stock_code order by announcement_date) is null,'9999-12-01',lead(announcement_date,1)over(partition by stock_code order by announcement_date)) as end_date
from ods_financial_analysis_indicator_di
),
t5 as (
-- 有同一天上榜两次的情况,把原因凭借其他金额类字段去掉 ；拼接字段名加s
select trade_date,
       stock_code,
       concat_ws(';',collect_list(interpret)) as interprets,
       concat_ws(';',collect_list(reason_for_lhb)) as reason_for_lhbs
from ods_stock_lhb_detail_em_di
where td between '%s' and '%s'
      group by td,trade_date,stock_code
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
       plate.industry_plate,
       plate.concept_plates,
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
       if(lag(t1.close_price,4)over(partition by t1.stock_code order by t1.trade_date) is null,null,avg(t1.close_price)over(partition by t1.stock_code order by t1.trade_date rows between 4 preceding and current row)) as ma_5d,
       if(lag(t1.close_price,9)over(partition by t1.stock_code order by t1.trade_date) is null,null,avg(t1.close_price)over(partition by t1.stock_code order by t1.trade_date rows between 9 preceding and current row)) as ma_10d,
       if(lag(t1.close_price,19)over(partition by t1.stock_code order by t1.trade_date) is null,null,avg(t1.close_price)over(partition by t1.stock_code order by t1.trade_date rows between 19 preceding and current row)) as ma_20d,
       if(lag(t1.close_price,29)over(partition by t1.stock_code order by t1.trade_date) is null,null,avg(t1.close_price)over(partition by t1.stock_code order by t1.trade_date rows between 29 preceding and current row)) as ma_30d,
       if(lag(t1.close_price,59)over(partition by t1.stock_code order by t1.trade_date) is null,null,avg(t1.close_price)over(partition by t1.stock_code order by t1.trade_date rows between 59 preceding and current row)) as ma_60d,
       if(lead(t1.close_price,1)over(partition by t1.stock_code order by t1.trade_date) is null or t1.open_price = 0,null,(t1.open_price-lead(t1.close_price,1)over(partition by t1.stock_code order by t1.trade_date))/t1.open_price) as holding_yield_2d,
       if(lead(t1.close_price,4)over(partition by t1.stock_code order by t1.trade_date) is null or t1.open_price = 0,null,(t1.open_price-lead(t1.close_price,4)over(partition by t1.stock_code order by t1.trade_date))/t1.open_price) as holding_yield_5d
from ods_dc_stock_quotes_di t1
left join ods_lg_indicator_di t2
        on t1.trade_date = t2.trade_date
            and t1.stock_code = t2.stock_code
            and t1.td between '%s' and '%s'
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
left join dim_dc_stock_plate_df plate
        on t1.stock_code = plate.stock_code
where t1.td between '%s' and '%s'
        """% (start_date,end_date,start_date,end_date,start_date,end_date)).createOrReplaceTempView('tmp_dwd_01')

        spark.sql("""
select *,
       if(reason_for_lhbs is not null,1,0) as is_lhb,
       if(lhb_num_60d>0,1,0) is_lhb_60d,
       if(percent_rank()over(partition by trade_date order by total_market_value)<0.333,1,0) as is_min_market_value,
       if(lag(volume_ratio_1d,1)over(partition by stock_code order by trade_date) >1 and volume_ratio_1d >1,1,0) as is_rise_volume_2d,
       if(lag(volume_ratio_1d,1)over(partition by stock_code order by trade_date) >1 and volume_ratio_1d >1
           and lag(close_price,1)over(partition by stock_code order by trade_date) < lag(open_price,1)over(partition by stock_code order by trade_date)
                        and close_price<open_price,1,0) as is_rise_volume_2d_low,
       if(high_price>ma_5d,1,0) as is_rise_ma_5d,
       if(high_price>ma_10d,1,0) as is_rise_ma_10d,
       if(high_price>ma_20d,1,0) as is_rise_ma_20d,
       if(high_price>ma_30d,1,0) as is_rise_ma_30d,
       if(high_price>ma_60d,1,0) as is_rise_ma_60d
from tmp_dwd_01
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
       t1.interprets,
       t1.reason_for_lhbs,
       t1.lhb_num_5d,
       t1.lhb_num_10d,
       t1.lhb_num_30d,
       t1.lhb_num_60d,
       t1.ma_5d,
       t1.ma_10d,
       t1.ma_20d,
       t1.ma_30d,
       t1.ma_60d,
       concat_ws(',',if(t1.is_min_market_value=1,'小市值',null),
                     if(t1.is_lhb=1,'今天龙虎榜',null),
                     if(t1.is_lhb_60d=1,'最近60天龙虎榜',null),
                     if(t1.is_rise_volume_2d=1,'连续两天放量-',null),
                     if(t1.is_rise_volume_2d_low=1,'连续两天放量且低收-',null),
                     if(t1.is_rise_ma_5d=1,'上穿5日均线',null),
                     if(t1.is_rise_ma_10d=1,'上穿10日均线',null),
                     if(t1.is_rise_ma_20d=1,'上穿20日均线',null),
                     if(t1.is_rise_ma_30d=1,'上穿30日均线',null),
                     if(t1.is_rise_ma_60d=1,'上穿60日均线',null)
       ) as stock_label_names,
       (
        t1.is_min_market_value+
        t1.is_lhb+
        t1.is_lhb_60d+
        t1.is_rise_volume_2d+
        t1.is_rise_volume_2d_low+
        t1.is_rise_ma_5d+
        t1.is_rise_ma_10d+
        t1.is_rise_ma_20d+
        t1.is_rise_ma_30d+
        t1.is_rise_ma_60d
        ) as stock_label_num,
       concat_ws(',',if(t1.is_min_market_value=1,'小市值',null),
                     if(t1.is_lhb=1,'今天龙虎榜',null),
                     if(t1.is_lhb_60d=1,'最近60天龙虎榜',null),
                     if(t1.is_rise_volume_2d=1,'连续两天放量-',null),
                     if(t1.is_rise_volume_2d_low=1,'连续两天放量且低收-',null)
       ) as factor_names,
       (
        t1.is_min_market_value+
        t1.is_lhb+
        t1.is_lhb_60d-
        t1.is_rise_volume_2d-
        t1.is_rise_volume_2d_low
        ) as factor_score,
       t1.holding_yield_2d,
       t1.holding_yield_5d,
       tfp.suspension_time,
       tfp.suspension_deadline,
       tfp.suspension_period,
       tfp.suspension_reason,
       tfp.belongs_market,
       tfp.estimated_resumption_time,
       current_timestamp() as update_time,
       t1.trade_date as td
from tmp_dwd_02 t1
left join ods_dc_stock_tfp_di tfp
    on t1.trade_date = tfp.trade_date
        and t1.stock_code = tfp.stock_code
        and tfp.td between '%s' and '%s'
        """% (start_date,end_date))
         # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
        spark_df.repartition(1).write.insertInto('stock.dwd_stock_quotes_di', overwrite=True)  # 如果执行不存在这个表，会报错

    except Exception as e:
        print(e)
    print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/05_quantitative_trading_hive/ods/dwd_stock_quotes_di.py all
# nohup dwd_stock_quotes_di.py update 20221101 >> my.log 2>&1 &
# python dwd_stock_quotes_di.py all
# python dwd_stock_quotes_di.py update 20210101 20211231
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

