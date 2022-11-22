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

warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.DBUtils import sqlalchemyUtil
from util.CommonUtils import get_process_num, get_code_group, get_code_list

# 太多任务一起执行 mysql不释放内存 要分开断开连接重连
def get_data(start_date, end_date,engine):

    if start_date == '20210101':
        # 全量
        # 优化执行计划 才写这么长
        tmp_ods_dc_stock_quotes_df = '''
        insert into tmp_ods_dc_stock_quotes_df
        select trade_date,
               stock_code,
               stock_name,
               open_price,
               close_price,
               high_price,
               low_price,
               volume,
               if(lag(volume,1)over(partition by stock_code order by trade_date) is null or volume is null,null,volume/lag(volume,1,null)over(partition by stock_code order by trade_date)) as volume_ratio_1d,
               if(lag(volume,4)over(partition by stock_code order by trade_date) is null or volume is null,null,volume/avg(volume)over(partition by stock_code order by trade_date rows between 4 preceding and current row)) as volume_ratio_5d,
               turnover,
               amplitude,
               change_percent,
               change_amount,
               turnover_rate,
               if(lag(close_price,4)over(partition by stock_code order by trade_date) is null,null,avg(turnover_rate)over(partition by stock_code order by trade_date rows between 4 preceding and current row)) as turnover_rate_5d,
               if(lag(close_price,9)over(partition by stock_code order by trade_date) is null,null,avg(turnover_rate)over(partition by stock_code order by trade_date rows between 9 preceding and current row)) as turnover_rate_10d,
               # 如果不够5日数据 则为空
               if(lag(close_price,4)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 4 preceding and current row)) as ma_5d,
               if(lag(close_price,9)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 9 preceding and current row)) as ma_10d,
               if(lag(close_price,19)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 19 preceding and current row)) as ma_20d,
               if(lag(close_price,29)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 29 preceding and current row)) as ma_30d,
               if(lag(close_price,59)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 59 preceding and current row)) as ma_60d,
               if(lead(close_price,1)over(partition by stock_code order by trade_date) is null or open_price = 0,null,(open_price-lead(close_price,1)over(partition by stock_code order by trade_date))/open_price) as holding_yield_2d,
               if(lead(close_price,4)over(partition by stock_code order by trade_date) is null or open_price = 0,null,(open_price-lead(close_price,4)over(partition by stock_code order by trade_date))/open_price) as holding_yield_5d
        from ods_dc_stock_quotes_di;
        '''
        tmp_dwd_01 = '''
        insert into tmp_dwd_01
        with t3 as (
        select *,
              if(lead(announcement_date,1)over(partition by stock_code order by announcement_date) is null,'9999-12-01',lead(announcement_date,1)over(partition by stock_code order by announcement_date)) as end_date
        from ods_stock_lrb_em_di
        ),
        t4 as (
        select *,
              if(lead(announcement_date,1)over(partition by stock_code order by announcement_date) is null,'9999-12-01',lead(announcement_date,1)over(partition by stock_code order by announcement_date)) as end_date
        from ods_financial_analysis_indicator_di
        )
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
               t2.total_market_value,
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
               t5.interpret,
               t5.reason_for_lhb,
               t5.lhb_net_buy,
               t5.lhb_buy_amount,
               t5.lhb_sell_amount,
               t5.lhb_turnover,
               t5.total_turnover,
               t5.nbtt,
               t5.ttt,
               t1.ma_5d,
               t1.ma_10d,
               t1.ma_20d,
               t1.ma_30d,
               t1.ma_60d,
               t1.holding_yield_2d,
               t1.holding_yield_5d,
               if(t5.reason_for_lhb is not null,1,0) as is_lhb,
               if(high_price>ma_5d,1,0) as is_rise_ma_5d,
               if(high_price>ma_10d,1,0) as is_rise_ma_10d,
               if(high_price>ma_20d,1,0) as is_rise_ma_20d,
               if(high_price>ma_30d,1,0) as is_rise_ma_30d,
               if(high_price>ma_60d,1,0) as is_rise_ma_60d
        from tmp_ods_dc_stock_quotes_df t1
        left join ods_lg_indicator_di t2
                on t1.trade_date = t2.trade_date
                    and t1.stock_code = t2.stock_code
        # 区间关联左闭右开
        left join t3
                on t1.trade_date >= t3.announcement_date and t1.trade_date <t3.end_date
                    and t1.stock_code = t3.stock_code
        # 区间关联左闭右开
        left join  t4
               on t1.trade_date >= t4.announcement_date and t1.trade_date <t4.end_date
                 and t1.stock_code = t4.stock_code
        left join ods_stock_lhb_detail_em_di t5
               on t1.trade_date = t5.trade_date
                    and t1.stock_code = t5.stock_code;
        '''
    else:
        s_date = '20210101'
        td_df = ak.tool_trade_date_hist_sina()
        daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
        # 增量 前5个交易日 但是要预够假期
        start_date = daterange_df.iloc[-5,0]
        # start_date = pd.to_datetime(start_date).date() - datetime.timedelta(20)
        end_date = pd.to_datetime(end_date).date()

        tmp_ods_dc_stock_quotes_df = '''
        insert into tmp_ods_dc_stock_quotes_df
        select trade_date,
               stock_code,
               stock_name,
               open_price,
               close_price,
               high_price,
               low_price,
               volume,
               if(lag(volume,1)over(partition by stock_code order by trade_date) is null or volume is null,null,volume/lag(volume,1,null)over(partition by stock_code order by trade_date)) as volume_ratio_1d,
               if(lag(volume,4)over(partition by stock_code order by trade_date) is null or volume is null,null,volume/avg(volume)over(partition by stock_code order by trade_date rows between 4 preceding and current row)) as volume_ratio_5d,
               turnover,
               amplitude,
               change_percent,
               change_amount,
               turnover_rate,
               if(lag(close_price,4)over(partition by stock_code order by trade_date) is null,null,avg(turnover_rate)over(partition by stock_code order by trade_date rows between 4 preceding and current row)) as turnover_rate_5d,
               if(lag(close_price,9)over(partition by stock_code order by trade_date) is null,null,avg(turnover_rate)over(partition by stock_code order by trade_date rows between 9 preceding and current row)) as turnover_rate_10d,
               # 如果不够5日数据 则为空
               if(lag(close_price,4)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 4 preceding and current row)) as ma_5d,
               if(lag(close_price,9)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 9 preceding and current row)) as ma_10d,
               if(lag(close_price,19)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 19 preceding and current row)) as ma_20d,
               if(lag(close_price,29)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 29 preceding and current row)) as ma_30d,
               if(lag(close_price,59)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 59 preceding and current row)) as ma_60d,
               if(lead(close_price,1)over(partition by stock_code order by trade_date) is null or open_price = 0,null,(open_price-lead(close_price,1)over(partition by stock_code order by trade_date))/open_price) as holding_yield_2d,
               if(lead(close_price,4)over(partition by stock_code order by trade_date) is null or open_price = 0,null,(open_price-lead(close_price,4)over(partition by stock_code order by trade_date))/open_price) as holding_yield_5d
        from ods_dc_stock_quotes_di
        where trade_date between '%s' and '%s';
        '''
        tmp_dwd_01 = '''
        insert into tmp_dwd_01
        with t3 as (
        select *,
              if(lead(announcement_date,1)over(partition by stock_code order by announcement_date) is null,'9999-12-01',lead(announcement_date,1)over(partition by stock_code order by announcement_date)) as end_date
        from ods_stock_lrb_em_di
        ),
        t4 as (
        select *,
              if(lead(announcement_date,1)over(partition by stock_code order by announcement_date) is null,'9999-12-01',lead(announcement_date,1)over(partition by stock_code order by announcement_date)) as end_date
        from ods_financial_analysis_indicator_di
        )
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
               t2.total_market_value,
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
               t5.interpret,
               t5.reason_for_lhb,
               t5.lhb_net_buy,
               t5.lhb_buy_amount,
               t5.lhb_sell_amount,
               t5.lhb_turnover,
               t5.total_turnover,
               t5.nbtt,
               t5.ttt,
               t1.ma_5d,
               t1.ma_10d,
               t1.ma_20d,
               t1.ma_30d,
               t1.ma_60d,
               t1.holding_yield_2d,
               t1.holding_yield_5d,
               if(t5.reason_for_lhb is not null,1,0) as is_lhb,
               if(high_price>ma_5d,1,0) as is_rise_ma_5d,
               if(high_price>ma_10d,1,0) as is_rise_ma_10d,
               if(high_price>ma_20d,1,0) as is_rise_ma_20d,
               if(high_price>ma_30d,1,0) as is_rise_ma_30d,
               if(high_price>ma_60d,1,0) as is_rise_ma_60d
        from tmp_ods_dc_stock_quotes_df t1
        left join ods_lg_indicator_di t2
                on t1.trade_date = t2.trade_date
                    and t1.stock_code = t2.stock_code
                    and t2.trade_date between '%s' and '%s'
        # 区间关联左闭右开
        left join t3
                on t1.trade_date >= t3.announcement_date and t1.trade_date <t3.end_date
                    and t1.stock_code = t3.stock_code
        # 区间关联左闭右开
        left join  t4
               on t1.trade_date >= t4.announcement_date and t1.trade_date <t4.end_date
                 and t1.stock_code = t4.stock_code
        left join ods_stock_lhb_detail_em_di t5
               on t1.trade_date = t5.trade_date
                    and t1.stock_code = t5.stock_code
                    and t5.trade_date between '%s' and '%s';
        '''

    delete_tmp_ods_dc_stock_quotes_df = '''truncate table tmp_ods_dc_stock_quotes_df;'''
    delete_tmp_dwd_01 = '''truncate table tmp_dwd_01;'''

    try:
        #
        engine.execute(delete_tmp_ods_dc_stock_quotes_df)
        engine.execute(tmp_ods_dc_stock_quotes_df)
        engine.execute(delete_tmp_dwd_01)
        engine.execute(tmp_dwd_01)
    except Exception as e:
        print(e)
    print("tmp_dwd_01：执行完毕！！！")

# nohup tmp_dwd_01.py update 20221101 >> my.log 2>&1 &
# python tmp_dwd_01.py all
# python tmp_dwd_01.py update 20210101 20211231
if __name__ == '__main__':
    # between and 两边都包含
    engine = sqlalchemyUtil().engine
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
    get_data(start_date, end_date,engine)
    engine.dispose()
    end_time = time.time()
    print('程序运行时间：耗时{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))

