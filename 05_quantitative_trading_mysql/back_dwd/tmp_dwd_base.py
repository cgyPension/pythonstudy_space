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

    delete_tmp_dwd_base_sql = '''truncate table tmp_dwd_base;'''

    if start_date == '20210101':
        # 全量
        # 优化执行计划 才写这么长
        sql_tmp_dwd_base = '''
insert into tmp_dwd_base
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
       t1.turnover,
       t1.amplitude,
       t1.change_percent,
       t1.change_amount,
       t1.turnover_rate,
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
       plate.industry_plate,
       plate.concept_plates,
       t5.interpret,
       t5.reason_for_lhb,
       t5.lhb_net_buy,
       t5.lhb_buy_amount,
       t5.lhb_sell_amount,
       t5.lhb_turnover,
       t5.total_turnover,
       t5.nbtt,
       t5.ttt,
       t6.suspension_time,
       t6.suspension_deadline,
       t6.suspension_period,
       t6.suspension_reason,
       t6.belongs_market,
       t6.estimated_resumption_time
from ods_dc_stock_quotes_di t1
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
            and t1.stock_code = t5.stock_code
left join ods_dc_stock_tfp_di t6
    on t1.trade_date = t6.trade_date
        and t1.stock_code = t6.stock_code
left join dim_dc_stock_plate_df plate
        on t1.stock_code = plate.stock_code;
        '''

    else:
        s_date = '20210101'
        td_df = ak.tool_trade_date_hist_sina()
        daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
        # 增量 前5个交易日 但是要预够假期
        start_date = daterange_df.iloc[-5,0]
        # start_date = pd.to_datetime(start_date).date() - datetime.timedelta(20)
        end_date = pd.to_datetime(end_date).date()

        sql_tmp_dwd_base = '''
insert into tmp_dwd_base
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
       t1.turnover,
       t1.amplitude,
       t1.change_percent,
       t1.change_amount,
       t1.turnover_rate,
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
       plate.industry_plate,
       plate.concept_plates,
       t5.interpret,
       t5.reason_for_lhb,
       t5.lhb_net_buy,
       t5.lhb_buy_amount,
       t5.lhb_sell_amount,
       t5.lhb_turnover,
       t5.total_turnover,
       t5.nbtt,
       t5.ttt,
       t6.suspension_time,
       t6.suspension_deadline,
       t6.suspension_period,
       t6.suspension_reason,
       t6.belongs_market,
       t6.estimated_resumption_time
from ods_dc_stock_quotes_di t1
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
            and t5.trade_date between '%s' and '%s'
left join ods_dc_stock_tfp_di t6
    on t1.trade_date = t6.trade_date
        and t1.stock_code = t6.stock_code
        and t6.trade_date between '%s' and '%s'
left join dim_dc_stock_plate_df plate
        on t1.stock_code = plate.stock_code
where t1.trade_date between '%s' and '%s';
        '''% (start_date,end_date,start_date,end_date,start_date,end_date,start_date,end_date)


    try:
        #
        engine.execute(delete_tmp_dwd_base_sql)
        engine.execute(sql_tmp_dwd_base)
    except Exception as e:
        print(e)
    print("tmp_dwd_base：执行完毕！！！")

# nohup tmp_dwd_base.py update 20221101 >> my.log 2>&1 &
# python tmp_dwd_base.py all
# python tmp_dwd_base.py update 20210101 20211231
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

