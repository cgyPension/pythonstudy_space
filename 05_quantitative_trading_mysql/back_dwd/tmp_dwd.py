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
def get_data(engine):

    delete_tmp_dwd_sql = '''truncate table tmp_dwd;'''

    sql_tmp_dwd ='''
insert into tmp_dwd
select trade_date,
       stock_code,
       stock_name,
       open_price,
       close_price,
       high_price,
       low_price,
       volume,
       if(lag(volume,1,null)over(partition by stock_code order by trade_date) is null or volume is null,null,volume/lag(volume,1,null)over(partition by stock_code order by trade_date)) as volume_ratio_1d,
       if(lag(volume,4,null)over(partition by stock_code order by trade_date) is null or volume is null,null,volume/avg(volume)over(partition by stock_code order by trade_date rows between 4 preceding and current row)) as volume_ratio_5d,
       turnover,
       amplitude,
       change_percent,
       change_amount,
       turnover_rate,
       if(lag(close_price,9,null)over(partition by stock_code order by trade_date) is null,null,avg(turnover_rate)over(partition by stock_code order by trade_date rows between 9 preceding and current row)) as turnover_rate_10d,
       total_market_value,
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
       industry_plate,
       concept_plates,
       interpret,
       reason_for_lhb,
       lhb_net_buy,
       lhb_buy_amount,
       lhb_sell_amount,
       lhb_turnover,
       total_turnover,
       nbtt,
       ttt,
       sum(if(reason_for_lhb is not null,1,0))over(partition by stock_code order by trade_date rows between 4 preceding and current row) as lhb_num_5d,
       sum(if(reason_for_lhb is not null,1,0))over(partition by stock_code order by trade_date rows between 9 preceding and current row) as lhb_num_10d,
       sum(if(reason_for_lhb is not null,1,0))over(partition by stock_code order by trade_date rows between 29 preceding and current row) as lhb_num_30d,
       sum(if(reason_for_lhb is not null,1,0))over(partition by stock_code order by trade_date rows between 59 preceding and current row) as lhb_num_60d,
       # 如果不够5日数据 则为空
       if(lag(close_price,4,null)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 4 preceding and current row)) as ma_5d,
       if(lag(close_price,9,null)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 9 preceding and current row)) as ma_10d,
       if(lag(close_price,19,null)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 19 preceding and current row)) as ma_20d,
       if(lag(close_price,29,null)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 29 preceding and current row)) as ma_30d,
       if(lag(close_price,59,null)over(partition by stock_code order by trade_date) is null,null,avg(close_price)over(partition by stock_code order by trade_date rows between 59 preceding and current row)) as ma_60d,
       if(lead(close_price,1,null)over(partition by stock_code order by trade_date) is null or open_price = 0,null,(open_price-lead(close_price,1,null)over(partition by stock_code order by trade_date))/open_price) as holding_yield_2d,
       if(lead(close_price,4,null)over(partition by stock_code order by trade_date) is null or open_price = 0,null,(open_price-lead(close_price,4,null)over(partition by stock_code order by trade_date))/open_price) as holding_yield_5d,
       suspension_time,
       suspension_deadline,
       suspension_period,
       suspension_reason,
       belongs_market,
       estimated_resumption_time
from tmp_dwd_base;
    '''

    try:
        engine.execute(delete_tmp_dwd_sql)
        engine.execute(sql_tmp_dwd)
    except Exception as e:
        print(e)
    print("tmp_dwd.py：执行完毕！！！")

# nohup tmp_dwd.py update 20221101 >> my.log 2>&1 &
# python tmp_dwd.py all
# python tmp_dwd.py update 20210101 20211231
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
    get_data(engine)
    engine.dispose()
    end_time = time.time()
    print('程序运行时间：耗时{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))

