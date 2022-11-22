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

    # 全量
    # 优化执行计划 才写这么长
    tmp_dwd_02 = '''
    insert into tmp_dwd_02
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
           interpret,
           reason_for_lhb,
           lhb_net_buy,
           lhb_buy_amount,
           lhb_sell_amount,
           lhb_turnover,
           total_turnover,
           nbtt,
           ttt,
           sum(is_lhb)over(partition by stock_code order by trade_date rows between 4 preceding and current row) as lhb_num_5d,
           sum(is_lhb)over(partition by stock_code order by trade_date rows between 9 preceding and current row) as lhb_num_10d,
           sum(is_lhb)over(partition by stock_code order by trade_date rows between 29 preceding and current row) as lhb_num_30d,
           sum(is_lhb)over(partition by stock_code order by trade_date rows between 59 preceding and current row) as lhb_num_60d,
           ma_5d,
           ma_10d,
           ma_20d,
           ma_30d,
           ma_60d,
           holding_yield_2d,
           holding_yield_5d,
           is_lhb,
           if(sum(is_lhb)over(partition by stock_code order by trade_date rows between 59 preceding and current row)>0,1,0) is_lhb_60d,
           if(percent_rank()over(partition by trade_date order by total_market_value)<0.333,1,0) as is_min_market_value,
           if(lag(volume_ratio_1d,1)over(partition by stock_code order by trade_date) >1 and volume_ratio_1d >1,1,0) as is_rise_volume_2d,
           if(lag(volume_ratio_1d,1)over(partition by stock_code order by trade_date) >1 and volume_ratio_1d >1
               and lag(close_price,1)over(partition by stock_code order by trade_date) < lag(open_price,1)over(partition by stock_code order by trade_date)
                            and close_price<open_price,1,0) as is_rise_volume_2d_low,
           is_rise_ma_5d,
           is_rise_ma_10d,
           is_rise_ma_20d,
           is_rise_ma_30d,
           is_rise_ma_60d
    from tmp_dwd_01;
    '''

    delete_tmp_dwd_02 = '''truncate table tmp_dwd_02;'''

    try:
        #
        engine.execute(delete_tmp_dwd_02)
        engine.execute(tmp_dwd_02)
    except Exception as e:
        print(e)
    print("tmp_dwd_02：执行完毕！！！")

# nohup tmp_dwd_02.py update 20221101 >> my.log 2>&1 &
# python tmp_dwd_02.py all
# python tmp_dwd_02.py update 20210101 20211231
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

