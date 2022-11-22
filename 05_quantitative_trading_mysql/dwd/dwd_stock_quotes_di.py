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
        delete_sql = '''truncate table dwd_stock_quotes_di;'''
        sql = '''
        insert into dwd_stock_quotes_di (trade_date, stock_code, stock_name, open_price, close_price, high_price, low_price,
                                 volume, volume_ratio_1d, volume_ratio_5d, turnover, amplitude, change_percent,
                                 change_amount, turnover_rate, turnover_rate_5d, turnover_rate_10d, total_market_value,
                                 industry_plate, concept_plates, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
                                 net_profit, net_profit_yr, total_business_income, total_business_income_yr,
                                 business_fee, sales_fee, management_fee, finance_fee, total_business_fee,
                                 business_profit, total_profit, ps_business_cash_flow, return_on_equity, npadnrgal,
                                 net_profit_growth_rate, interpret, reason_for_lhb, lhb_net_buy, lhb_buy_amount,
                                 lhb_sell_amount, lhb_turnover, total_turnover, nbtt, ttt, lhb_num_5d, lhb_num_10d,
                                 lhb_num_30d, lhb_num_60d, ma_5d, ma_10d, ma_20d, ma_30d, ma_60d, stock_label_names,
                                 stock_label_num, factor_names, factor_score, holding_yield_2d, holding_yield_5d,
                                 suspension_time, suspension_deadline, suspension_period, suspension_reason,
                                 belongs_market, estimated_resumption_time)
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
               plate.industry_plate,
               plate.concept_plates,
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
               t1.interpret,
               t1.reason_for_lhb,
               t1.lhb_net_buy,
               t1.lhb_buy_amount,
               t1.lhb_sell_amount,
               t1.lhb_turnover,
               t1.total_turnover,
               t1.nbtt,
               t1.ttt,
               t1.lhb_num_5d,
               t1.lhb_num_10d,
               t1.lhb_num_30d,
               t1.lhb_num_60d,
               t1.ma_5d,
               t1.ma_10d,
               t1.ma_20d,
               t1.ma_30d,
               t1.ma_60d,
               concat_ws(',',if(is_min_market_value=1,'小市值',null),
                             if(is_lhb=1,'今天龙虎榜',null),
                             if(is_lhb_60d=1,'最近60天龙虎榜',null),
                             if(is_rise_volume_2d=1,'连续两天放量-',null),
                             if(is_rise_volume_2d_low=1,'连续两天放量且低收-',null),
                             if(is_rise_ma_5d=1,'上穿5日均线',null),
                             if(is_rise_ma_10d=1,'上穿10日均线',null),
                             if(is_rise_ma_20d=1,'上穿20日均线',null),
                             if(is_rise_ma_30d=1,'上穿30日均线',null),
                             if(is_rise_ma_60d=1,'上穿60日均线',null)
               ) as stock_label_names,
               (
                is_min_market_value+
                is_lhb+
                is_lhb_60d+
                is_rise_volume_2d+
                is_rise_volume_2d_low+
                is_rise_ma_5d+
                is_rise_ma_10d+
                is_rise_ma_20d+
                is_rise_ma_30d+
                is_rise_ma_60d
                ) as stock_label_num,
               concat_ws(',',if(is_min_market_value=1,'小市值',null),
                             if(is_lhb=1,'今天龙虎榜',null),
                             if(is_lhb_60d=1,'最近60天龙虎榜',null),
                             if(is_rise_volume_2d=1,'连续两天放量-',null),
                             if(is_rise_volume_2d_low=1,'连续两天放量且低收-',null)
               ) as factor_names,
               (
                is_min_market_value+
                is_lhb+
                is_lhb_60d-
                is_rise_volume_2d-
                is_rise_volume_2d_low
                ) as factor_score,
               t1.holding_yield_2d,
               t1.holding_yield_5d,
               tfp.suspension_time,
               tfp.suspension_deadline,
               tfp.suspension_period,
               tfp.suspension_reason,
               tfp.belongs_market,
               tfp.estimated_resumption_time
        from tmp_dwd_02 t1
        left join ods_dc_stock_tfp_di tfp
            on t1.trade_date = tfp.trade_date
                and t1.stock_code = tfp.stock_code
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
        delete_sql = '''delete from dwd_stock_quotes_di where trade_date between '%s' and '%s';'''% (start_date,end_date)
        sql = '''
        insert into dwd_stock_quotes_di (trade_date, stock_code, stock_name, open_price, close_price, high_price, low_price,
                                 volume, volume_ratio_1d, volume_ratio_5d, turnover, amplitude, change_percent,
                                 change_amount, turnover_rate, turnover_rate_5d, turnover_rate_10d, total_market_value,
                                 industry_plate, concept_plates, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
                                 net_profit, net_profit_yr, total_business_income, total_business_income_yr,
                                 business_fee, sales_fee, management_fee, finance_fee, total_business_fee,
                                 business_profit, total_profit, ps_business_cash_flow, return_on_equity, npadnrgal,
                                 net_profit_growth_rate, interpret, reason_for_lhb, lhb_net_buy, lhb_buy_amount,
                                 lhb_sell_amount, lhb_turnover, total_turnover, nbtt, ttt, lhb_num_5d, lhb_num_10d,
                                 lhb_num_30d, lhb_num_60d, ma_5d, ma_10d, ma_20d, ma_30d, ma_60d, stock_label_names,
                                 stock_label_num, factor_names, factor_score, holding_yield_2d, holding_yield_5d,
                                 suspension_time, suspension_deadline, suspension_period, suspension_reason,
                                 belongs_market, estimated_resumption_time)
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
               plate.industry_plate,
               plate.concept_plates,
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
               t1.interpret,
               t1.reason_for_lhb,
               t1.lhb_net_buy,
               t1.lhb_buy_amount,
               t1.lhb_sell_amount,
               t1.lhb_turnover,
               t1.total_turnover,
               t1.nbtt,
               t1.ttt,
               t1.lhb_num_5d,
               t1.lhb_num_10d,
               t1.lhb_num_30d,
               t1.lhb_num_60d,
               t1.ma_5d,
               t1.ma_10d,
               t1.ma_20d,
               t1.ma_30d,
               t1.ma_60d,
               concat_ws(',',if(is_min_market_value=1,'小市值',null),
                             if(is_lhb=1,'今天龙虎榜',null),
                             if(is_lhb_60d=1,'最近60天龙虎榜',null),
                             if(is_rise_volume_2d=1,'连续两天放量-',null),
                             if(is_rise_volume_2d_low=1,'连续两天放量且低收-',null),
                             if(is_rise_ma_5d=1,'上穿5日均线',null),
                             if(is_rise_ma_10d=1,'上穿10日均线',null),
                             if(is_rise_ma_20d=1,'上穿20日均线',null),
                             if(is_rise_ma_30d=1,'上穿30日均线',null),
                             if(is_rise_ma_60d=1,'上穿60日均线',null)
               ) as stock_label_names,
               (
                is_min_market_value+
                is_lhb+
                is_lhb_60d+
                is_rise_volume_2d+
                is_rise_volume_2d_low+
                is_rise_ma_5d+
                is_rise_ma_10d+
                is_rise_ma_20d+
                is_rise_ma_30d+
                is_rise_ma_60d
                ) as stock_label_num,
               concat_ws(',',if(is_min_market_value=1,'小市值',null),
                             if(is_lhb=1,'今天龙虎榜',null),
                             if(is_lhb_60d=1,'最近60天龙虎榜',null),
                             if(is_rise_volume_2d=1,'连续两天放量-',null),
                             if(is_rise_volume_2d_low=1,'连续两天放量且低收-',null)
               ) as factor_names,
               (
                is_min_market_value+
                is_lhb+
                is_lhb_60d-
                is_rise_volume_2d-
                is_rise_volume_2d_low
                ) as factor_score,
               t1.holding_yield_2d,
               t1.holding_yield_5d,
               tfp.suspension_time,
               tfp.suspension_deadline,
               tfp.suspension_period,
               tfp.suspension_reason,
               tfp.belongs_market,
               tfp.estimated_resumption_time
        from tmp_dwd_02 t1
        left join ods_dc_stock_tfp_di tfp
            on t1.trade_date = tfp.trade_date
                and t1.stock_code = tfp.stock_code
                and t2.trade_date between '%s' and '%s'
        left join dim_dc_stock_plate_df plate
                on t1.stock_code = plate.stock_code;
        '''

    try:
        engine.execute(sql)
        engine.execute(delete_sql)
    except Exception as e:
        print(e)
    print("dwd_stock_quotes_di：执行完毕！！！")

# nohup dwd_stock_quotes_di.py update 20221101 >> my.log 2>&1 &
# python dwd_stock_quotes_di.py all
# python dwd_stock_quotes_di.py update 20210101 20211231
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

