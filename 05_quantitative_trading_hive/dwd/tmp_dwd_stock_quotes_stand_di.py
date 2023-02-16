import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import time
from datetime import date
import pandas as pd
from util.CommonUtils import get_spark

def get_data():
    '''
    经常需要修改不标签字段 这样可以提高dwd_stock_quotes_stand_di全量重跑的速度
    '''
    try:
        appName = os.path.basename(__file__)
        spark = get_spark(appName)
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
                       t1.rps_120d,
                       t1.rps_250d,
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
                       t2.stock_label_names,
                       t2.stock_label_num,
                       t2.sub_factor_names,
                       t2.sub_factor_score,
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
                       t1.suspension_time,
                       t1.suspension_deadline,
                       t1.suspension_period,
                       t1.suspension_reason,
                       t1.belongs_market,
                       t1.estimated_resumption_time,
                       t1.f_volume,
                       t1.f_volume_ratio_1d,
                       t1.f_volume_ratio_5d,
                       t1.f_turnover,
                       t1.f_turnover_rate,
                       t1.f_turnover_rate_5d,
                       t1.f_turnover_rate_10d,
                       t1.f_total_market_value,
                       t1.f_pe,
                       t1.f_pe_ttm,
                       current_timestamp() as update_time,
                       t1.td
                from stock.dwd_stock_quotes_stand_di t1
                left join stock.dwd_stock_quotes_di t2
                    on t1.trade_date = t2.trade_date
                        and t1.stock_code = t2.stock_code
        """)
        spark_df.repartition(1).write.insertInto('stock.dwd_stock_quotes_stand_di', overwrite=True)
        spark.stop
    except Exception as e:
        print(e)
    print('{}：执行完毕！！！'.format(appName))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/tmp_dwd_stock_quotes_stand_di.py
# nohup tmp_dwd_stock_quotes_stand_di.py >> my.log 2>&1 &
# python tmp_dwd_stock_quotes_stand_di.py
if __name__ == '__main__':
    start_time = time.time()
    get_data()
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))
