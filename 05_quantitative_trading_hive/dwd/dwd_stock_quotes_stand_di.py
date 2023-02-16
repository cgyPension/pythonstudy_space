import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import findspark
import time
import warnings
from datetime import date,datetime
import akshare as ak
import numpy as np
import pandas as pd
from util.CommonUtils import get_spark
from util.factorFormatUtils import factor_clean

# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

def get_data(factors,start_date, end_date):
    '''去极值 标准化 行业市值中性化'''
    appName = os.path.basename(__file__)
    findspark.init()
    spark = get_spark(appName)
    start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
    s_date = '20210101'
    # 增量 因为持股5日收益 要提前6个交易日 这里是下一交易日开盘买入 持股两天 在第二天的收盘卖出
    td_df = ak.tool_trade_date_hist_sina()
    daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < start_date)]
    daterange_df = daterange_df.iloc[-7:, 0].reset_index(drop=True)
    if daterange_df.empty:
        start_date = pd.to_datetime(start_date).date()
    else:
        start_date = pd.to_datetime(daterange_df[0]).date()

    spark_df = spark.sql("""
        select *,log(total_market_value) as market_value
        from stock.dwd_stock_quotes_di
        where td between '%s' and '%s'
        order by trade_date
        """ % (start_date, end_date))
    pd_df = spark_df.toPandas()
    # result_df = pd_df.copy()
    result_df = pd.DataFrame(columns=['trade_date','stock_code'])
    # 原本是直接用pd_df leftjoin 改用full会不会更省内存
    for factor in factors:
        r_df = factor_clean(factor, pd_df)
        # result_df = pd.merge(result_df, r_df, how='left', on=['trade_date','stock_code'])
        result_df = pd.merge(result_df, r_df, how='outer', on=['trade_date','stock_code'])

    result_df = pd.merge(pd_df, result_df, how='left', on=['trade_date', 'stock_code'])
    # print('merge：', result_df[['trade_date', 'stock_code', 'turnover','pe_ttm']].head())
    result_df['update_time'] = datetime.now()
    result_df['td'] = result_df['trade_date']
    result_df = result_df[['trade_date','stock_code','stock_name','open_price','close_price','high_price','low_price','volume','volume_ratio_1d','volume_ratio_5d','turnover','amplitude','change_percent','change_amount','turnover_rate','turnover_rate_5d','turnover_rate_10d','total_market_value','industry_plate','concept_plates','rps_5d','rps_10d','rps_20d','rps_50d','rps_120d','rps_250d','rs','rsi_6d','rsi_12d','ma_5d','ma_10d','ma_20d','ma_50d','ma_120d','ma_150d','ma_200d','ma_250d','high_price_250d','low_price_250d','stock_label_names','stock_label_num','sub_factor_names','sub_factor_score','holding_yield_2d','holding_yield_5d','hot_rank','interprets','reason_for_lhbs','lhb_num_5d','lhb_num_10d','lhb_num_30d','lhb_num_60d','pe','pe_ttm','pb','ps','ps_ttm','dv_ratio','dv_ttm','net_profit','net_profit_yr','total_business_income','total_business_income_yr','business_fee','sales_fee','management_fee','finance_fee','total_business_fee','business_profit','total_profit','ps_business_cash_flow','return_on_equity','npadnrgal','net_profit_growth_rate','suspension_time','suspension_deadline','suspension_period','suspension_reason','belongs_market','estimated_resumption_time','f_volume','f_volume_ratio_1d','f_volume_ratio_5d','f_turnover','f_turnover_rate','f_turnover_rate_5d','f_turnover_rate_10d','f_total_market_value','f_pe','f_pe_ttm','update_time','td']]
    # print('result_df：',result_df.query('stock_code=="sh600018"')[['trade_date','stock_code','turnover','pe_ttm']].sort_values(by=['trade_date','stock_code'],ascending=True))
    # print('result_df：',result_df.info())
    # print('result_df：',result_df[['trade_date','stock_code','turnover','pe_ttm']].head())
    s_df = spark.createDataFrame(result_df)
    s_df.repartition(1).write.insertInto('stock.dwd_stock_quotes_stand_di', overwrite=True)
    spark.stop()
    print('{}：执行完毕！！！'.format(appName))

# 不能全量跑 要一年 甚至半年 否则会py4j.protocol.Py4JError: SparkSession does not exist in the JVM
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_quotes_stand_di.py update 20210101 20210701
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_quotes_stand_di.py update 20210701 20211231
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_quotes_stand_di.py update 20220101 20220701
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_quotes_stand_di.py update 20220701 20221231
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_quotes_stand_di.py update 20230101 20230203
if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        run_type = sys.argv[1]
        if run_type == 'all':
            start_date = '20210101'
            end_date = date.today().strftime('%Y%m%d')
        else:
            start_date = date.today().strftime('%Y%m%d')
            end_date = start_date
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]
    # start_date, end_date = '20230110','20230110'
    # start_date, end_date = '20230110','20230113'
    start_time = time.time()
    factors = ['volume','volume_ratio_1d','volume_ratio_5d','turnover','turnover_rate','turnover_rate_5d','turnover_rate_10d','total_market_value','pe','pe_ttm']
    # factors = ['turnover','pe_ttm']
    get_data(factors,start_date, end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))