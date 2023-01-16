import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import time
import warnings
from datetime import date,datetime
import akshare as ak
import numpy as np
import pandas as pd
import statsmodels.api as sm
from util.factorFormatUtils import neutralization, factor_ic, ic_ir
from util.CommonUtils import get_spark

# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


def get_data(start_date, end_date):
    '''
    缺失值处理：由于缺失值处理在方法上都使用了截面均值，或直接删除缺失数据，这样只能单个因子检测 除非忽略一部分数据 全部因子一起检测

    '''
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()

    # 剔除ST、涨停、停牌、新股
    # 要是ICIR检测  # 剔除ST、涨停、停牌、新股
    # f_dwd 行业市值中性化要不要去掉？  # 剔除ST、涨停、停牌、新股 还是不要剔除这些了


    # 需要进行 去极值、标准化、行业市值中性化的因子
    # factors_a = ['volume_ratio_1d','volume_ratio_5d','total_market_value','turnover_rate','pe_ttm']
    # for factor in factors_a:
    #     pass
    #
    # # 可以直接使用的因子
    # factors_b = ['sub_factor_score']
    # for factor in factors_b:
    #     pass
    factor = 'volume_ratio_1d'
    spark_df = spark.sql("""
       with tmp_01 as (
       select trade_date,
              stock_code,
              stock_name,
              %s as factor,
              -- 原dwd参考的是实际操作用开盘价
              lead(close_price,%s)over(partition by stock_code order by trade_date)/close_price-1 as holding_yield_n,
              total_market_value,
              industry_plate,
              suspension_time,
              estimated_resumption_time
       from stock.dwd_stock_quotes_di
       where td between '%s' and '%s'
               and stock_name not rlike 'ST'
               and nvl(concept_plates,'保留null') not rlike '次新股'
               and nvl(stock_label_names,'保留null') not rlike '当天涨停'
              
       ),
        --去除or 停复牌
       tmp_02 as (
       select a.*
       from tmp_01 a
       left join (select trade_date,lead(trade_date,1)over(order by trade_date) as next_trade_date from stock.ods_trade_date_hist_sina_df) b
            on a.trade_date = b.trade_date
       where a.suspension_time is null
             or a.estimated_resumption_time < b.next_trade_date
       ),
       -- 去极值
       tmp_median as (
        select trade_date,
               stock_code,
               stock_name,
               (case when factor<=median-3*new_median then median-3*new_median
                     when factor>=median+3*new_median then median+3*new_median
                     else factor end) as factor,
               holding_yield_n,
               total_market_value,
               industry_plate
        from (
              select  *,
                      percentile(factor,0.5)over(partition by trade_date) as median,
                      percentile(abs(factor-percentile(factor,0.5)over(partition by trade_date)),0.5)over(partition by trade_date) as new_median
              from tmp_02
              where holding_yield_n is not null
                     and factor is not null
             )
       ),
       -- 标准化
       tmp_std as (
        select trade_date,
               stock_code,
               stock_name,
               factor-avg(factor)over(partition by trade_date)/std(factor)over(partition by trade_date) as factor,
               holding_yield_n,
               -- 取对数为了近似正态分布
               log(total_market_value) as market_value,
               industry_plate
        from tmp_median
       )
       select *
       from tmp_std
        """% (factor,2,start_date, end_date))

    pd_df = spark_df.toPandas()
    pd_df.rename(columns={'factor': factor}, inplace=True)
    pd_df[factor] = pd_df[factor].astype(float)
    pd_df['holding_yield_n'] = pd_df['holding_yield_n'].astype(float)

    r_df = neutralization(factor,pd_df)
    a = factor_ic(factor,r_df)
    print(a)
    b = ic_ir(factor,a)
    print(b)

    spark.stop()
    print('{}：执行完毕！！！'.format(appName))



# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/stock_icir_df.py 20221201 20230112
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/stock_icir_df.py 20221226 20221230
if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        start_date = sys.argv[1]
        end_date = date.today().strftime('%Y%m%d')
    elif len(sys.argv) == 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    start_date, end_date = '20220601','20221226'
    start_time = time.time()
    get_data(start_date,end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))