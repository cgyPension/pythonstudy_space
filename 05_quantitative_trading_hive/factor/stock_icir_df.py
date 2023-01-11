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
from util.algorithmUtils import mt_rsi, ta_rsi
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

    spark_df = spark.sql("""
       with tmp_01 as (
       select trade_date,
              stock_code,
              stock_name,
              volume_ratio_1d,
              holding_yield_2d,
              holding_yield_5d,
              total_market_value,
              industry_plate
       from stock.dwd_stock_quotes_di
       where td between '%s' and '%s'
               and stock_name not rlike 'ST'
               and nvl(concept_plates,'保留null') not rlike '次新股'
               and nvl(stock_label_names,'保留null') not rlike '当天涨停'
               and volume_ratio_1d is not null
               and holding_yield_2d is not null
               and holding_yield_5d is not null
       ),
        --去除or 停复牌
       tmp_02 as (
       select *
       from tmp_01
       where suspension_time is null
             or estimated_resumption_time < date_add(trade_date,1)
       ),
       -- 去极值
       tmp_median as (
        select trade_date,
               stock_code,
               stock_name,
               (case when volume_ratio_1d<=median-3*new_median then median-3*new_median
                     when volume_ratio_1d>=median+3*new_median then median+3*new_median
                     else volume_ratio_1d end) as volume_ratio_1d,
               holding_yield_2d,
               holding_yield_5d,
               total_market_value,
               industry_plate,
        from (
              select  *,
                      percentile(volume_ratio_1d,0.5)over(partition by trade_date) as median,
                      percentile(abs(volume_ratio_1d-percentile(volume_ratio_1d,0.5)over(partition by trade_date)),0.5)over(partition by trade_date) as new_median
              from tmp_02
             )
       ),
       -- 标准化
       tmp_std as (
        select trade_date,
               stock_code,
               stock_name,
               volume_ratio_1d-avg(volume_ratio_1d)over(partition by trade_date)/std(volume_ratio_1d)over(partition by trade_date) as volume_ratio_1d,
               holding_yield_2d,
               holding_yield_5d,
               log(total_market_value) as total_market_value,
               industry_plate,
        from tmp_median
       )
       select *
       from tmp_std
        """% (start_date, end_date))

    pd_df = spark_df.toPandas()
    # pd_df['rsi_6d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta_rsi(x['close'],6))
    # pd_df['rsi_12d'] = pd_df.groupby('stock_code',group_keys=False).apply(lambda x: ta_rsi(x['close'],12))
    # pd_df['update_time'] = datetime.now()
    # pd_df['td'] = pd_df['trade_date']
    # pd_df = pd_df[['trade_date', 'stock_code', 'stock_name', 'rsi_6d', 'rsi_12d', 'update_time', 'td']]
    # spark_df = spark.createDataFrame(pd_df)
    # spark_df.repartition(1).write.insertInto('factor.stock_icir_df', overwrite=True)

    # x = cfoa_day.iloc[:, 1:]  # 市值/行业
    # y = cfoa_day.iloc[:, 0]  # 因子值

    # 行业市值中性化 第一个是y = 因子值 第二个是x= 市值+行业
    pd_df['OLS'] = pd_df.groupby('trade_date', group_keys=False).apply(lambda x: sm.OLS(x['volume_ratio_1d'].astype(float),
                                                                                        x['total_market_value'].join(sm.categorical(x['industry_plate'],drop=True)).astype(float), hasconst=False, missing='drop').fit().resid)

    print(pd_df)

    spark.stop()
    print('{}：执行完毕！！！'.format(appName))



# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/stock_icir_df.py 20230103 20230106
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

    start_time = time.time()
    get_data(start_date,end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))