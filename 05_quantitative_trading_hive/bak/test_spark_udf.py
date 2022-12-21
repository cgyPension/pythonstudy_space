import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import get_spark
from pyspark.sql.types import *
import pandas as pd
# import talib as ta
# import MyTT as mt

# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


def cj_func(a:pd.Series,b:pd.Series):
    return a * b

# def mt_rsi(close:pd.Series,n):
#     dif = close - mt.REF(close, 1)
#     return mt.RD(mt.SMA(mt.MAX(dif, 0), n) / mt.SMA(mt.ABS(dif), n) * 100)

def m_sum(close:pd.Series):
    # rt = close.sum()
    rt=pd.Series(close).sum()
    return rt

'''环境有问题单独识别不了别的文件'''
def registerUDF(spark):
    '''批量注册udf'''
    spark_udf = spark.udf
    spark_udf.register('cj_func', cj_func, returnType=DecimalType(precision=20,scale=4))
    spark_udf.register('m_sum',m_sum, returnType=DecimalType(precision=20,scale=4))
    # spark_udf.register('mt_rsi', mt_rsi, returnType=DecimalType(precision=20, scale=4))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/test_spark_udf.py
# nohup python /opt/code/05_quantitative_trading_hive/bak/test_spark.py >> my.log 2>&1 &
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/test_spark_udf.py
# 这个要用远程调试执行 不然本地读不了linux
if __name__ == '__main__':
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    registerUDF(spark)

    spark.sql("""
        select trade_date,
           stock_code,
           stock_name,
           close_price,
           turnover_rate,
           cj_func(close_price,turnover_rate) as cj
           -- m_sum(close_price) as ms
           -- m_sum(close_price)over(partition by trade_date) as ms 不支持
    from stock.ods_dc_stock_quotes_di
    where td = '2022-12-01'
        """).show(20)

#     spark.sql("""
#     select trade_date,
#        stock_code,
#        stock_name,
#        close_price,
#        turnover_rate,
#        cj_func(close_price,turnover_rate) as cj,
#        --ta_rsi(close_price,6) as ta,
#        mt_rsi(close_price,6) as mt
# from stock.ods_dc_stock_quotes_di
# where td >= '2022-12-01'
#     """).show()

    # spark.sql("""
    #     select trade_date,
    #        stock_code,
    #        stock_name,
    #        close_price,
    #        turnover_rate,
    #        cj_func(close_price,turnover_rate) as cj,
    #        ta_rsi(close_price,6)over(partition by stock_code order by trade_date asc) as ta,
    #        mt_rsi(close_price,6)over(partition by stock_code order by trade_date asc) as mt
    # from stock.ods_dc_stock_quotes_di
    # where td >= '2022-12-01'
    #     """).show()

    print('{}：执行完毕！！！'.format(appName))
    spark.stop()
