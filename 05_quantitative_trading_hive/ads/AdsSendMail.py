import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
import time
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.df_SendMail import send_mail
from datetime import date
import pandas as pd
from util.CommonUtils import get_spark

def get_data(start_date):
    try:
        appName = os.path.basename(__file__)
        spark = get_spark(appName)
        start_date = pd.to_datetime(start_date).date()
        sql = """
                select trade_date as `交易日期`,
                       stock_code as `股票代码`,
                       stock_name as `股票名称`,
                       open_price as `开盘价`,
                       close_price as `收盘价`,
                       high_price as `最高价`,
                       low_price as `最低价`,
                       change_percent as `涨跌幅%%`,
                       volume_ratio_1d as `量比_1d 与昨日对比`,
                       volume_ratio_5d as `量比：过去5个交易日`,
                       turnover_rate as `换手率%%`,
                       turnover_rate_5d as `5日平均换手率%%`,
                       turnover_rate_10d as `10日平均换手率%%`,
                       total_market_value as `总市值`,
                       round(pe_ttm/net_profit_growth_rate,4) as `peg`,
                       industry_plate as `行业板块`,
                       concept_plates as `概念板块 ,拼接`,
                       stock_label_names as `股票标签名称 ,拼接`,
                       sub_factor_names as `主观因子标签名称 ,拼接`,
                       stock_strategy_name as `股票策略名称 股票标签名称 +拼接`,
                       stock_strategy_ranking as `策略内排行`
                from stock.ads_stock_suggest_di
                where td = '%s'
                      and stock_strategy_name !='小市值+市盈率TTM+换手率'
        """% (start_date)

        spark_df = spark.sql(sql)
        pd_df = spark_df.toPandas()

        # 发送邮件
        send_mail(pd_df,'最近资金流入国企')
    except Exception as e:
        print(e)
    print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/05_quantitative_trading_hive/ods/tmp_dwd_stock_quotes_stand_di.py
# nohup tmp_dwd_stock_quotes_stand_di.py >> my.log 2>&1 &
# python tmp_dwd_stock_quotes_stand_di.py
if __name__ == '__main__':
    start_time = time.time()
    # start_date = date.today()
    start_date = '20221205'
    get_data(start_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))
