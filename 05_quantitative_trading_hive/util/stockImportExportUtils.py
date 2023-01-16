import os
import sys
import execjs
import time
from datetime import date
from datetime import datetime
import pandas as pd
import akshare as ak
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import get_spark, str_pre
from util.同花顺自选股 import all_del_add_stocks, all_zt_stock_add_account

'''股票导入导出工具类'''
def datagrip_hive_where(codes):
    try:
        appName = os.path.basename(__file__)
        current_dt = date.today()
        df = ak.tool_trade_date_hist_sina()
        df = df[df['trade_date'] <= current_dt].reset_index(drop=True)
        # 当天或上一交易日
        current_dt = df.iat[-1, 0]
        r = []
        print(codes)
        for code in codes:
            code = str_pre(code)
            r.append(code)
        print("td = '{}' and stock_code in ({})".format(current_dt, str(r)[1:-1]))
    except Exception as e:
        print(e)
    print('{}：执行完毕！！！'.format(appName))

def export_hive(start_date,sql,stock_strategy_name='all量化投资操作',ths=False,csv=False):
    start_date = pd.to_datetime(start_date).date()
    # 新浪财经的股票交易日历数据
    df = ak.tool_trade_date_hist_sina()
    df = df[df['trade_date'] > start_date].reset_index(drop=True)
    next_date = df.iat[0, 0]  # 下一交易日
    print(next_date.strftime('%m%d') + stock_strategy_name)
    print(next_date.strftime('%Y%m%d') + stock_strategy_name)

    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    spark_df = spark.sql(sql)
    pd_df = spark_df.toPandas()
    codes = pd_df['stock_code'].tolist()
    print(codes)
    if ths:
        all_del_add_stocks(codes)
    elif csv:
        path1 = '/opt/code/pythonstudy_space/{}.sel'.format(stock_strategy_name)  # .sel 同花顺要求的格式
        path2 = '/opt/code/pythonstudy_space/{}.txt'.format(stock_strategy_name)  # .txt 东方财富要求格式
        pd_df.to_csv(path1, index=False, header=0,mode='w', encoding='utf-8')
    spark.stop()
    print('{}：执行完毕！！！'.format(appName))

def export_stock_ndzt_hive(start_date,codes,stock_strategy_name='女帝打板',ths=False,csv=False):
    r = []
    for code in codes:
        code = str_pre(code)
        r.append(code)
    sql = """
                select substr(stock_code,3) as stock_code
                from stock.dwd_stock_quotes_di
                where td = '%s'
                    and stock_code in(%s)
                    and stock_label_names rlike '当天涨停'
                order by sub_factor_score desc,turnover_rate
            """% (start_date,str(r)[1:-1])
    export_hive(start_date,sql, stock_strategy_name)

def export_stock_zt_hive(start_date,stock_strategy_name='昨日涨停',ths=False,csv=False):
    sql = """
                select substr(stock_code,3) as stock_code
                from stock.dwd_stock_quotes_di
                where td = '%s'
                    and stock_label_names rlike '当天涨停'
                order by sub_factor_score desc,turnover_rate
            """% (start_date)
    export_hive(start_date,sql, stock_strategy_name)

def export_stock_qs_hive(start_date,stock_strategy_name='昨日强势',ths=False,csv=False):
    sql = """
                select substr(stock_code,3) as stock_code
                from stock.dwd_stock_strong_di
                where td = '%s'
                order by change_percent
            """% (start_date)
    export_hive(start_date,sql, stock_strategy_name)


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/util/stockImportExportUtils.py
if __name__ == '__main__':
    start_time = time.time()
    codes = ['002689','002094','002651','002264','002808','002888','003040','002762','002238','002766','003028']

    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))