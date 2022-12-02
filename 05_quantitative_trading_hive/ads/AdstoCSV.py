import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import akshare as ak
import time
from util.df_SendMail import send_mail
from datetime import date
import pandas as pd
from util.CommonUtils import get_spark

def get_data(start_date):
    try:
        appName = os.path.basename(__file__)
        # 本地模式
        spark = get_spark(appName)
        start_date=pd.to_datetime(start_date).date()
        # 新浪财经的股票交易日历数据
        df = ak.tool_trade_date_hist_sina()
        df = df[df['trade_date'] > start_date].reset_index(drop=True)
        next_date = df.iat[0,0] # 下一个交易日

        sql = """
                select substr(stock_code,3)
                from stock.ads_stock_suggest_di
                where td = '%s';
        """% (start_date)

        spark_df = spark.sql(sql)
        pd_df = spark_df.toPandas()

        # 写入文件
        print('{}小市值策略'.format(next_date.strftime('%m%d')))
        print('{}小市值策略'.format(next_date.strftime('%Y%m%d')))
        # 定时上传到ptrade的文件路径
        # path = 'C:/Users/Administrator/Desktop/trade_data.csv'
        # .sel 同花顺要求的格式
        path = '/opt/code/pythonstudy_space/量化投资操作.sel'
        pd_df.to_csv(path, index=False, header=0,mode='w', encoding='utf-8')
    except Exception as e:
        print(e)
    print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/05_quantitative_trading_hive/ods/AdsSendMail.py
# nohup AdsSendMail.py >> my.log 2>&1 &
# python AdsSendMail.py
if __name__ == '__main__':
    start_time = time.time()
    # current_dt = date.today()
    start_date = '20221201'
    get_data(start_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))
