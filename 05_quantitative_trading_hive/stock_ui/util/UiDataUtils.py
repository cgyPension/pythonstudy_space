import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from datetime import date, datetime
import pandas as pd
import akshare as ak
from PyQt5.QtCore import QDate

from util.CommonUtils import get_spark


class UiDataUtils:
    """
    application 程序控制
    """
    def __init__(self):
        # 配置信息
        self.appName = os.path.basename(__file__)
        self.spark = get_spark(self.appName)
        trade_df = ak.tool_trade_date_hist_sina()
        trade_df = trade_df[(trade_df['trade_date'] >= datetime.timedelta(date.today() - 365))&(trade_df['trade_date'] <= date.today())].reset_index(drop=True)
        self.start_date = trade_df.iloc[0,0]
        self.end_date = trade_df.iloc[-1,0]

    def start(self):
        stock_df, industry_df = self.get_dwd_stock_quotes_stand_di(self), self.get_industry_plate(self)

    def get_dwd_stock_quotes_stand_di(self):
        spark_df = self.spark.sql('''
        select trade_date,
               stock_code,
               stock_name,
               open_price,
               close_price,
               high_price,
               low_price,
               rps_5d,
               rps_10d,
               rps_20d,
               rps_50d,
               rs,
               ma_5d,
               ma_10d,
               ma_20d,
               ma_50d,
               ma_120d,
               ma_200d,
               ma_250d
        from stock.dwd_stock_quotes_stand_di
        where td between '%s' and '%s'
        '''% (self.start_date,self.end_date))
        pd_df = spark_df.toPandas()
        return pd_df

    def get_industry_plate(self):
        spark_df = self.spark.sql('''
        with t2 as (
        select trade_date,
               industry_plate,
               rps_5d,
               rps_10d,
               rps_20d,
               rps_50d
        from stock.dim_dc_stock_plate_di
        where td between '%s' and '%s'
            group by trade_date,industry_plate,rps_5d,rps_10d,rps_20d,rps_50d
        )
        select t1.trade_date,
               t1.industry_plate,
               t1.open_price,
               t1.close_price,
               t1.high_price,
               t1.low_price,
               t1.change_percent,
               t1.volume,
               t1.turnover,
               t1.turnover_rate,
               t2.rps_5d,
               t2.rps_10d,
               t2.rps_20d,
               t2.rps_50d
        from stock.ods_dc_stock_industry_plate_hist_di t1
        left join t2
        where t1.td between '%s' and '%s'
        '''% (self.start_date,self.end_date,self.start_date,self.end_date))
        pd_df = spark_df.toPandas()
        return pd_df

    # 查询板块所有成分股
    def get_industry_stock(self,industry_plate):
        spark_df = self.spark.sql('''
        select trade_date,
               stock_code,
               stock_name,
               open_price,
               close_price,
               high_price,
               low_price,
               rps_5d,
               rps_10d,
               rps_20d,
               rps_50d,
               rs,
               ma_5d,
               ma_10d,
               ma_20d,
               ma_50d,
               ma_120d,
               ma_200d,
               ma_250d
        from stock.dwd_stock_quotes_stand_di
        where td between '%s' and '%s'
                where industry_plate ='%s'
        '''% (self.start_date,self.end_date,industry_plate))
        pd_df = spark_df.toPandas()
        return pd_df

    def trade_date(self):
        # 新浪财经的股票交易日历数据
        df = ak.tool_trade_date_hist_sina()
        df = df[(df['trade_date'] <= date.today()) & (pd.to_datetime('2021-01-01').date() <= df['trade_date'])].reset_index(drop=True)
        return QDate.fromString(df, 'yyyy-MM-dd')

    def __exit__(self):
        self.spark.stop()
        print('{}：执行完毕！！！'.format(self.appName))

def str_amount(x):
    return '%.2f亿' % (float(x) / 100000000)

def str_value(x):
    return '%.2f万手' % (float(x) / 10000)

def str_change(x):
    change_str = '%.2f%%' % ((x['close'] - x['open']) / x['open'] * 100)
    return change_str

def trade_date():
    # 新浪财经的股票交易日历数据
    df = ak.tool_trade_date_hist_sina()
    df = df[(df['trade_date'] <= date.today()) & (pd.to_datetime('2021-01-01').date() <= df['trade_date'])].reset_index(drop=True)
    # return QDate.fromString(df, 'yyyy-MM-dd')
    return df

if __name__ == '__main__':
    # 项目启动类
    application = UiDataUtils()
    stock_df, industry_df = application.start()
