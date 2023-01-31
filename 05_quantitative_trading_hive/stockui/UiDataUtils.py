import os
import sys
# 在linux会识别不了包 所以要加临时搜索目
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from datetime import date
import datetime
import pandas as pd
import akshare as ak
from util.CommonUtils import get_spark
from PyQt5.QtCore import QDate



class UiDataUtils:
    """
    application 程序控制
    """
    def __init__(self):
        # 配置信息
        self.appName = os.path.basename(__file__)
        self.spark = get_spark(self.appName)
        trade_df = ak.tool_trade_date_hist_sina()
        self.q_end_date = self.get_max_date()
        self.end_date = self.get_max_date().toPyDate()
        trade_df = trade_df[(trade_df['trade_date'] >= self.end_date - datetime.timedelta(365)) & (
                    trade_df['trade_date'] <= self.end_date)].reset_index(drop=True)
        self.start_date = trade_df.iloc[0,0]
        self.q_start_date = QDate.fromString(str(self.start_date), 'yyyy-MM-dd')


    def get_all_stock(self,start_date,end_date):
        spark_df = self.spark.sql('''
        select trade_date,
               stock_code as code,
               stock_name as name,
               round(open_price,2) as open_price,
               round(close_price,2) as close_price,
               round(high_price,2) as high_price,
               round(low_price,2) as low_price,
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
        order by (rps_20d-rps_5d) desc 
        '''% (start_date,end_date))
        pd_df = spark_df.toPandas()
        return pd_df

    def query_stock(self,stock_codes,start_date,end_date):
        spark_df = self.spark.sql('''
        select trade_date,
               stock_code as code,
               stock_name as name,
               open_price,
               close_price,
               high_price,
               low_price,
               volume,
               turnover,
               change_percent,
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
                and stock_code in ('%s')
        order by trade_date
        '''% (start_date,end_date,stock_codes))
        pd_df = spark_df.toPandas()
        return pd_df


    def query_stock_rps(self,stock_code,start_date,end_date):
        start_date,end_date = pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()
        spark_df = self.spark.sql('''
               select rps_5d,
                      rps_10d,
                      rps_20d,
                      rps_50d
                from stock.dwd_stock_quotes_stand_di
                where td between '%s' and '%s'
                        and stock_code = '%s'
        '''% (start_date,end_date,stock_code))
        pd_df = spark_df.toPandas()
        return pd_df

    def get_all_plate(self,start_date,end_date):
        start_date = pd.to_datetime(start_date).date()
        spark_df = self.spark.sql('''
                select trade_date,
                      plate_name as name,
                      round(change_percent,2) as change_percent,
                      volume,
                      turnover,
                      round(turnover_rate,2) as turnover_rate,
                      rps_5d,
                      rps_10d,
                      rps_15d,
                      rps_20d,
                      rps_50d,
                      round(open_price,2) as open_price,
                      round(close_price,2) as close_price,
                      round(high_price,2) as high_price,
                      round(low_price,2) as low_price,
                      ma_5d,
                      ma_10d,
                      ma_20d,
                      ma_50d,
                      ma_120d,
                      ma_150d,
                      ma_200d,
                      ma_250d,
                      high_price_250d,
                      low_price_250d
                from stock.dim_plate_df
                where td between '%s' and '%s'
                        and ma_250d is not null 
                        -- 成交额>=50亿
                        and turnover>=5000000000
                order by (rps_5d+rps_10d+rps_15d+rps_20d) desc,change_percent desc
        '''% (start_date,end_date))
        pd_df = spark_df.toPandas()
        return pd_df

    def query_plate(self,plate_name,start_date,end_date):
        start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
        spark_df = self.spark.sql('''
                select trade_date,
                      plate_name as name,
                      round(change_percent,2) as change_percent,
                      volume,
                      turnover,
                      round(turnover_rate,2) as turnover_rate,
                      rps_5d,
                      rps_10d,
                      rps_15d,
                      rps_20d,
                      rps_50d,
                      round(open_price,2) as open_price,
                      round(close_price,2) as close_price,
                      round(high_price,2) as high_price,
                      round(low_price,2) as low_price,
                      ma_5d,
                      ma_10d,
                      ma_20d,
                      ma_50d,
                      ma_120d,
                      ma_150d,
                      ma_200d,
                      ma_250d,
                      high_price_250d,
                      low_price_250d
                from stock.dim_plate_df
                where td between '%s' and '%s'
                    and plate_name = '%s'
        '''% (start_date,end_date,plate_name))
        pd_df = spark_df.toPandas()
        return pd_df

    def query_plate_rps(self,plate_name,start_date,end_date):
        start_date,end_date = pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()
        spark_df = self.spark.sql('''
                select rps_5d,
                      rps_10d,
                      rps_15d,
                      rps_20d,
                      rps_50d
                from stock.dim_plate_df
                where td between '%s' and '%s'
                        and plate_name = '%s'
        '''% (start_date,end_date,plate_name))
        pd_df = spark_df.toPandas()
        return pd_df



    def query_plate_range(self,plate_name,start_date,end_date):
        start_date,end_date = pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()
        spark_df = self.spark.sql('''
                select plate_name as name,
                      rps_5d,
                      rps_10d,
                      rps_15d,
                      rps_20d,
                      rps_50d
                from stock.dim_plate_df
                where td between '%s' and '%s'
                        and plate_name = '%s'
        '''% (start_date,end_date,plate_name))
        pd_df = spark_df.toPandas()
        return pd_df

    def query_plate_stock(self, plate):
        '''查询板块所有成分股'''
        spark_df = self.spark.sql('''
                with t1 as (
                select stock_code,
                       stock_name,
                       industry_plate,
                       concept_plates,
                       rps_20d,
                       rps_5d
                from stock.dwd_stock_quotes_stand_di
                where td ='%s'
                )
                select stock_code as code,
                       stock_name as name
                from t1
                where industry_plate = '%s'
                        or concept_plates rlike '%s'
                order by (rps_20d-rps_5d) desc
        ''' % (self.end_date, plate, plate))
        pd_df = spark_df.toPandas()
        return pd_df

    def get_trading_date(self,start_date,end_date):
        start_date,end_date = pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()
        # 获取市场的交易时间
        spark_df = self.spark.sql('''
        select trade_date
        from stock.ods_trade_date_hist_sina_df
        where trade_date between '%s' and '%s'
        '''% (start_date,end_date))
        pd_df = spark_df.toPandas()
        return pd_df

    def get_max_date(self):
        '''得到数据最大日期'''
        spark_df = self.spark.sql('''
        select max(td)
        from stock.dwd_stock_quotes_stand_di
        ''')
        pd_df = spark_df.toPandas()
        # 转为qt用的格式QDate
        return QDate.fromString(str(pd_df.iloc[0, 0]), 'yyyy-MM-dd')

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


def trade_date(date, is_Forward=True):
    """返回一个合法的交易日期QDate"""
    date_str = date.toString('yyyy-MM-dd')
    # date_str = legal_trade_date(date_str, is_Forward)
    return QDate.fromString(date_str, 'yyyy-MM-dd')

# def trade_date():
#     # 新浪财经的股票交易日历数据
#     df = ak.tool_trade_date_hist_sina()
#     df = df[(df['trade_date'] <= date.today()) & (pd.to_datetime('2021-01-01').date() <= df['trade_date'])].reset_index(drop=True)
#     df['trade_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
#     return QDate.fromString(df['trade_date'], 'yyyy-MM-dd')
#     # return df



# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/back_stockui/UiDataUtils.py
if __name__ == '__main__':
    print(11)
