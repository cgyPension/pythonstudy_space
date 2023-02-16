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
from util.CommonUtils import get_spark, get_trade_date_nd
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
        start_date_lag30 = get_trade_date_nd(start_date, -30)
        spark_df = self.spark.sql('''
            with t1 as (
            select *,
                   max(rps_5d)over(partition by stock_code order by trade_date rows between 19 preceding and current row)-rps_5d as rank_top
            from stock.dwd_stock_quotes_stand_di
            where td between '%s' and '%s'
            )
            select trade_date,
                   stock_code as code,
                   stock_name as name,
                   round(open_price,2) as open_price,
                   round(close_price,2) as close_price,
                   round(high_price,2) as high_price,
                   round(low_price,2) as low_price,
                   round(change_percent,2) as change_percent,
                   volume_ratio_1d
                   volume_ratio_5d,
                   turnover_rate,
                   industry_plate,
                   concept_plates,
                   stock_label_names,
                   sub_factor_names,
                   sub_factor_score,
                   hot_rank,
                   rps_5d,
                   rps_10d,
                   rps_20d,
                   rps_50d
            from t1
            where trade_date between '%s' and '%s'
            order by rank_top desc,turnover_rate
        '''% (start_date_lag30,end_date,start_date,end_date))
        pd_df = spark_df.toPandas()
        return pd_df

    def query_stock(self,stock_codes,start_date,end_date):
        '''要来做k线图的字段'''
        spark_df = self.spark.sql('''
        select trade_date,
               stock_code as code,
               stock_name as name,
               round(open_price,2) as open_price,
               round(close_price,2) as close_price,
               round(high_price,2) as high_price,
               round(low_price,2) as low_price,
               round(change_percent,2) as change_percent,
               volume,
               turnover,
               rps_5d,
               rps_10d,
               rps_20d,
               rps_50d,
               round(ma_5d,2)as ma_5d,
               round(ma_10d,2)as ma_10d,
               round(ma_20d,2)as ma_20d,
               round(ma_50d,2)as ma_50d,
               round(ma_120d,2)as ma_120d,
               round(ma_200d,2)as ma_200d,
               round(ma_250d,2)as ma_250d
        from stock.dwd_stock_quotes_stand_di
        where td between '%s' and '%s'
                and stock_code in ('%s')
        order by trade_date
        '''% (start_date,end_date,stock_codes))
        pd_df = spark_df.toPandas()
        return pd_df

    def query_stock_where(self,start_date,end_date,text=None):
        start_date_lag30 = get_trade_date_nd(start_date, -30)
        if text is None:
           spark_df = self.spark.sql('''
               with t1 as (
               select *,
                      max(rps_5d)over(partition by stock_code order by trade_date rows between 19 preceding and current row)-rps_5d as rank_top
               from stock.dwd_stock_quotes_stand_di
               where td between '%s' and '%s'
               )
               select trade_date,
                      stock_code as code,
                      stock_name as name,
                      round(open_price,2) as open_price,
                      round(close_price,2) as close_price,
                      round(high_price,2) as high_price,
                      round(low_price,2) as low_price,
                      round(change_percent,2) as change_percent,
                      volume_ratio_1d
                      volume_ratio_5d,
                      turnover_rate,
                      industry_plate,
                      concept_plates,
                      stock_label_names,
                      sub_factor_names,
                      sub_factor_score,
                      hot_rank,
                      rps_5d,
                      rps_10d,
                      rps_20d,
                      rps_50d
               from t1
               where trade_date between '%s' and '%s'
               order by rank_top desc,turnover_rate
           '''% (start_date_lag30,end_date,start_date,end_date))
        elif 'td' not in text:
            if 'order by' not in text:
                spark_df = self.spark.sql('''
                    with t1 as (
                    select *,
                           max(rps_5d)over(partition by stock_code order by trade_date rows between 19 preceding and current row)-rps_5d as rank_top
                    from stock.dwd_stock_quotes_stand_di
                    where td between '%s' and '%s'
                    )
                    select trade_date,
                           stock_code as code,
                           stock_name as name,
                           round(open_price,2) as open_price,
                           round(close_price,2) as close_price,
                           round(high_price,2) as high_price,
                           round(low_price,2) as low_price,
                           round(change_percent,2) as change_percent,
                           volume_ratio_1d
                           volume_ratio_5d,
                           turnover_rate,
                           industry_plate,
                           concept_plates,
                           stock_label_names,
                           sub_factor_names,
                           sub_factor_score,
                           hot_rank,
                           rps_5d,
                           rps_10d,
                           rps_20d,
                           rps_50d
                    from t1
                    where trade_date between '%s' and '%s'
                    and %s
                    order by rank_top desc,turnover_rate
                ''' % (start_date_lag30, end_date, start_date, end_date,text))
            else:
                spark_df = self.spark.sql('''
                    with t1 as (
                    select *,
                           max(rps_5d)over(partition by stock_code order by trade_date rows between 19 preceding and current row)-rps_5d as rank_top
                    from stock.dwd_stock_quotes_stand_di
                    where td between '%s' and '%s'
                    )
                    select trade_date,
                           stock_code as code,
                           stock_name as name,
                           round(open_price,2) as open_price,
                           round(close_price,2) as close_price,
                           round(high_price,2) as high_price,
                           round(low_price,2) as low_price,
                           round(change_percent,2) as change_percent,
                           volume_ratio_1d
                           volume_ratio_5d,
                           turnover_rate,
                           industry_plate,
                           concept_plates,
                           stock_label_names,
                           sub_factor_names,
                           sub_factor_score,
                           hot_rank,
                           rps_5d,
                           rps_10d,
                           rps_20d,
                           rps_50d
                    from t1
                    where trade_date between '%s' and '%s'
                    and %s
                ''' % (start_date_lag30, end_date, start_date, end_date,text))
        else:
            spark_df = self.spark.sql('''
               select trade_date,
                      stock_code as code,
                      stock_name as name,
                      round(open_price,2) as open_price,
                      round(close_price,2) as close_price,
                      round(high_price,2) as high_price,
                      round(low_price,2) as low_price,
                      round(change_percent,2) as change_percent,
                      volume_ratio_1d
                      volume_ratio_5d,
                      turnover_rate,
                      industry_plate,
                      concept_plates,
                      stock_label_names,
                      sub_factor_names,
                      sub_factor_score,
                      hot_rank,
                      rps_5d,
                      rps_10d,
                      rps_20d,
                      rps_50d
               from stock.dwd_stock_quotes_stand_di
               where %s
               ''' % (text))
        pd_df = spark_df.toPandas()
        return pd_df

    def get_all_plate(self,start_date,end_date):
        start_date = pd.to_datetime(start_date).date()
        spark_df = self.spark.sql('''
        select trade_date,
              plate_code as code,
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
               --20日内rps首次三线翻红
              is_rps_red,
              round(open_price,2) as open_price,
              round(close_price,2) as close_price,
              round(high_price,2) as high_price,
              round(low_price,2) as low_price,
              round(ma_5d,2)as ma_5d,
              round(ma_10d,2)as ma_10d,
              round(ma_20d,2)as ma_20d,
              round(ma_50d,2)as ma_50d,
              round(ma_120d,2)as ma_120d,
              round(ma_200d,2)as ma_200d,
              round(ma_250d,2)as ma_250d,
              round(high_price_250d,2)as high_price_250d,
              round(low_price_250d,2) as low_price_250d
        from stock.dim_plate_df
        where td between '%s' and '%s'
                -- 成交额>=50亿
                -- and ma_250d is not null
                -- and turnover>=5000000000
        order by trade_date,is_rps_red desc,(rps_5d+rps_10d+rps_15d+rps_20d) desc,change_percent desc
        ''' % (start_date, end_date))
        pd_df = spark_df.toPandas()
        return pd_df

    def query_plate(self,plate_code,start_date,end_date):
        '''要来做k线图的字段'''
        start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
        spark_df = self.spark.sql('''
                select trade_date,
                      plate_code as code,
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
                      is_rps_red,
                      round(open_price,2) as open_price,
                      round(close_price,2) as close_price,
                      round(high_price,2) as high_price,
                      round(low_price,2) as low_price,
                      round(ma_5d,2)as ma_5d,
                      round(ma_10d,2)as ma_10d,
                      round(ma_20d,2)as ma_20d,
                      round(ma_50d,2)as ma_50d,
                      round(ma_120d,2)as ma_120d,
                      round(ma_200d,2)as ma_200d,
                      round(ma_250d,2)as ma_250d,
                      round(high_price_250d,2)as high_price_250d,
                      round(low_price_250d,2) as low_price_250d
                from stock.dim_plate_df
                where td between '%s' and '%s'
                    and plate_code = '%s'
        '''% (start_date,end_date,plate_code))
        pd_df = spark_df.toPandas()
        return pd_df

    def query_plate_stock(self, plate_name,text=None):
        '''查询板块所有成分股 按最近rps差排序 中文查询'''
        end_date_lag30 = get_trade_date_nd(self.end_date, -30)
        if text is None:
            spark_df = self.spark.sql('''
                    with t1 as (
                    select *,
                           max(rps_5d)over(partition by stock_code order by trade_date rows between 19 preceding and current row)-rps_5d as rank_top
                    from stock.dwd_stock_quotes_stand_di
                    where td between '%s' and '%s'
                    ),
                    t2 as (
                    select *
                    from t1
                    where trade_date ='%s'
                    )
                    select stock_code as code,
                           stock_name as name,
                           round(change_percent,2) as change_percent,
                           volume_ratio_1d,
                           volume_ratio_5d,
                           round(turnover_rate,2)  as turnover_rate,
                           industry_plate,
                           concept_plates,
                           stock_label_names,
                           sub_factor_names,
                           sub_factor_score,
                           hot_rank,
                           rps_5d,
                           rps_10d,
                           rps_20d,
                           rps_50d
                    from t2
                    where industry_plate = '%s'
                            or concept_plates rlike '%s'
                    order by rank_top desc,turnover_rate
            ''' % (end_date_lag30, self.end_date, self.end_date, plate_name, plate_name))
        elif 'td' not in text:
            if 'order by' not in text:
                spark_df = self.spark.sql('''
                        with t1 as (
                        select *,
                               max(rps_5d)over(partition by stock_code order by trade_date rows between 19 preceding and current row)-rps_5d as rank_top
                        from stock.dwd_stock_quotes_stand_di
                        where td between '%s' and '%s'
                        ),
                        t2 as (
                        select *
                        from t1
                        where trade_date ='%s'
                        )
                        select stock_code as code,
                               stock_name as name,
                               round(change_percent,2) as change_percent,
                               volume_ratio_1d,
                               volume_ratio_5d,
                               round(turnover_rate,2)  as turnover_rate,
                               industry_plate,
                               concept_plates,
                               stock_label_names,
                               sub_factor_names,
                               sub_factor_score,
                               hot_rank,
                               rps_5d,
                               rps_10d,
                               rps_20d,
                               rps_50d
                        from t2
                        where industry_plate = '%s'
                                or concept_plates rlike '%s'
                                and %s
                        order by rank_top desc,turnover_rate
                ''' % (end_date_lag30, self.end_date, self.end_date, plate_name, plate_name,text))
            else:
                spark_df = self.spark.sql('''
                        with t1 as (
                        select *,
                               max(rps_5d)over(partition by stock_code order by trade_date rows between 19 preceding and current row)-rps_5d as rank_top
                        from stock.dwd_stock_quotes_stand_di
                        where td between '%s' and '%s'
                        ),
                        t2 as (
                        select *
                        from t1
                        where trade_date ='%s'
                        )
                        select stock_code as code,
                               stock_name as name,
                               round(change_percent,2) as change_percent,
                               volume_ratio_1d,
                               volume_ratio_5d,
                               round(turnover_rate,2)  as turnover_rate,
                               industry_plate,
                               concept_plates,
                               stock_label_names,
                               sub_factor_names,
                               sub_factor_score,
                               hot_rank,
                               rps_5d,
                               rps_10d,
                               rps_20d,
                               rps_50d
                        from t2
                        where industry_plate = '%s'
                                or concept_plates rlike '%s'
                                and %s
                ''' % (end_date_lag30, self.end_date, self.end_date, plate_name, plate_name,text))
        else:
            spark_df = self.spark.sql('''
                    with t1 as (
                    select *
                    from stock.dwd_stock_quotes_stand_di
                    where %s
                    )
                    select stock_code as code,
                           stock_name as name,
                           round(change_percent,2) as change_percent,
                           volume_ratio_1d,
                           volume_ratio_5d,
                           round(turnover_rate,2)  as turnover_rate,
                           industry_plate,
                           concept_plates,
                           stock_label_names,
                           sub_factor_names,
                           sub_factor_score,
                           hot_rank,
                           rps_5d,
                           rps_10d,
                           rps_20d,
                           rps_50d
                    from t2
                    where industry_plate = '%s'
                            or concept_plates rlike '%s'
            ''' % (text, plate_name, plate_name))


        pd_df = spark_df.toPandas()
        return pd_df

    def get_trading_date(self,start_date,end_date):
        '''获取区间内的交易日期'''
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

def trade_q_date(date):
    """返回一个QDate类型日期"""
    date_str = date.toString('yyyy-MM-dd')
    return QDate.fromString(date_str, 'yyyy-MM-dd')

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/back_stockui/UiDataUtils.py
if __name__ == '__main__':
    print(11)
