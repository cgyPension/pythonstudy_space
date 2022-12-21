import os
import sys
import time
import warnings
import datetime
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import akshare as ak
import numpy as np
import pandas as pd

from util.CommonUtils import get_spark

warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


def get_data(start_date):
    try:
        appName = os.path.basename(__file__)
        # 本地模式
        spark = get_spark(appName)

        # 只能获取当天的数据
        df = ak.stock_board_industry_name_em()
        # 去重、保留最后一次出现的
        df.drop_duplicates(subset=['板块代码'], keep='last', inplace=True)
        
        start_date = pd.to_datetime(start_date).date()
        df['trade_date'] = start_date
        df['td'] = df['trade_date'] # 分区字段放最后
        df['update_time'] = datetime.datetime.now()

        df.rename(columns={'板块名称': 'industry_plate','板块代码': 'industry_plate_code','最新价': 'new_price','涨跌额': 'change_amount','涨跌幅': 'change_percent','总市值': 'total_market_value','换手率': 'turnover_rate','上涨家数': 'rise_num','下跌家数': 'fall_num','领涨股票': 'leading_stock_name','领涨股票-涨跌幅': 'leading_stock_change_percent'}, inplace=True)
        df = df[['trade_date','industry_plate_code','industry_plate','new_price','change_amount','change_percent','total_market_value','turnover_rate','rise_num','fall_num','leading_stock_name','leading_stock_change_percent','update_time','td']]
        # MySQL无法处理nan
        df = df.replace({np.nan: None})
 
        spark_df = spark.createDataFrame(df)
        # # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
        spark_df.repartition(1).write.insertInto('stock.ods_dc_stock_industry_plate_rt_di',overwrite=True)  # 如果执行不存在这个表，会报错
        spark.stop
    except Exception as e:
        print(e)
    print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/05_quantitative_trading_hive/ods/ods_dc_stock_industry_plate_rt_di.py
# nohup python ods_dc_stock_industry_plate_rt_di.py >> my.log 2>&1 &
# python ods_dc_stock_industry_plate_rt_di.py
if __name__ == '__main__':
    start_time = time.time()
    start_date = datetime.date.today()
    get_data(start_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))