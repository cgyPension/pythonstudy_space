import os
import sys
import time
import warnings
from datetime import date, datetime
import akshare as ak
import numpy as np
import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import str_pre, get_spark
warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)



def get_data(start_date, end_date):
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    try:
        '''
        目标地址: http://data.eastmoney.com/hsgtcg/StockStatistics.aspx
        https://data.eastmoney.com/hsgtcg/StockStatistics.html
        描述: 东方财富网-数据中心-沪深港通-沪深港通持股-每日个股统计
        限量: 单次获取指定 market 的 start_date 和 end_date 之间的所有数据, 该接口只能获取近期的数据
    
        原网站更新数据很慢 基本要第二天 而且还存在持股市值变化(元)还没更新的情况
        '''
        df = ak.stock_hsgt_stock_statistics_em(symbol="北向持股", start_date=start_date,end_date=end_date)
        if df.empty:
            return
    except Exception as e:
        if "'NoneType' object is not subscriptable" in str(e):
            print('https://data.eastmoney.com/hsgtcg/StockStatistics.html   {}网站数据没更新 第二天重跑昨天！！！'.format(appName))
            # return pd.DataFrame
            return
        else:
            print(e)


    # 去重、保留最后一次出现的
    df.drop_duplicates(subset=['持股日期', '股票代码'], keep='last', inplace=True)
    df['stock_code'] = df['股票代码'].apply(str_pre)
    df['td'] = df['持股日期']
    df['update_time'] = datetime.now()

    df.rename(columns={'持股日期': 'trade_date', '股票简称': 'stock_name', '当日涨跌幅': 'change_percent', '持股数量': 'hold_stock_nums',
                       '持股市值': 'hold_market', '持股数量占发行股百分比': 'hold_stock_nums_rate', '持股市值变化-1日': 'hold_market_1d',
                       '持股市值变化-5日': 'hold_market_5d', '持股市值变化-10日': 'hold_market_10d'}, inplace=True)
    df = df[['trade_date', 'stock_code', 'stock_name', 'change_percent', 'hold_stock_nums', 'hold_market',
             'hold_stock_nums_rate', 'hold_market_1d', 'hold_market_5d', 'hold_market_10d', 'update_time', 'td']]
    # MySQL无法处理nan
    df = df.replace({np.nan: None})
    # 数据中存在None，进行替换才能创基
    # df = df.replace(pd.NA, '')
    spark_df = spark.createDataFrame(df)
    spark_df.repartition(1).write.insertInto('stock.ods_stock_hsgt_stock_di', overwrite=True)  # 如果执行不存在这个表，会报错
    print('{}：执行完毕！！！'.format(appName))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_stock_hsgt_stock_di.py all
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_stock_hsgt_stock_di.py update 20230214 20230214
# nohup python ods_stock_hsgt_stock_di.py update 20220930 >> my.log 2>&1 &
# python ods_stock_hsgt_stock_di.py all
# python ods_stock_hsgt_stock_di.py update 20221101  20221110
if __name__ == '__main__':
    start_date = date.today().strftime('%Y%m%d')
    end_date = start_date

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
    get_data(start_date, end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))
