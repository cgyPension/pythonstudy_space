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

warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

from util.CommonUtils import str_pre, get_spark

def get_data(start_date, end_date):
   appName = os.path.basename(__file__)
   # 本地模式
   spark = get_spark(appName)

   # 还没到下一期时候要重跑上一期  数据会有公告日期超过 接口的枚举日期
   # param date: choice of {"20200331", "20200630", "20200930", "20201231", "..."}; 从 20100331 开始
   daterange = pd.date_range(pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date(), freq='Q-Mar')
   pd_df = pd.DataFrame()
   if daterange.empty:
       # 增量 覆盖
       # end_date_year_start = date(pd.to_datetime(start_date).year, 1, 1) # 获取日期年的一月一日
       end_date_year_start = pd.to_datetime('20210101').date()
       last_date = pd.date_range(end_date_year_start, pd.to_datetime(end_date).date(), freq='Q-Mar')
       df = pd.DataFrame()
       df['time'] = last_date
       # 上一期的日期
       single_date = df.iat[df.idxmax()[0], 0]
       try:
           # 东方财富-数据中心-年报季报-业绩快报-利润表
           df = ak.stock_lrb_em(date=single_date.strftime("%Y%m%d"))
           # print('ods_stock_lrb_em_di：正在处理{}...'.format(start_date))
           # 去重、保留最后一次出现的
           df.drop_duplicates(subset=['股票代码'], keep='last', inplace=True)
           df['stock_code'] = df['股票代码'].apply(str_pre)
           df['announcement_date'] = pd.to_datetime(df['公告日期'])
           df['td'] = df['announcement_date']
           df['update_time'] = datetime.now()

           df.rename(columns={'股票简称': 'stock_name', '净利润': 'net_profit',
                              '净利润同比': 'net_profit_yr', '营业总收入': 'total_business_income',
                              '营业总收入同比': 'total_business_income_yr', '营业总支出-营业支出': 'business_fee',
                              '营业总支出-销售费用': 'sales_fee', '营业总支出-管理费用': 'management_fee', '营业总支出-财务费用': 'finance_fee',
                              '营业总支出-营业总支出': 'total_business_fee', '营业利润': 'business_profit', '利润总额': 'total_profit'},
                     inplace=True)
           df = df[['announcement_date', 'stock_code', 'stock_name', 'net_profit', 'net_profit_yr', 'total_business_income',
                'total_business_income_yr', 'business_fee', 'sales_fee', 'management_fee', 'finance_fee',
                'total_business_fee', 'business_profit', 'total_profit','update_time','td']]
           # MySQL无法处理nan
           df = df.replace({np.nan: None})
           pd_df = pd_df.append(df)

       except Exception as e:
           print(e)
   else:
        for single_date in daterange:
             try:
                 # 东方财富-数据中心-年报季报-业绩快报-利润表
                 df = ak.stock_lrb_em(date=single_date.strftime("%Y%m%d"))
                 # print('ods_stock_lrb_em_di：正在处理{}...'.format(start_date))
                 # 去重、保留最后一次出现的
                 df.drop_duplicates(subset=['股票代码'], keep='last', inplace=True)
                 df['stock_code'] = df['股票代码'].apply(str_pre)
                 df['announcement_date'] = pd.to_datetime(df['公告日期'])
                 df['td'] = df['announcement_date']
                 df['update_time'] = datetime.now()

                 df.rename(columns={'股票简称':'stock_name','净利润':'net_profit','净利润同比':'net_profit_yr','营业总收入':'total_business_income','营业总收入同比':'total_business_income_yr','营业总支出-营业支出':'business_fee','营业总支出-销售费用':'sales_fee','营业总支出-管理费用':'management_fee','营业总支出-财务费用':'finance_fee','营业总支出-营业总支出':'total_business_fee','营业利润':'business_profit','利润总额':'total_profit'}, inplace=True)
                 df = df[['announcement_date','stock_code','stock_name','net_profit','net_profit_yr','total_business_income','total_business_income_yr','business_fee','sales_fee','management_fee','finance_fee','total_business_fee','business_profit','total_profit','update_time','td']]
                 # MySQL无法处理nan
                 df = df.replace({np.nan: None})
                 pd_df = pd_df.append(df)
             except Exception as e:
                 print(e)

   spark_df = spark.createDataFrame(pd_df)
   # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
   spark_df.repartition(1).write.insertInto('stock.ods_stock_lrb_em_di', overwrite=True)  # 这个份数据有点难定
   print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/05_quantitative_trading_hive/ods/ods_stock_lrb_em_di.py all
# nohup python ods_stock_lrb_em_di.py update 20220930 >> my.log 2>&1 &
# python ods_stock_lrb_em_di.py all
# python ods_stock_lrb_em_di.py update 20220930
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
