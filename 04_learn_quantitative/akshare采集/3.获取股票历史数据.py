import os
import time
import pandas as pd
import akshare as ak
import warnings
from sqlalchemy import create_engine
from datetime import timedelta, datetime
warnings.filterwarnings("ignore")

pd.set_option('max_rows', None)  # 显示最多行数
pd.set_option('max_columns', None) # 显示最多列数
pd.set_option('expand_frame_repr', False) # 当列太多时显示不清楚
pd.set_option('display.unicode.east_asian_width', True) # 设置输出右对齐


# 输入参数
period="daily" # 周期 'daily', 'weekly', 'monthly'
start_date="20220822" # 数据获取开始日期
end_date="20220822" # 数据获取结束日期
adj="hfq" # 复权类型 qfq": "前复权", "hfq": "后复权", "": "不复权"

# 利用东财实时行情数据接口获取所有股票代码接口
df=ak.stock_zh_a_spot_em()

code_list=df[['序号','代码','名称']].values

# 创建文件存储路径
def create_path():
    global path
    date_str=str(pd.to_datetime(start_date).date()) #日期转换成字符串


    path = os.path.join("..", "all_stock_candle", "stock", date_str)
    # 保存数据
    if not os.path.exists(path) :
        # os.mkdir(path)  # 可以建一级文件夹
        os.makedirs(path)  # 可以建多级文件夹
    file_name = ak_code + ".csv"
    return os.path.join(path,file_name)


# 获取所有历史上股票的历史数据
for i, ak_code,ak_name in code_list:
    print(i,ak_code,ak_name)
    try:
        # 利用东财历史行情数据接口获取股票数据
        df= ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date,end_date=end_date, adjust=adj)

    except Exception as e:
        print(e)
        continue

    if ak_code.startswith('6') == True:
        df['股票代码'] = ak_code + '.SH'
    elif ak_code.startswith('8')or ak_code.startswith('4') == True:
        df['股票代码'] = ak_code + '.BJ'
    else:
        df['股票代码'] = ak_code + '.SZ'

    df['股票名称'] = ak_name
    df.rename(columns={'日期': '交易日期',  '开盘': '开盘价', '最高': '最高价','最低': '最低价', '收盘': '收盘价'}, inplace=True)
    df=df[['交易日期','股票代码','股票名称','开盘价','最高价','最低价','收盘价','成交量','成交额','振幅','涨跌幅','涨跌额','换手率']]
    df.sort_values(by=['交易日期'],ascending=True,inplace=True)
    df.reset_index(drop=True, inplace=True)

    # path=create_path()
    # df.to_csv(path, index=False, mode='w',encoding='gbk')
    # time.sleep(2)
    # 写入mysql 会自动建表
    engine = create_engine('mysql+pymysql://root:123456@hadoop102:3306/stock?charset=utf8')
    df.to_sql('test_stock', engine, chunksize=100000, index=None)