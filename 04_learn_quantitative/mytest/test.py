# 使用AKSHARE + mysql 实现动态抓取个股的交易历史数据
# 同理外面再包一层循环就可以把所有的交易历史数据下载每个股票一个表。
# 后续下载历史数据并且定制下每天更新脚本这样历史交易数据就解决了。
#
# 后续就是弄个回测框架
#
# 添加宏观因素 再添加个股微观因素   再历史回测因素相关性
import time

import pandas as pd
# import mysql.connector
from sqlalchemy import create_engine
import akshare as ak
# 输出显示设置
pd.set_option('max_rows',None)
pd.set_option('max_columns',None)
pd.set_option('expand_frame_repr',False)
pd.set_option('display.unicode.ambiguous_as_wide',True)
pd.set_option('display.unicode.east_asian_width',True)

# 参数设置
period="daily" # 周期 'daily', 'weekly', 'monthly'
start_date="20170301" # 数据获取开始日期
end_date='20210907' # 数据获取结束日期
adjust="hfq" # 复权类型 qfq": "前复权", "hfq": "后复权", "": "不复权"

# 利用东财实时行情数据接口获取所有股票代码接口
df = ak.stock_zh_a_spot_em()
code_list = df[['代码','名称']].values

# 获取所有历史上股票的历史数据
for i in range(len(code_list)):
    ak_code = code_list[i][0]
    ak_name = code_list[i][1]
    # print(f"已下载到{i+1}支股票，股票代码为：{ak_code}，股票名称为：{ak_name}")

    try:
        # 东财接口 没有市值 要从网易接口join 补充
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date, adjust=adjust)
    except Exception as e:
        print(e)

    try:
        # 网易接口 获取市值
        stock_zh_a_hist_163_df = ak.stock_zh_a_hist_163(symbol=ak_code, start_date=start_date, end_date=end_date)
    except Exception as e:
        print(e)

    stock_zh_a_hist_df['股票代码'] = ak_code
    stock_zh_a_hist_df['股票名称'] = ak_name
    # # 在股票代码前加上交易所简称
    stock_zh_a_hist_df['股票代码'] = stock_zh_a_hist_df['股票代码'].astype(str)
    stock_zh_a_hist_df.loc[stock_zh_a_hist_df['股票代码'].str.startswith('6'),'股票代码'] = 'sh' + stock_zh_a_hist_df['股票代码']
    stock_zh_a_hist_df.loc[stock_zh_a_hist_df['股票代码'].str.startswith('4') | stock_zh_a_hist_df['股票代码'].str.startswith('8'),'股票代码'] = 'bj' + stock_zh_a_hist_df['股票代码']
    stock_zh_a_hist_df.loc[stock_zh_a_hist_df['股票代码'].str.startswith('3') | stock_zh_a_hist_df['股票代码'].str.startswith('0'),'股票代码'] = 'sz' + stock_zh_a_hist_df['股票代码']

    # 同一个交易日期 加上网易的市值
    # stock_zh_a_hist_df.loc[stock_zh_a_hist_df['日期']==stock_zh_a_hist_163_df['日期'], '总市值'] = stock_zh_a_hist_163_df['总市值']
    print(stock_zh_a_hist_df)

    # print(stock_zh_a_hist_163_df)
    # stock_zh_a_hist_df['总市值'] = stock_zh_a_hist_df.loc[stock_zh_a_hist_df['日期']==stock_zh_a_hist_163_df['日期'], '总市值']
    # stock_zh_a_hist_df['流通市值'] = stock_zh_a_hist_df.loc[stock_zh_a_hist_df['日期']==stock_zh_a_hist_163_df['日期'], '流通市值']
    #
    # stock_zh_a_hist_df.rename(columns={'日期':'交易日期','开盘':'开盘价','收盘':'收盘价','最高':'最高价','最低':'最低价'})
    # stock_zh_a_hist_df = stock_zh_a_hist_df[['交易日期','股票代码','股票名称','开盘价','收盘价','最高价','最低价','成交量','成交额','振幅','涨跌幅','涨跌额','换手率','总市值','流通市值']]
    #
    # # 排序、去重
    # stock_zh_a_hist_df.sort_values(by=['交易日期'],ascending=True,inplace=True)
    # stock_zh_a_hist_df.drop_duplicates(subset=['股票代码','交易日期'],keep='first',inplace=True)
    # stock_zh_a_hist_df.reset_index(drop=True,inplace=True)

    # 写入mysql

    time.sleep(2)

# data = stock_zh_a_hist_df
#
# engine = create_engine("mysql+mysqlconnector://root:123456@hadoop102:3306/stock?charset=utf8")
# data.to_sql("test_table", engine, index=False)
