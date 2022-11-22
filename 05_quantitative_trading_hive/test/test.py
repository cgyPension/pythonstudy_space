import datetime
import multiprocessing
import os
import sys
import time
import warnings
from datetime import date

import akshare as ak
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt



warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
#matplotlib中文显示设置
plt.rcParams['font.sans-serif']=['FangSong']   #中文仿宋
plt.rcParams['font.sans-serif']=['SimHei']     #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False       #用来正常显示负号



if __name__ == '__main__':
    # 描述: 东方财富股票指数数据, 历史数据按日频率更新
    # 沪深300 000300
    # stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sh000300")
    # print(stock_zh_index_daily_df)
    # s_date = '20210101'
    # start_date = '20210101'
    # end_date = '20221122'
    # td_df = ak.tool_trade_date_hist_sina()
    # if start_date < '20210111':
    #     start_date = pd.to_datetime(start_date).date()
    # else:
    #     daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
    #     # print(daterange_df)
    #     # 增量 前5个交易日 但是要预够假期
    #     start_date = daterange_df.iloc[-5, 0]
    # end_date = pd.to_datetime(end_date).date()
    # print(start_date)
    # print(end_date)
    s_date = '20210101'
    start_date = '20210101'
    # start_date = '20210120'
    end_date = '20221122'
    td_df = ak.tool_trade_date_hist_sina()

    daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
    # print(daterange_df)
        # 增量 前5个交易日 但是要预够假期
    daterange_df = daterange_df.iloc[-5:, 0].reset_index(drop=True)
    print(daterange_df,type(daterange_df))
    start_date if daterange_df.empty else start_date = pd.to_datetime(daterange_df[0]).strftime('%Y-%m-%d')

    print(start_date)

    # end_date = pd.to_datetime(end_date).date()

    # print(end_date)
