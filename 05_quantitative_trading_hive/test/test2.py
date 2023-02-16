import datetime
import multiprocessing
import os
import sys
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import warnings
from datetime import date
import akshare as ak
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
#matplotlib中文显示设置
plt.rcParams['font.sans-serif']=['FangSong']   #中文仿宋
plt.rcParams['font.sans-serif']=['SimHei']     #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False       #用来正常显示负号


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/test/test2.py
if __name__ == '__main__':
    # df = pd.DataFrame({"A": ['a', 'b', 'b', 'd'],
    #                    "B": [11, 2, -4, 3],
    #                    "C": [1, 2, None, 6],
    #                    "D": [5.23, 4.66, 2, None]})
    #
    # print(df)
    #
    # daterange = pd.date_range(pd.to_datetime('20210101').date(), pd.to_datetime('20230101').date(), freq='Q-Mar')
    # print(daterange)
    # start_date, end_date = pd.to_datetime(pd.DataFrame(daterange).iloc[0,0]).date(), pd.DataFrame(daterange).iloc[-1,0]
    # print(type(start_date))
    # df = ak.stock_hsgt_stock_statistics_em(symbol="北向持股", start_date='20230213',end_date='20230213')
    # df = ak.stock_hsgt_stock_statistics_em(symbol="北向持股", start_date='20230214',end_date='20230214')
    # print(df.sort_values(by='持股市值', ascending=False))

    # df = ak.stock_fund_stock_holder('600004')
    # df.drop_duplicates(subset=['截止日期', '基金代码'], keep='last', inplace=True)
    # df = df[pd.to_datetime(df['截止日期']) >= pd.to_datetime('2021-03-31')]
    # df = ak.fund_name_em()
    # print(df.head())


    # df = ak.stock_report_fund_hold_detail(symbol='000068', date='20210331')
    df = ak.stock_report_fund_hold_detail(symbol='008286', date='20210331')
    print(df)

