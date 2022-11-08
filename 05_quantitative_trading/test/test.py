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

    stock_a_indicator_df = ak.stock_a_lg_indicator(symbol="601398")
    # print(stock_a_indicator_df['trade_date']>pd.datetime.date(2022,11,3))
    # print(stock_a_indicator_df[stock_a_indicator_df['trade_date']>datetime.datetime.strptime('20221103', '%Y%m%d').date()])
    print(stock_a_indicator_df[stock_a_indicator_df['trade_date']>pd.to_datetime("20221103").date()])

    # plt.hist(stock_a_indicator_df.pe)  # 市盈率-正方图
    # plt.hist(stock_a_indicator_df.pe_ttm)  # 市盈率-直方图
    #
    # plt.show()

