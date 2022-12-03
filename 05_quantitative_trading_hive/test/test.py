import datetime
import multiprocessing
import os
import sys
import time
import warnings
from datetime import date
import talib as ta
import akshare as ak
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

from util.CommonUtils import get_spark

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



if __name__ == '__main__':
    # 描述: 东方财富股票指数数据, 历史数据按日频率更新
    # 沪深300 000300
    # stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sh000300")
    # print(stock_zh_index_daily_df)
    # df = ak.stock_a_lg_indicator(symbol='000609')
    # print(df)

    # df = ak.stock_tfp_em(date='20221130')
    # print(df)
    # t1 = pd.Timestamp('2019-01-10')
    # print(type(t1),t1)

    # print(pd.to_datetime('20221101').date().strftime('%Y%m%d')+'哈哈')
    # tock_zh_valuation_baidu_df = ak.stock_zh_valuation_baidu(symbol="600584", indicator="市盈率(TTM)")
    # print(tock_zh_valuation_baidu_df)
    # ods 单独没有这个29号数据
    # df = ak.stock_a_lg_indicator(symbol="600584")
    df = ak.stock_a_lg_indicator(symbol="000760")
    print(df)
