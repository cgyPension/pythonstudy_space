import datetime
import multiprocessing
import os
import sys
import time
import random
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from bak.情绪数据源 import *
from util import btUtils
import warnings
from datetime import date
import talib as ta
import akshare as ak
import numpy as np
import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt
import plotly.express as px
from decimal import Decimal
from util.CommonUtils import get_spark
from tqdm import tqdm
from lxml import etree
import requests
import json
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

def get_data():
    start_date = '20230103'
    end_date = '20230103'
    # start_date = '20221203'
    # end_date = '20221203'
    daterange = pd.date_range(pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date(), freq='Q-Mar')
    print(daterange)
    pd_df = pd.DataFrame()
    if daterange.empty:
        # 增量 覆盖
        end_date_year_start = pd.to_datetime('20210101').date()
        last_date = pd.date_range(end_date_year_start, pd.to_datetime(end_date).date(), freq='Q-Mar')
        print(last_date)
        if last_date.empty:
            print('kkkkkkk')
            return
        df = pd.DataFrame()
        df['time'] = last_date
        # 上一期的日期
        single_date = df.iat[df.idxmax()[0], 0]
        print(single_date)
    else:
        print('else')
    print('main')

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/test/test.py
if __name__ == '__main__':
    # 描述: 东方财富股票指数数据, 历史数据按日频率更新
    # 沪深300 000300
    # stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sh000300")
    # print(stock_zh_index_daily_df)
    # df = ak.stock_a_lg_indicator(symbol='000609')
    # df = df[df['trade_date'] >= pd.to_datetime('20221226').date()]
    # print(df)
    # print(df.dtypes)
    #
    # a = Decimal(1.25)
    # b = 4.33
    # print(type(a))
    # print(type(Decimal(b)))
    #
    # df['total_mv'] = df['total_mv'].apply(lambda x: Decimal(x))
    # df['total_mv'] = df['total_mv'].astype(float)
    # # print(df.dtypes)
    # print(df.display())
    #
    # df2 = pd.DataFrame({"A": [Decimal(5.2), Decimal(5.2), Decimal(5.2), Decimal(5.2)],
    #                    "D": [5, 4, 2, 8]})
    # print(df2.dtypes)

    # df = ak.stock_tfp_em(date='20221130')
    # print(df)
    # t1 = pd.Timestamp('2019-01-10')
    # print(type(t1),t1)

    # print(pd.to_datetime('20221101').date().strftime('%Y%m%d')+'哈哈')
    # tock_zh_valuation_baidu_df = ak.stock_zh_valuation_baidu(symbol="600584", indicator="市盈率(TTM)")
    # print(tock_zh_valuation_baidu_df)
    # ods 单独没有这个29号数据
    # 字段是 date open close high low volume


    # df = pd.DataFrame({"A": [5, 3, 3, 4],
    #                    "B": [11, 2, 4, 3],
    #                    "C": [4, 3, 8, 5],
    #                    "D": [5, 4, 2, 8]})
    # print(type(df.values.tolist()))
    # df2 = pd.DataFrame({"A": [pd.to_datetime('20221101').date(), pd.to_datetime('20221201').date(), pd.to_datetime('20220104').date(), pd.to_datetime('20221020').date()]})
    # df['r'] = df.A.shift(1)
    # df = ak.stock_a_lg_indicator(symbol='000609')
    # print(df)

    # stock_board_concept_hist_em(symbol=concept_plate, start_date=start_date, end_date=end_date, period='daily',adjust='')
    # stock_board_concept_hist_em_df1 = ak.stock_board_concept_hist_em(symbol="熊去氧胆酸", start_date="20221216",end_date="20221216", period="daily", adjust="")
    # stock_board_concept_hist_em_df2 = ak.stock_board_concept_hist_em(symbol="熊去氧胆酸", start_date="20221216",end_date="20221216", period='daily', adjust="qfq")
    # stock_board_concept_hist_em_df3 = ak.stock_board_concept_hist_em(symbol="熊去氧胆酸", start_date="20221216",end_date="20221216", period='daily',adjust="hfq")
    # print('111',stock_board_concept_hist_em_df1)
    # print('222',stock_board_concept_hist_em_df2)
    # print('333',stock_board_concept_hist_em_df3)

    # lst99 = [['001', [1,11]],['002', [2,22]],['003', [3,33]]]
    # for i,(num,(a,b)), in enumerate(lst99):
    #     print(i+1, num,a,b)
    #
    # print(int(5/2))
    # pa_group = [['001', 'df'],['002', 'df'],['003', 'df']]
    # for i in range(len(pa_group)):
    #     print(len(pa_group))
    #     print(i)
    #     num,kk = pa_group[i][0],pa_group[i][1]
    #     print(num,kk)

    # stock_hot_rank_em_df = ak.stock_hot_rank_em()
    # print(stock_hot_rank_em_df)

    # stock_hot_follow_xq_df = ak.stock_hot_follow_xq(symbol="最热门")
    # print(stock_hot_follow_xq_df)

    # get_data()
    # df = ak.stock_lrb_em(date='20221231')
    # df = ak.stock_board_concept_name_em()
    # print(df)
    import numpy as np
    import statsmodels.api as sm
    import matplotlib.pyplot as plt

    # 构造函数
    number = 50
    group = np.zeros(number, int)
    print(group)
    group[20:40] = 1
    group[40:] = 2
    print(group)
    category = sm.categorical(group, drop=True)  # 构造分类变量
    print(category)
    x = np.linspace(0, 10, number)
    X = np.column_stack((x, category))
    X = sm.add_constant(X)
    bata = np.array([2, 3, 4, 5, 6])
    e = np.random.normal(size=number)
    y = np.dot(X, bata) + e

    # 建立方程
    model = sm.OLS(y, X).fit()
    # print(model.summary())
        #
        # # 东方财富-数据中心-年报季报-业绩快报-利润表
        # df = ak.stock_lrb_em(date=single_date.strftime("%Y%m%d"))
        # print(df)

