import datetime
import multiprocessing
import os
import sys
import time
import random
import statsmodels.api as sm
from util.algorithmUtils import rps

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
from scipy import stats
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


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/test/test2.py
if __name__ == '__main__':
    df = pd.DataFrame({"A": ['a', 'b', 'b', 'd'],
                       "B": [11, 2, -4, 3],
                       "C": [1, 2, None, 6],
                       "D": [5.23, 4.66, 2, None]})

    print(df)
    # # print(df.dropna(subset=["C","D"]))
    # print(df[['B','C']].corr())
    # print(df[['B','C']].dropna().corr())
    # print(round(6.1234567891123456789,10))
    # result_df = pd.DataFrame(columns=['A'])
    # a = pd.merge(result_df, df, how='outer', on=['A'])
    # print('a',a)
    # print(str(pd.to_datetime('20220101').date()))

