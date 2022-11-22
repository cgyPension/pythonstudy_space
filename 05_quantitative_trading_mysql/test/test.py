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

from util.DBUtils import sqlalchemyUtil

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


import os
import smtplib
import sys
import time
from datetime import date
from email.mime.text import MIMEText
from email.utils import formataddr
import akshare as ak
import pandas as pd
if __name__ == '__main__':
    s_date = '20210101'
    start_date = '20221115'
    # df = ak.stock_financial_analysis_indicator(symbol='601398')
    df = ak.stock_financial_analysis_indicator(symbol='871396')

    df['日期'] = df['日期'].astype('date')
    print(type(df['日期']))

    # df = df[pd.to_datetime(df['日期']) >= pd.to_datetime(start_date)]
    # # df = df[df['日期'].astype('date') >= pd.to_datetime(start_date).date()]
    # print(df)

    # td_df = ak.tool_trade_date_hist_sina()
    # daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date<pd.to_datetime(start_date).date())]
    # print(daterange_df)
    # print(daterange_df.iloc[-5,0],type(daterange_df.iloc[-5,0]))
    # print(daterange_df)
    # current_dt = df.iat[0, 0]
