import pandas as pd


import akshare as ak
import warnings
from datetime import date

import akshare as ak
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from util.CommonUtils import str_pre
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
if __name__ == '__main__':
    # stock_lhb_detail_em_df = ak.stock_lhb_detail_em(start_date="20221111", end_date="20221114")
    # print(stock_lhb_detail_em_df)
    # stock_sina_lhb_ggtj_df = ak.stock_sina_lhb_ggtj(recent_day="5")
    # print(stock_sina_lhb_ggtj_df)
    # single_date = '20221112'
    # df = ak.stock_lhb_detail_em(single_date, single_date)
    # print('ods_stock_lhb_detail_em_di：正在处理{}...'.format(single_date))

    # current_dt = date.today()
    start_date = '20221101'
    end_date = '20221115'
    # 新浪财经的股票交易日历数据
    df = ak.tool_trade_date_hist_sina()
    # df = df[df['trade_date'] >= pd.to_datetime(start_date).date()].reset_index(drop=True)
    df = df[(df.trade_date >= pd.to_datetime(start_date).date()) & (df.trade_date<=pd.to_datetime(end_date).date())]
    for single_date in df.trade_date:
        print(single_date.strftime("%Y%m%d"),type(single_date))
    # print(df)
    # current_dt = df.iat[0, 0]  # 下一个交易日
