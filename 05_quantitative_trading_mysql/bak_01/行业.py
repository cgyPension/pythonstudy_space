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

if __name__ == '__main__':
    # 同化顺行业板块 所有名称 305个
    # stock_board_industry_name_ths_df = ak.stock_board_industry_name_ths()
    # print(stock_board_industry_name_ths_df)

    # 同花顺-行业板块-成份股
    # stock_board_industry_cons_ths_df = ak.stock_board_industry_cons_ths(symbol="半导体及元件")
    # print(stock_board_industry_cons_ths_df)

    # 同花顺-行业板块-指数日频数据 没啥用
    # stock_board_industry_index_ths_df = ak.stock_board_industry_index_ths(symbol="半导体及元件", start_date="20200101",
    #                                                                       end_date="20211027")
    # print(stock_board_industry_index_ths_df)

    # 东方财富-行业板块 所有名称 含当天上涨家数 领涨股票 86个板块
    # stock_board_industry_name_em_df = ak.stock_board_industry_name_em()
    # print(stock_board_industry_name_em_df)

    # 东方财富-成份股 行业板块
    # stock_board_industry_cons_em_df = ak.stock_board_industry_cons_em(symbol="小金属")
    # print(stock_board_industry_cons_em_df)

    # 同花顺 - 概念时间表 所有概率名称 565个
    # stock_board_concept_name_ths_df = ak.stock_board_concept_name_ths()
    # print(stock_board_concept_name_ths_df)

    # 同花顺-概念板块-成份股
    # stock_board_concept_cons_ths_df = ak.stock_board_concept_cons_ths(symbol="人脸识别")
    # print(stock_board_concept_cons_ths_df)

    # 同花顺 - 成份股 - 行业代码
    # stock_board_cons_ths_df = ak.stock_board_cons_ths(symbol="881121")
    # print(stock_board_cons_ths_df)

    # stock_board_concept_info_ths_df = ak.stock_board_concept_info_ths()
    # print(stock_board_concept_info_ths_df)

    # stock_board_industry_info_ths_df = ak.stock_board_industry_info_ths()
    # print(stock_board_industry_info_ths_df)

    # 东方财富-概念板块 392个
    # stock_board_concept_name_em_df = ak.stock_board_concept_name_em()
    # print(stock_board_concept_name_em_df)

    # 东方财富-沪深板块-概念板块-板块成份
    stock_board_concept_cons_em_df = ak.stock_board_concept_cons_em(symbol="车联网")
    print(stock_board_concept_cons_em_df)

    # 东方财富 - 数据中心 - 年报季报 - 业绩快报
    # stock_yjkb_em_df = ak.stock_yjkb_em(date="20220331")
    # print(stock_yjkb_em_df)

    # 东方财富 - 数据中心 - 年报季报 - 业绩预告
    # stock_yjyg_em_df = ak.stock_yjyg_em(date="20221030")
    # print(stock_yjyg_em_df)

    # 东方财富 - 数据中心 - 年报季报 - 业绩报表
    # stock_yjbb_em_df = ak.stock_yjbb_em(date="20220930")
    # print(stock_yjbb_em_df)