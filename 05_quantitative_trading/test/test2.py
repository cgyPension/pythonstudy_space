import pandas as pd

from util.CommonUtils import get_code_group, get_code_list
import akshare as ak
# code_group=get_code_group(10, get_code_list())
# print(code_group)
# print(type(code_group))
#
# code_group=get_code_group(10, get_code_list())
# print(code_group)
# print(type(get_code_list()))
# print(ak.tool_trade_date_hist_sina())
import os
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert

from util.DBUtils import sqlalchemyUtil
# code_list = get_code_list()
# print(code_list)
# print(type(code_list))
#
# df = pd.DataFrame([['603182','N嘉华'],['300374','中铁装配'],['301033','迈普医学']], columns=('代码', '名称'))
#
# print(df)
# print(type(df))
#
# print(np.array(df).tolist())
# print(type(np.array(df)))
df = ak.stock_tfp_em(date='20220523')
ak_code = df['代码']
print(ak_code)

# stock_tfp_em_df = ak.stock_tfp_em(date="20220523")
# print(stock_tfp_em_df)