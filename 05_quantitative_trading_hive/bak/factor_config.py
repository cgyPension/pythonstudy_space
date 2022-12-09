import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import numpy as np
import pandas as pd

from program.config import *
from program.utils.Function import *
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# 因子:排序
# True从小到大
factor_info = {'BP': False, 'ROE': False,

               }
factor_list = list(factor_info.keys())

option_list = ['市值中性化', '行业中性化', '去极值', '科创创业板']

stock_data_path = root_path + '/data/数据整理/'
all_data_list = get_file_in_folder(stock_data_path, file_type='.pkl')

date_start = '2007-01-01'

recall_cycle = 55

factor_score_list = [
    'T_ABS均值',
    'T_ABS>2(%)',
    'IC_均值',
    'IC_IR',
    'IC>0.00(%)',
    'IC>0.02(%)',
    'IC>0.05(%)',
]

global start_time
start_time = datetime.now()


def reset_time():
    global start_time
    start_time = datetime.now()


def diff_time():
    global start_time
    return (datetime.now() - start_time)


def print_diff_time(str=" "):
    base_str = "@diff_time_" + str + ":"
    print(base_str, diff_time())
