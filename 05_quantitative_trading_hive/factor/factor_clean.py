import os
import sys
import time
import warnings
from datetime import date,datetime
import akshare as ak
import numpy as np
import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

#
def data_clean(factor, new_stock_filter=new_stock_filter, st_filter=st_filter, suspended_filter=suspended_filter,limit_up_down_filter=limit_up_down_filter, index_fix=index_fix):
    # 剔除ST、涨停、停牌、新股
    factor = factor.mask(new_stock_filter).mask(st_filter).mask(suspended_filter).mask(limit_up_down_filter).mask(
        ~index_fix).dropna(how='all')
    print('券池过滤完毕')
    # 离群值处理
    factor = factor.apply(lambda x: filter_extreme_MAD(x, 3), axis=1)
    # 标准化
    factor = factor.sub(factor.mean(axis=1), axis=0).div(factor.std(axis=1), axis=0)  # add
    print('因子数据清洗完成，已剔除离群值、中性化处理')
    # 行业市值中性化
    factor = neutralization(factor)
    print('因子完成行业市值中性化')

    return factor

# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_dc_stock_tfp_di.py all
# nohup python ods_dc_stock_tfp_di.py update 20221010 20221010 >> my.log 2>&1 &
# python ods_dc_stock_tfp_di.py all
if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        run_type = sys.argv[1]
        if run_type == 'all':
            start_date = '20210101'
            end_date = date.today().strftime('%Y%m%d')
        else:
            start_date = date.today().strftime('%Y%m%d')
            end_date = start_date
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]

    start_time = time.time()

    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))