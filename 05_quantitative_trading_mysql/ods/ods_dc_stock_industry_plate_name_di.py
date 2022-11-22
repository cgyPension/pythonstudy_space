import multiprocessing
import os
import sys
import time
import warnings
from datetime import date

import akshare as ak
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.DBUtils import sqlalchemyUtil
from util.CommonUtils import get_process_num, get_code_group, get_code_list

def get_data(engine):

    try:
        # 只能获取当天的数据
        df = ak.stock_board_industry_name_em()

        start_date = date.today().strftime('%Y-%m-%d')
        df['trade_date'] = start_date

        df.rename(columns={'板块名称': 'industry_plate','板块代码': 'industry_plate_code','最新价': 'new_price','涨跌额': 'change_amount','涨跌幅': 'change_percent','总市值': 'total_market_value','换手率': 'turnover_rate','上涨家数': 'rise_num','下跌家数': 'fall_num','领涨股票': 'leading_stock_name','领涨股票-涨跌幅': 'leading_stock_change_percent'}, inplace=True)
        df = df[['trade_date','industry_plate_code','industry_plate','new_price','change_amount','change_percent','total_market_value','turnover_rate','rise_num','fall_num','leading_stock_name','leading_stock_change_percent']]
        # MySQL无法处理nan
        df = df.replace({np.nan: None})
        result_list = np.array(df).tolist()

        start_date = date.today().strftime('%Y%m%d')
        engine.execute(
            """delete from ods_dc_stock_industry_plate_name_di where trade_date = '%s';"""% (start_date)
        )
        # 写入mysql append replace
        # 重复主键不插入
        engine.execute(
            """
            insert ignore into ods_dc_stock_industry_plate_name_di (trade_date,industry_plate_code,industry_plate,new_price,change_amount,change_percent,total_market_value,turnover_rate,rise_num,fall_num,leading_stock_name,leading_stock_change_percent)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s);
            """, result_list
        )
    except Exception as e:
        print(e)
    print('ods_dc_stock_industry_plate_name_di：执行完毕！！！')


# nohup python ods_dc_stock_industry_plate_name_di.py >> my.log 2>&1 &
# python ods_dc_stock_industry_plate_name_di.py
if __name__ == '__main__':
    engine = sqlalchemyUtil().engine

    start_time = time.time()
    get_data(engine)
    engine.dispose()
    end_time = time.time()
    print('程序运行时间：{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))