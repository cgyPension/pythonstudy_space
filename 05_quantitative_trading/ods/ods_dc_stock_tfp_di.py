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

def get_data(start_date, end_date, engine):
    """
    获取指定日期的A股数据写入mysql
    :param start_date: 数据获取开始日期
    :param end_date: 数据获取结束日期
    :param engine: 数据库连接
    :return: None
    """
    # time.sleep(1)
    result_list = []
    daterange = pd.date_range(start_date, end_date)
    for single_date in daterange:
        try:
            # 两市停复牌
            df = ak.stock_tfp_em(date=single_date.strftime("%Y%m%d"))

            if df.empty:
                continue
            if df['代码'].startswith('6'):
                df['代码'] = df['代码'] + '.SH'
            elif df['代码'].startswith('8') or df['代码'].startswith('4') == True:
                df['代码'] = df['代码'] + '.BJ'
            else:
                df['代码'] = df['代码'] + '.SZ'

            df['trade_date'] = single_date.strftime("%Y-%m-%d")
            df.rename(columns={'代码': 'stock_code','名称': 'stock_name','停牌时间': 'suspension_time','停牌截止时间': 'suspension_deadline','停牌期限': 'suspension_period','停牌原因': 'suspension_reason','所属市场': 'belongs_market','预计复牌时间': 'estimated_resumption_time'}, inplace=True)
            df = df[['trade_date', 'stock_code', 'stock_name','suspension_time', 'suspension_deadline','suspension_period','suspension_reason','belongs_market','estimated_resumption_time']]
            result_list.extend(np.array(df).tolist())

        except Exception as e:
            print(e)

    # 写入mysql append replace
    # 重复主键不插入
    engine.execute(
        """
       insert ignore into ods_dc_stock_quotes_di (trade_date, stock_code, stock_name, before_open_price,open_price, close_price, high_price, low_price,
                                   volume, turnover, change_percent, change_amount, turnover_rate,total_market_value,circulating_market_value)
                                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, result_list
    )

    sqlalchemyUtil().closeEngine()


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

    engine = sqlalchemyUtil().engine

    start_time = time.time()
    get_data(start_date, end_date, engine)
    print('程序运行时间：{}s'.format(time.time() - start_time))
