### 导包
from datetime import datetime

import akshare as ak
import pandas as pd
import numpy as np
import os

from util.CommonUtils import get_code_list
from util.DBUtils import sqlalchemyUtil

code_list = get_code_list()
period = 'daily'
start_date = '20220905'
end_date = '20220909'
adjust = "hfq"
engine = sqlalchemyUtil().engine
# target_table=os.path.splitext(os.path.basename(__file__))[0]
target_table = 'ods_dc_stock_quotes_di'
timeout = 10  # 函数运行限时超时时间
ak_code = '600519'#贵州茅台
def main():
    result_list = []
    try:
        df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date,
                                adjust=adjust)
        # if df.empty:
        #     continue
        if ak_code.startswith('6'):
            df['stock_code'] = ak_code + '.SH'
        elif ak_code.startswith('8') or ak_code.startswith('4') == True:
            df['stock_code'] = ak_code + '.BJ'
        else:
            df['stock_code'] = ak_code + '.SZ'

        df['stock_name'] = '测试'
        df.rename(columns={'日期': 'trade_date', '开盘': 'open_price', '收盘': 'close_price', '最高': 'high_price',
                           '最低': 'low_price', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
                           '涨跌幅': 'change_percent',
                           '涨跌额': 'change_amount', '换手率': 'turnover_rate'}, inplace=True)
        df = df[['trade_date', 'stock_code', 'stock_name', 'open_price', 'close_price', 'high_price', 'low_price',
                 'volume',
                 'turnover', 'amplitude', 'change_percent', 'change_amount', 'turnover_rate']]
        print(df)
        result_list.extend(np.array(df).tolist())
        print(result_list)
        engine.execute(
            """
           insert ignore into ods_dc_stock_quotes_di (trade_date, stock_code, stock_name, open_price, close_price, high_price, low_price,
                                       volume, turnover, amplitude, change_percent, change_amount, turnover_rate)
                                       values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, result_list
        )
        # sqlalchemyUtil().closeEngine()
    except:
        KeyError()


if __name__ == '__main__':
    start_time = datetime.now()
    main()
    print('程序运行时间：{}s'.format(datetime.now() - start_time))
