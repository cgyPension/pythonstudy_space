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
from util.CommonUtils import get_process_num, get_code_group_v2, get_code_list_v2


def multiprocess_run(code_list, start_date, end_date, engine, process_num):
    code_group = get_code_group_v2(process_num, code_list)
    result_list = []

    with multiprocessing.Pool(processes=process_num) as pool:
        # 多进程异步计算
        for i in range(len(code_group)):
            codes = code_group[i]
            # 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
            result_list.append(pool.apply_async(get_group_data, args=(codes, start_date, end_date, i, len(code_group), len(code_list),)))
        # 阻止后续任务提交到进程池
        pool.close()
        # 等待所有进程结束
        pool.join()

    for r in result_list:
        rl = r.get()
        if rl:
            # 写入mysql append replace
            # 重复主键不插入
            engine.execute(
                """
               insert ignore into ods_dc_stock_quotes_di (trade_date, stock_code, stock_name, before_open_price,open_price, close_price, high_price, low_price,
                                           volume, turnover, change_percent, change_amount, turnover_rate,total_market_value,circulating_market_value)
                                           values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, rl
            )
        else:
            print('rl为空')
    sqlalchemyUtil().closeEngine()


def get_group_data(code_list, start_date, end_date, i, n, total):
    result_list = []
    for codes in code_list:
        ak_code = codes[0]
        print('ods_163_stock_quotes_di：{}启动,父进程为{}：第{}组/共{}组，{}个)正在处理{}...'.format(os.getpid(), os.getppid(), i, n, total, ak_code))

        df = get_data(ak_code, start_date, end_date)
        if df.empty:
            continue
        result_list.extend(np.array(df).tolist())
    return result_list


def get_data(ak_code, start_date, end_date):
    """
    获取指定日期的A股数据写入mysql

    :param start_date: 数据获取开始日期
    :param end_date: 数据获取结束日期
    :return: None
    """
    # time.sleep(1)
    for i in range(1):
        try:
            # 补充东财 市值字段
            df = ak.stock_zh_a_hist_163(symbol=ak_code, start_date=start_date, end_date=end_date)

            if df.empty:
                continue
            if ak_code.startswith('6'):
                df['stock_code'] = ak_code + '.SH'
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                df['stock_code'] = ak_code + '.BJ'
            else:
                df['stock_code'] = ak_code + '.SZ'

            df.rename(columns={'日期': 'trade_date', '股票代码': 'stock_code','名称': 'stock_name','开盘价': 'open_price', '收盘价': 'close_price', '最高价': 'high_price',
                               '最低价': 'low_price', '前收盘': 'before_open_price', '成交量': 'volume', '成交金额': 'turnover', '涨跌幅': 'change_percent',
                               '涨跌额': 'change_amount', '换手率': 'turnover_rate', '总市值': 'total_market_value', '流通市值': 'circulating_market_value'}, inplace=True)
            df = df[['trade_date', 'stock_code', 'stock_name', 'before_open_price','open_price', 'close_price', 'high_price', 'low_price',
                     'volume',
                     'turnover', 'change_percent', 'change_amount', 'turnover_rate','total_market_value','circulating_market_value']]
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame


if __name__ == '__main__':
    code_list = get_code_list_v2()

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
    process_num = get_process_num()

    start_time = time.time()
    multiprocess_run(code_list, start_date, end_date, engine, process_num)
    print('程序运行时间：{}s'.format(time.time() - start_time))
