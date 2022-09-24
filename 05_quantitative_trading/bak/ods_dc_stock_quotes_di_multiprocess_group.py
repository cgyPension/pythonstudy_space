import multiprocessing
import os
import sys
import time
from datetime import date, datetime

import numpy as np
import pandas as pd
import warnings
import akshare as ak

warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

from sqlalchemy import insert
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.DBUtils import pymysqlUtil, sqlalchemyUtil, process_num
from util.CommonUtils import get_code_list, get_process_num, multiprocessing_func, get_code_group


def multiprocess_run(code_list,period,start_date, end_date, adjust, engine, process_num):
    code_group=get_code_group(process_num, code_list)
    if start_date is None or end_date is None:
        # 今天 要下午3点后运行
        start_date = date.today().strftime('%Y%m%d')
        end_date = start_date
    else:
        start_date = start_date
        end_date = end_date

    with multiprocessing.Pool(processes=process_num) as pool:
        # 多进程异步计算
        for codes in code_group:
            # print(codes)
            # 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
            pool.apply_async(get_group_data, args=(codes, period,start_date, end_date, adjust,engine,))

        # 阻止后续任务提交到进程池
        pool.close()
        # 等待所有进程结束
        pool.join()

    sqlalchemyUtil().closeEngine()



def get_group_data(code_list,period,start_date, end_date, adjust, engine):
    result_list = []
    for ak_code in code_list:
        # print(ak_code)
        print('{}启动,父进程为{}：{}/{})正在处理{}...'.format(os.getpid(), os.getppid(), (code_list.index(ak_code)+1,len(code_list), ak_code)))
        # df = get_data(ak_code,period, start_date, end_date, adjust)
        # time.sleep(1)

        for i in range(1):
            # 东财接口  没有市值 要从网易接口join 补充
            # print(ak_code)
            try:
                # 东财接口  没有市值 要从网易接口join 补充
                df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date,
                                        adjust=adjust)
            except Exception as e:
                print(e)
                return pd.DataFrame
            if df.empty:
                # return df
                continue
            if ak_code.startswith('6'):
                df['stock_code'] = ak_code + '.SH'
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                df['stock_code'] = ak_code + '.BJ'
            else:
                df['stock_code'] = ak_code + '.SZ'

            df.rename(columns={'日期': 'trade_date', '开盘': 'open_price', '收盘': 'close_price', '最高': 'high_price',
                               '最低': 'low_price', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
                               '涨跌幅': 'change_percent',
                               '涨跌额': 'change_amount', '换手率': 'turnover_rate'}, inplace=True)
            df = df[['trade_date', 'stock_code', 'open_price', 'close_price', 'high_price', 'low_price',
                     'volume',
                     'turnover', 'amplitude', 'change_percent', 'change_amount', 'turnover_rate']]

        if df.empty:
            continue
        result_list.extend(np.array(df).tolist())
    print(result_list)

    # print('get_group_data', result_list)
    # 写入mysql append replace
    # 重复主键不插入
    engine.execute(
        """
       insert ignore into ods_dc_stock_quotes_di (trade_date, stock_code, open_price, close_price, high_price, low_price,
                                   volume, turnover, amplitude, change_percent, change_amount, turnover_rate)
                                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, result_list
    )
    # print(result_list)
    # sqlalchemyUtil().closeEngine()
    # print('写入完成')



def get_data(ak_code,period,start_date, end_date, adjust):
    """
    获取指定日期的A股数据写入mysql

    :param period: 周期 'daily', 'weekly', 'monthly'
    :param start_date: 数据获取开始日期
    :param end_date: 数据获取结束日期
    :param adjust: 复权类型 qfq": "前复权", "hfq": "后复权", "": "不复权"
    :param engine: myslq url
    :return: None
    """
    # time.sleep(1)

    for i in range(1):
        # 东财接口  没有市值 要从网易接口join 补充
        # print(ak_code)
        try:
            # 东财接口  没有市值 要从网易接口join 补充
            df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date,adjust=adjust)
        except Exception as e:
            print(e)
            return pd.DataFrame
        if df.empty:
            return df
        if ak_code.startswith('6'):
            df['stock_code'] = ak_code + '.SH'
        elif ak_code.startswith('8') or ak_code.startswith('4') == True:
            df['stock_code'] = ak_code + '.BJ'
        else:
            df['stock_code'] = ak_code + '.SZ'

        df.rename(columns={'日期': 'trade_date', '开盘': 'open_price', '收盘': 'close_price', '最高': 'high_price',
                           '最低': 'low_price', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
                           '涨跌幅': 'change_percent',
                           '涨跌额': 'change_amount', '换手率': 'turnover_rate'}, inplace=True)
        df = df[['trade_date', 'stock_code', 'open_price', 'close_price', 'high_price', 'low_price',
                 'volume',
                 'turnover', 'amplitude', 'change_percent', 'change_amount', 'turnover_rate']]
        return df



code_list = get_code_list()
# code_list = np.array(pd.DataFrame([['603182','N嘉华'],['300374','中铁装配'],['301033','迈普医学']], columns=('代码', '名称')))
period = 'daily'
start_date = '20210101'
end_date = '20210101'
adjust = "hfq"
engine = sqlalchemyUtil().engine
# target_table=os.path.splitext(os.path.basename(__file__))[0]
# target_table = 'ods_dc_stock_quotes_di'
timeout = 10  # 函数运行限时超时时间
# process_num = get_process_num()
process_num = 6
if __name__ == '__main__':
    start_time = time.time()
    # common_run(code_list, period, start_date, end_date, adjust, engine)
    multiprocess_run(code_list, period, start_date, end_date, adjust, engine, process_num)
    print('程序运行时间：{}s'.format(time.time() - start_time))
