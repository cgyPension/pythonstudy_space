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


def multiprocess_run(code_list, period, start_date, end_date, adjust, engine, process_num):
    code_group = get_code_group(process_num, code_list)
    result_list = []

    with multiprocessing.Pool(processes=process_num) as pool:
        # 多进程异步计算
        for i in range(len(code_group)):
            codes = code_group[i]
            # 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
            result_list.append(pool.apply_async(get_group_data, args=(codes, period, start_date, end_date, adjust, i, len(code_group), len(code_list),)))
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
               insert ignore into ods_dc_stock_quotes_di (trade_date, stock_code, stock_name, open_price, close_price, high_price, low_price,
                                           volume, turnover, amplitude, change_percent, change_amount, turnover_rate)
                                           values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, rl
            )
        else:
            print('rl为空')
    print('ods_dc_stock_quotes_di：执行完毕！！！')

def get_group_data(code_list, period, start_date, end_date, adjust, i, n, total):
    result_list = []
    for codes in code_list:
        ak_code = codes[0]
        ak_name = codes[1]
        # print('ods_dc_stock_quotes_di：{}启动,父进程为{}：第{}组/共{}组，{}个)正在处理{}...'.format(os.getpid(), os.getppid(), i, n, total, ak_name))

        df = get_data(ak_code, ak_name, period, start_date, end_date, adjust)
        if df.empty:
            continue
        result_list.extend(np.array(df).tolist())
    return result_list


def get_data(ak_code, ak_name, period, start_date, end_date, adjust):
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
        try:
            df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date,
                                    adjust=adjust)
            if df.empty:
                continue
            if ak_code.startswith('6'):
                df['stock_code'] = 'sh' + ak_code
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                df['stock_code'] = 'bj' + ak_code
            else:
                df['stock_code'] = 'sz' + ak_code

            df['stock_name'] = ak_name
            df.rename(columns={'日期': 'trade_date', '开盘': 'open_price', '收盘': 'close_price', '最高': 'high_price',
                               '最低': 'low_price', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
                               '涨跌幅': 'change_percent',
                               '涨跌额': 'change_amount', '换手率': 'turnover_rate'}, inplace=True)
            df = df[['trade_date', 'stock_code', 'stock_name', 'open_price', 'close_price', 'high_price', 'low_price',
                     'volume',
                     'turnover', 'amplitude', 'change_percent', 'change_amount', 'turnover_rate']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame

# nohup python ods_dc_stock_quotes_di.py update 20221010 20221010 >> my.log 2>&1 &
# python ods_dc_stock_quotes_di.py all
if __name__ == '__main__':
    code_list = get_code_list()
    # code_list = np.array(pd.DataFrame(
    #     [['603182', 'N嘉华'], ['300374', '中铁装配'], ['301033', '迈普医学'], ['600322', '天房发展'], ['300591', '万里马'],
    #      ['300135', '宝利国际']], columns=('代码', '名称')))
    period = 'daily'
    start_date = date.today().strftime('%Y%m%d')
    end_date = start_date
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

    adjust = "hfq"
    engine = sqlalchemyUtil().engine
    process_num = get_process_num()

    start_time = time.time()
    multiprocess_run(code_list, period, start_date, end_date, adjust, engine, process_num)
    engine.dispose()
    end_time = time.time()
    print('程序运行时间：{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))
