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

def multiprocess_run(code_list, start_date, engine, process_num):
    code_group = get_code_group_v2(process_num, code_list)
    result_list = []

    with multiprocessing.Pool(processes=process_num) as pool:
        # 多进程异步计算
        for i in range(len(code_group)):
            codes = code_group[i]
            # 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
            result_list.append(pool.apply_async(get_group_data, args=(codes, start_date, i, len(code_group), len(code_list),)))
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
                insert ignore into ods_lg_indicator_di (trade_date, stock_code, stock_name, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
                                 total_market_value)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, rl
            )
        else:
            print('rl为空')
    sqlalchemyUtil().closeEngine()


def get_group_data(code_list, start_date, i, n, total):
    result_list = []
    for codes in code_list:
        ak_code = codes[0]
        ak_name = codes[1]
        print('ods_lg_indicator_di：{}启动,父进程为{}：第{}组/共{}组，{}个)正在处理{}...'.format(os.getpid(), os.getppid(), i, n, total, ak_name))

        df = get_data(ak_code, ak_name,start_date)
        if df.empty:
            continue
        result_list.extend(np.array(df).tolist())
    return result_list


def get_data(ak_code, ak_name,start_date):
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
            # 东财接口  没有市值 要从网易接口join 补充
            df = ak.stock_a_lg_indicator(symbol=ak_code)
            df = df[df['trade_date'] >= pd.to_datetime(start_date).date()]

            if df.empty:
                continue
            if ak_code.startswith('6'):
                df['stock_code'] = 'sh' + ak_code
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                df['stock_code'] = 'bj' + ak_code
            else:
                df['stock_code'] = 'sz' + ak_code

            df['stock_name'] = ak_name
            df.rename(columns={'total_mv': 'total_market_value'}, inplace=True)
            df = df[['trade_date','stock_code','stock_name','pe','pe_ttm','pb','ps','ps_ttm','dv_ratio','dv_ttm','total_market_value']]
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame

# nohup python ods_lg_indicator_di.py update 20221010 >> my.log 2>&1 &
# python ods_lg_indicator_di.py all
if __name__ == '__main__':
    code_list = get_code_list_v2()

    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        run_type = sys.argv[1]
        if run_type == 'all':
            start_date = '20210101'
        else:
            start_date = date.today().strftime('%Y%m%d')
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]

    engine = sqlalchemyUtil().engine
    process_num = get_process_num()

    start_time = time.time()
    multiprocess_run(code_list, start_date, engine, process_num)
    end_time = time.time()
    print('程序运行时间：{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))