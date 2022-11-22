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
from util.CommonUtils import get_process_num, get_code_group, get_code_list, get_industry_plate_group, str_pre


def multiprocess_run(engine, process_num):
    industry_plate_group = get_industry_plate_group(process_num)
    result_list = []

    with multiprocessing.Pool(processes=process_num) as pool:
        # 多进程异步计算
        for i in range(len(industry_plate_group)):
            codes = industry_plate_group[i]
            # 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
            result_list.append(pool.apply_async(get_group_data, args=(
            codes, i, len(industry_plate_group),)))
        # 阻止后续任务提交到进程池
        pool.close()
        # 等待所有进程结束
        pool.join()

    engine.execute("""truncate table ods_dc_stock_industry_plate_df;""")
    for r in result_list:
        rl = r.get()
        if rl:
            # 写入mysql append replace
            # 重复主键不插入
            engine.execute(
                """
                insert ignore into ods_dc_stock_industry_plate_df (stock_code, stock_name, industry_plate)
                                     values (%s, %s, %s);
                """, rl
            )
        else:
            print('rl为空')
    print('ods_dc_stock_board_industry_cons_df：执行完毕！！！')

def get_group_data(industry_plates, i, n):
    result_list = []
    for industry_plate in industry_plates:
        # print('ods_dc_stock_board_industry_cons_df：{}启动,父进程为{}：第{}组/共{}组)正在处理...'.format(os.getpid(), os.getppid(), i, n))

        df = get_data(industry_plate)
        if df.empty:
            continue
        result_list.extend(np.array(df).tolist())
    return result_list


def get_data(industry_plate):
    """
    获取指定日期的A股数据写入mysql

    :param start_date: 数据获取开始日期
    :param end_date: 数据获取结束日期
    :return: None
    """
    # time.sleep(1)
    for i in range(1):
        try:
            df = ak.stock_board_industry_cons_em(symbol=industry_plate)

            if df.empty:
                continue
            df['stock_code'] = df['代码'].apply(str_pre)
            df['industry_plate'] = industry_plate

            df.rename(columns={'名称': 'stock_name'}, inplace=True)
            df = df[['stock_code','stock_name','industry_plate']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame

# nohup ods_dc_stock_industry_plate_df.py >> my.log 2>&1 &
# python ods_dc_stock_industry_plate_df.py
if __name__ == '__main__':
    engine = sqlalchemyUtil().engine
    process_num = get_process_num()
    start_time = time.time()
    multiprocess_run(engine, process_num)
    engine.dispose()
    end_time = time.time()
    print('程序运行时间：{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))