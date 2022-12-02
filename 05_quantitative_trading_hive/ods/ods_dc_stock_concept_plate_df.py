import datetime
import multiprocessing
import os
import sys
import time
import warnings
from datetime import date
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import akshare as ak
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


from util.CommonUtils import get_process_num, get_concept_plate_group, str_pre, get_spark


def multiprocess_run(process_num):
    appName = os.path.basename(__file__)
    # 本地模式
    spark = get_spark(appName)

    concept_plate_group = get_concept_plate_group(process_num)
    result_list = []

    with multiprocessing.Pool(processes=process_num) as pool:
        # 多进程异步计算
        for i in range(len(concept_plate_group)):
            codes = concept_plate_group[i]
            # 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
            result_list.append(pool.apply_async(get_group_data, args=(
            codes, i, len(concept_plate_group),)))
        # 阻止后续任务提交到进程池
        pool.close()
        # 等待所有进程结束
        pool.join()

    pd_df = pd.DataFrame()
    for r in result_list:
        rl = r.get()
        if rl.empty:
            print('rl为空')
        else:
            pd_df = pd_df.append(rl)

    pd_df['update_time'] = datetime.datetime.now()
    spark_df = spark.createDataFrame(pd_df)
    # 全量覆盖
    spark_df.repartition(1).write.insertInto('stock.ods_dc_stock_concept_plate_df', overwrite=True)
    spark.stop
    print('{}：执行完毕！！！'.format(appName))

def get_group_data(concept_plates, i, n):
    pd_df = pd.DataFrame()
    for concept_plate in concept_plates:
        # print('ods_dc_stock_concept_plate_df：{}启动,父进程为{}：第{}组/共{}组)正在处理...'.format(os.getpid(), os.getppid(), i, n))

        df = get_data(concept_plate)
        if df.empty:
            continue
        pd_df = pd_df.append(df)
    return pd_df


def get_data(concept_plate):
    """
    获取指定日期的A股数据写入mysql

    :param start_date: 数据获取开始日期
    :param end_date: 数据获取结束日期
    :return: None
    """
    # time.sleep(1)
    for i in range(1):
        try:
            df = ak.stock_board_concept_cons_em(symbol=concept_plate)
            if df.empty:
                continue
            # 去重、保留最后一次出现的
            df.drop_duplicates(subset=['代码'], keep='last', inplace=True)

            df['stock_code'] = df['代码'].apply(str_pre)
            df['concept_plate'] = concept_plate

            df.rename(columns={'名称': 'stock_name'}, inplace=True)
            df = df[['stock_code','stock_name','concept_plate']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame

# spark-submit /opt/code/05_quantitative_trading_hive/ods/ods_dc_stock_concept_plate_df.py
# nohup ods_dc_stock_concept_plate_df.py >> my.log 2>&1 &
# python ods_dc_stock_concept_plate_df.py
if __name__ == '__main__':
    process_num = get_process_num()
    start_time = time.time()
    multiprocess_run(process_num)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))