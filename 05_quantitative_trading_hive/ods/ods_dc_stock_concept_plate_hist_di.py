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

from util.CommonUtils import get_process_num, get_concept_plate_group, get_spark

def multiprocess_run(start_date, end_date,process_num):
    appName = os.path.basename(__file__)
    spark = get_spark(appName)

    concept_plate_group = get_concept_plate_group(process_num)
    result_list = []

    with multiprocessing.Pool(processes=process_num) as pool:
        # 多进程异步计算
        for i in range(len(concept_plate_group)):
            codes = concept_plate_group[i]
            # 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
            result_list.append(pool.apply_async(get_group_data, args=(codes, start_date, end_date,i, len(concept_plate_group),)))
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

    spark_df = spark.createDataFrame(pd_df)
    # 全量覆盖
    spark_df.repartition(1).write.insertInto('stock.ods_dc_stock_concept_plate_hist_di', overwrite=True)
    spark.stop
    print('{}：执行完毕！！！'.format(appName))

def get_group_data(concept_plates, start_date, end_date, i, n):
    pd_df = pd.DataFrame()
    for concept in concept_plates:
        concept_plate_code = concept[0]
        concept_plate = concept[1]
        # print('ods_dc_stock_concept_plate_df：{}启动,父进程为{}：第{}组/共{}组)正在处理...'.format(os.getpid(), os.getppid(), i, n))
        df = get_data(concept_plate_code,concept_plate, start_date, end_date)
        if df.empty:
            continue
        pd_df = pd_df.append(df)
    return pd_df


def get_data(concept_plate_code,concept_plate, start_date, end_date):
    """
    获取指定日期的A股数据写入mysql

    :param start_date: 数据获取开始日期
    :param end_date: 数据获取结束日期
    :return: None
    """
    # time.sleep(1)
    for i in range(1):
        try:
            # 这个历史接口三种复权方式价格 都是一样的 指数好像是没有复权的
            df = ak.stock_board_concept_hist_em(symbol=concept_plate, start_date=start_date,end_date=end_date,period='daily', adjust='qfq')
            if df.empty:
                continue
            # 去重、保留最后一次出现的
            df.drop_duplicates(subset=['日期'], keep='last', inplace=True)

            df['trade_date'] = df['日期'].apply(lambda x: pd.to_datetime(x).date())
            df['concept_plate_code'] = concept_plate_code
            df['concept_plate'] = concept_plate
            df['update_time'] = datetime.datetime.now()
            df['td'] = df['trade_date']

            df.rename(columns={'开盘': 'open_price', '收盘': 'close_price', '最高': 'high_price','最低': 'low_price', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude','涨跌幅': 'change_percent',
                               '涨跌额': 'change_amount', '换手率': 'turnover_rate'}, inplace=True)
            df = df[['trade_date', 'concept_plate_code','concept_plate', 'open_price', 'close_price', 'high_price', 'low_price', 'change_percent', 'change_amount', 'volume','turnover', 'amplitude','turnover_rate', 'update_time', 'td']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_dc_stock_concept_plate_hist_di.py update 20210104 20221216
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_dc_stock_concept_plate_hist_di.py all
# nohup python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_dc_stock_concept_plate_hist_di.py update 20221121 20221121 >> my.log 2>&1 &
if __name__ == '__main__':
    process_num = get_process_num()
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

    start_time = time.time()
    multiprocess_run(start_date, end_date,process_num)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))