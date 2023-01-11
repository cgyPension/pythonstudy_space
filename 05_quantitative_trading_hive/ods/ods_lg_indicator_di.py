import multiprocessing
import os
import random
import sys
import time
import warnings
from datetime import date, datetime
import akshare as ak
import numpy as np
import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
from util.DBUtils import hiveUtil
from util.CommonUtils import get_process_num, get_code_group, get_code_list, get_spark

def multiprocess_run(code_list, start_date,end_date,hive_engine, process_num):
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    code_group = get_code_group(process_num, code_list)
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

    # 这里多进程写入 不可以直接用overwrite
    hive_engine.execute("""alter table stock.ods_lg_indicator_di drop if exists partition (td >= '%s',td <='%s')""" % (pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()))
    for r in result_list:
        rl = r.get()
        if rl.empty:
            print('rl为空')
        else:
            spark_df = spark.createDataFrame(rl)
            spark_df.repartition(1).write.insertInto('stock.ods_lg_indicator_di', overwrite=False)

    # 多进程的需要合并分区内小文件
    hive_sql="""show partitions %s""" % ('stock.ods_lg_indicator_di')
    pd_df = pd.read_sql(hive_sql, hive_engine)
    pd_df['partition'] = pd.to_datetime(pd_df['partition'].apply(lambda x: x.split('=')[1]))
    pd_df = pd_df[(pd_df.partition >= pd.to_datetime(start_date)) & (pd_df.partition <= pd.to_datetime(end_date))]
    for single_date in pd_df.partition:
        hive_engine.execute("""alter table stock.ods_lg_indicator_di partition (td ='%s') concatenate""" % (single_date.strftime("%Y-%m-%d")))

    spark.stop
    print('{}：执行完毕！！！'.format(appName))

def get_group_data(code_list, start_date, i, n, total):
    pd_df = pd.DataFrame()
    for codes in code_list:
        ak_code = codes[0]
        ak_name = codes[1]
        # print('ods_lg_indicator_di：{}启动,父进程为{}：第{}组/共{}组，{}个)正在处理{}...'.format(os.getpid(), os.getppid(), i, n, total, ak_name))

        df = get_data(ak_code, ak_name,start_date)
        if df.empty:
            continue
        pd_df = pd_df.append(df)
    return pd_df


def get_group_data_write(code_list, start_date):
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    pd_df = pd.DataFrame()
    for codes in code_list:
        ak_code = codes[0]
        ak_name = codes[1]
        # print('ods_lg_indicator_di：{}启动,父进程为{}：第{}组/共{}组，{}个)正在处理{}...'.format(os.getpid(), os.getppid(), i, n, total, ak_name))

        df = get_data(ak_code, ak_name,start_date)
        if df.empty:
            continue
        pd_df = pd_df.append(df)
    spark_df = spark.createDataFrame(pd_df)
    spark_df.repartition(1).write.insertInto('stock.ods_lg_indicator_di', overwrite=True)

def get_data(ak_code, ak_name,start_date):
    # time.sleep(random.randint(5,10))
    for i in range(1):
        try:
            # 乐咕乐股-A 股个股指标表 没有京股数据会是随机数
            df = ak.stock_a_lg_indicator(symbol=ak_code)
            if df.empty:
                continue
            df = df[df['trade_date'] >= pd.to_datetime(start_date).date()]
            df.drop_duplicates(subset=['trade_date'], keep='last', inplace=True)
            if ak_code.startswith('6'):
                df['stock_code'] = 'sh' + ak_code
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                df['stock_code'] = 'bj' + ak_code
            else:
                df['stock_code'] = 'sz' + ak_code

            df['stock_name'] = ak_name
            df['td'] = pd.to_datetime(df['trade_date'])
            df['update_time'] = datetime.now()

            df.rename(columns={'total_mv': 'total_market_value'}, inplace=True)
            df = df[['trade_date','stock_code','stock_name','pe','pe_ttm','pb','ps','ps_ttm','dv_ratio','dv_ttm','total_market_value','update_time','td']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame

# nohup python ods_lg_indicator_di.py update 20221010 >> my.log 2>&1 &
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_lg_indicator_di.py all
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_lg_indicator_di.py update 20230105 20230105
if __name__ == '__main__':
    code_list = get_code_list()
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

    hive_engine = hiveUtil().engine
    process_num = get_process_num()

    start_time = time.time()
    multiprocess_run(code_list, start_date,end_date, hive_engine, 6)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))