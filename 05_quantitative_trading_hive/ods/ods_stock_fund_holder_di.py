import multiprocessing
import os
import sys
import time
import warnings
from datetime import date, datetime
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
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.DBUtils import hiveUtil
from util.CommonUtils import get_code_group, get_code_list, get_spark, str_pre

'''
接口有问题 只能等官方
'''
def multiprocess_run(start_date, end_date,code_list, hive_engine, process_num):
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    code_group = get_code_group(process_num, code_list)
    result_list = []

    daterange = pd.date_range(pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date(), freq='Q-Mar')
    if daterange.empty:
        # 增量 覆盖
        end_date_year_start = pd.to_datetime('20210101').date()
        announcement_date = pd.date_range(end_date_year_start, pd.to_datetime(end_date).date(), freq='Q-Mar')
        announcement_date_df = pd.DataFrame(announcement_date)
        # 上一期的日期 上上一期的日期
        single_date, lag_single_date = announcement_date_df.iloc[-1, 0], announcement_date_df.iloc[-2, 0]
        start_date, end_date = pd.to_datetime(lag_single_date).date(), pd.to_datetime(end_date).date()
    else:
        # start_date, end_date = pd.to_datetime(pd.DataFrame(daterange).iloc[0,0]).date(), pd.to_datetime(pd.DataFrame(daterange).iloc[-1,0]).date()
        start_date, end_date = pd.to_datetime(pd.DataFrame(daterange).iloc[0,0]).date(), pd.to_datetime(end_date).date()

    with multiprocessing.Pool(processes=process_num) as pool:
        for i in range(len(code_group)):
            codes = code_group[i]
            # 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
            result_list.append(pool.apply_async(get_group_data, args=(start_date,codes, i, len(code_group), len(code_list),)))
        pool.close()
        pool.join()

    # 这里多进程写入 不可以直接用overwrite
    # hive_engine.execute("""alter table stock.ods_stock_fund_holder_di drop if exists partition (td >= '%s',td <='%s')""" % (pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()))
    for r in result_list:
        rl = r.get()
        if rl.empty:
            print('rl为空')
        else:
            print(rl)
    #         spark_df = spark.createDataFrame(rl)
    #         spark_df.repartition(1).write.insertInto('stock.ods_stock_fund_holder_di', overwrite=False)
    #
    # # 多进程的需要合并分区内小文件
    # hive_sql="""show partitions %s""" % ('stock.ods_stock_fund_holder_di')
    # pd_df = pd.read_sql(hive_sql, hive_engine)
    # pd_df['partition'] = pd.to_datetime(pd_df['partition'].apply(lambda x: x.split('=')[1]))
    # pd_df = pd_df[(pd_df.partition >= pd.to_datetime(start_date)) & (pd_df.partition <= pd.to_datetime(end_date))]
    # for single_date in pd_df.partition:
    #     hive_engine.execute("""alter table stock.ods_stock_fund_holder_di partition (td ='%s') concatenate""" % (single_date.strftime("%Y-%m-%d")))
    # spark.stop
    print('{}：执行完毕！！！'.format(appName))

def get_group_data(start_date,code_list, i, n, total):
    pd_df = pd.DataFrame()
    for codes in code_list:
        ak_code = codes[0]
        ak_name = codes[1]
        df = get_data(start_date,ak_code, ak_name)
        if df.empty:
            continue
        pd_df = pd_df.append(df)
    return pd_df


def get_data(start_date,ak_code,ak_name):
    print(start_date,ak_code,ak_name)
    # time.sleep(1)
    for i in range(1):
        try:
            df = ak.stock_fund_stock_holder(ak_code)
            print(df)
            if df.empty:
                continue
            df.drop_duplicates(subset=['截止日期','基金代码'], keep='last', inplace=True)
            df = df[pd.to_datetime(df['截止日期']) >= pd.to_datetime(start_date)]

            df['stock_code'] = str_pre(ak_code)
            df['stock_name'] = ak_name
            df['截止日期'] = pd.to_datetime(df['截止日期'])
            df['td'] = df['截止日期']
            df['update_time'] = datetime.now()

            df.rename(columns={'基金名称':'fund_name','基金代码':'fund_code','持仓数量(股)':'hold_stock_nums','占流通股比例(%)':'hold_circulating_nums_rate','持股市值（元）':'hold_market','截止日期':'announcement_date'}, inplace=True)
            df = df[['announcement_date','stock_code','stock_name','fund_code','fund_name','hold_stock_nums','hold_circulating_nums_rate','hold_market','update_time','td']]
            df = df.replace({np.nan: None})
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_stock_fund_holder_di.py all
# spark-submit /opt/code/05_quantitative_trading_hive/ods/ods_stock_fund_holder_di.py all
# nohup python ods_stock_fund_holder_di.py update 20221010 >> my.log 2>&1 &
# 这个全量很慢 平时不能全量 要取最新日期
if __name__ == '__main__':
    # code_list = get_code_list()
    code_list = np.array(pd.DataFrame(
        [['603182', 'N嘉华'], ['300374', '中铁装配'], ['301033', '迈普医学'], ['600322', '天房发展'], ['300591', '万里马'],
         ['300135', '宝利国际']], columns=('代码', '名称')))
    start_date = date.today().strftime('%Y%m%d')
    end_date = start_date
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

    hive_engine = hiveUtil().engine
    start_time = time.time()
    multiprocess_run(start_date, end_date,code_list, hive_engine,2)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))