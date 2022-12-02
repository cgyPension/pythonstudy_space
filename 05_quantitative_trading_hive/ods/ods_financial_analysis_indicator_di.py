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
from util.CommonUtils import get_code_group, get_code_list, get_spark

# 这接口太脆了 不能开太多进程
def multiprocess_run(code_list, start_date, hive_engine, process_num = 2):
    appName = os.path.basename(__file__)
    # 本地模式
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

    end_date = date.today()

    # 这里多进程写入 不可以直接用overwrite
    hive_engine.execute("""alter table stock.ods_financial_analysis_indicator_di drop if exists partition (td >= '%s',td <='%s')""" % (pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()))
    for r in result_list:
        rl = r.get()
        if rl.empty:
            print('rl为空')
        else:
            spark_df = spark.createDataFrame(rl)
            spark_df.repartition(1).write.insertInto('stock.ods_financial_analysis_indicator_di', overwrite=False)

    # 多进程的需要合并分区内小文件
    hive_sql="""show partitions %s""" % ('stock.ods_financial_analysis_indicator_di')
    pd_df = pd.read_sql(hive_sql, hive_engine)
    pd_df['partition'] = pd.to_datetime(pd_df['partition'].apply(lambda x: x.split('=')[1]))
    pd_df = pd_df[(pd_df.partition >= pd.to_datetime(start_date)) & (pd_df.partition <= pd.to_datetime(end_date))]
    for single_date in pd_df.partition:
        hive_engine.execute("""alter table stock.ods_financial_analysis_indicator_di partition (td ='%s') concatenate""" % (single_date.strftime("%Y-%m-%d")))
    # merge_df = spark.sql("""
    # select *
    # from stock.ods_financial_analysis_indicator_di
    #     where td between '%s' and '%s'
    #         """ % (start_date, end_date))
    # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 1 个数据文件。
    # merge_df.repartition(1).write.insertInto('stock.ods_financial_analysis_indicator_di', overwrite=True)
    spark.stop
    print('{}：执行完毕！！！'.format(appName))

def get_group_data(code_list, start_date, i, n, total):
    pd_df = pd.DataFrame()
    for codes in code_list:
        ak_code = codes[0]
        ak_name = codes[1]
        # print('ods_financial_analysis_indicator_di：{}启动,父进程为{}：第{}组/共{}组，{}个)正在处理{}...'.format(os.getpid(), os.getppid(), i, n, total, ak_name))
        df = get_data(ak_code, ak_name,start_date)
        if df.empty:
            continue
        pd_df = pd_df.append(df)
    return pd_df


def get_data(ak_code, ak_name,start_date):

    # time.sleep(1)
    for i in range(1):
        try:
            # print(ak_code, ak_name)
            # 新浪财经-财务分析-财务指标
            df = ak.stock_financial_analysis_indicator(symbol=ak_code)
            if df.empty:
                continue
            df.drop_duplicates(subset=['日期'], keep='last', inplace=True)
            df = df[pd.to_datetime(df['日期']) >= pd.to_datetime(start_date)]

            if ak_code.startswith('6'):
                df['stock_code'] = 'sh' + ak_code
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                df['stock_code'] = 'bj' + ak_code
            else:
                df['stock_code'] = 'sz' + ak_code

            df['stock_name'] = ak_name
            df['日期'] = pd.to_datetime(df['日期'])
            df['td'] = df['日期']
            df['update_time'] = datetime.now()


            # df['每股经营性现金流(元)'] = df['每股经营性现金流(元)'].astype(float)
            # df['净资产收益率(%)'] = df['净资产收益率(%)'].astype(float)
            # df['扣除非经常性损益后的净利润(元)'] = df['扣除非经常性损益后的净利润(元)'].astype(float)
            # df['净利润增长率(%)'] = df['净利润增长率(%)'].astype(float)
            # 把所有列的类型都转化为数值型，出错的地方填入NaN，再把NaN的地方补0
            df['每股经营性现金流(元)'] = df['每股经营性现金流(元)'].apply(pd.to_numeric, errors='coerce').fillna(0.0)
            df['净资产收益率(%)'] = df['净资产收益率(%)'].apply(pd.to_numeric, errors='coerce').fillna(0.0)
            df['扣除非经常性损益后的净利润(元)'] = df['扣除非经常性损益后的净利润(元)'].apply(pd.to_numeric, errors='coerce').fillna(0.0)
            df['净利润增长率(%)'] = df['净利润增长率(%)'].apply(pd.to_numeric, errors='coerce').fillna(0.0)

            df.rename(columns={'日期':'announcement_date','每股经营性现金流(元)':'ps_business_cash_flow','净资产收益率(%)':'return_on_equity','扣除非经常性损益后的净利润(元)':'npadnrgal','净利润增长率(%)':'net_profit_growth_rate'}, inplace=True)
            df = df[['announcement_date','stock_code','stock_name','ps_business_cash_flow','return_on_equity','npadnrgal','net_profit_growth_rate','update_time','td']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame

# python /opt/code/05_quantitative_trading_hive/ods/ods_financial_analysis_indicator_di.py all
# spark-submit /opt/code/05_quantitative_trading_hive/ods/ods_financial_analysis_indicator_di.py all
# nohup python ods_financial_analysis_indicator_di.py update 20221010 >> my.log 2>&1 &
# 这个全量很慢 平时不能全量 要取最新日期
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
        else:
            start_date = date.today().strftime('%Y%m%d')
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]

    hive_engine = hiveUtil().engine
    start_time = time.time()
    multiprocess_run(code_list, start_date, hive_engine)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))