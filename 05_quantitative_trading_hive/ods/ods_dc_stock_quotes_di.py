import multiprocessing
import os
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
from util.DBUtils import hiveUtil
from util.CommonUtils import get_process_num, get_code_group, get_code_list, get_spark
warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


def multiprocess_run(code_list, period, start_date, end_date, adjust,hive_engine,process_num):
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
            result_list.append(pool.apply_async(get_group_data, args=(codes, period, start_date, end_date, adjust, i, len(code_group), len(code_list),)))
        # 阻止后续任务提交到进程池
        pool.close()
        # 等待所有进程结束
        pool.join()

    # 这里多进程写入 不可以直接用overwrite
    hive_engine.execute("""alter table stock.ods_dc_stock_quotes_di drop if exists partition (td >= '%s',td <='%s')""" % (pd.to_datetime(start_date).date(),pd.to_datetime(end_date).date()))
    for r in result_list:
        rl = r.get()
        if rl.empty:
            print('rl为空')
        else:
            spark_df = spark.createDataFrame(rl)
            spark_df.repartition(1).write.insertInto('stock.ods_dc_stock_quotes_di', overwrite=False)

    # 多进程的需要合并分区内小文件
    # 全量的话用hive合并分区很慢
    hive_sql="""show partitions %s""" % ('stock.ods_dc_stock_quotes_di')
    pd_df = pd.read_sql(hive_sql, hive_engine)
    pd_df['partition'] = pd.to_datetime(pd_df['partition'].apply(lambda x: x.split('=')[1]))
    pd_df = pd_df[(pd_df.partition >= pd.to_datetime(start_date)) & (pd_df.partition <= pd.to_datetime(end_date))]
    for single_date in pd_df.partition:
        hive_engine.execute("""alter table stock.ods_dc_stock_quotes_di partition (td ='%s') concatenate""" % (single_date.strftime("%Y-%m-%d")))
    # spark.sql("""
    # select *
    # from stock.ods_dc_stock_quotes_di
    #     where td between '%s' and '%s'
    #         """ % (start_date, end_date)).createOrReplaceTempView('tmp_merge_ods_dc_stock_quotes_di')
    #
    # merge_df = spark.sql("""select * from tmp_merge_ods_dc_stock_quotes_di""")
    # 不知道为什么覆盖分区失效 再删一次
    # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 1 个数据文件。
    # merge_df.repartition(1).write.insertInto('stock.ods_dc_stock_quotes_di', overwrite=True)
    spark.stop
    print('{}：执行完毕！！！'.format(appName))

def get_group_data(code_list, period, start_date, end_date, adjust, i, n, total):
    pd_df = pd.DataFrame()
    for codes in code_list:
        ak_code = codes[0]
        ak_name = codes[1]
        # print('ods_dc_stock_quotes_di：{}启动,父进程为{}：第{}组/共{}组，{}个)正在处理{}...'.format(os.getpid(), os.getppid(), i, n, total, ak_name))

        df = get_data(ak_code, ak_name, period, start_date, end_date, adjust)
        if df.empty:
            continue
        pd_df = pd_df.append(df)
    return pd_df


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
            df.drop_duplicates(subset=['日期'], keep='last', inplace=True)

            if ak_code.startswith('6'):
                df['stock_code'] = 'sh' + ak_code
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                df['stock_code'] = 'bj' + ak_code
            else:
                df['stock_code'] = 'sz' + ak_code

            df['stock_name'] = ak_name
            df['update_time'] = datetime.now()
            df['日期'] = pd.to_datetime(df['日期'])
            df['td'] = df['日期']
            df.rename(columns={'日期': 'trade_date', '开盘': 'open_price', '收盘': 'close_price', '最高': 'high_price',
                               '最低': 'low_price', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
                               '涨跌幅': 'change_percent',
                               '涨跌额': 'change_amount', '换手率': 'turnover_rate'}, inplace=True)
            df = df[['trade_date', 'stock_code', 'stock_name', 'open_price', 'close_price', 'high_price', 'low_price',
                     'volume',
                     'turnover', 'amplitude', 'change_percent', 'change_amount', 'turnover_rate','update_time','td']]
            # MySQL无法处理nan
            df = df.replace({np.nan: None})
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame

# 有hive连接不能用spark-submit方式提交？
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_dc_stock_quotes_di.py update 20221121 20221121
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_dc_stock_quotes_di.py all
# nohup python /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/ods_dc_stock_quotes_di.py update 20221121 20221121 >> my.log 2>&1 &
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
    process_num = get_process_num()
    hive_engine = hiveUtil().engine
    start_time = time.time()
    multiprocess_run(code_list, period, start_date, end_date, adjust,hive_engine, process_num)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))
