import os
import sys
from datetime import date

import pandas as pd
import warnings
import akshare as ak
import multiprocessing

from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows',None)
pd.set_option('max_columns',None)
pd.set_option('expand_frame_repr',False)
pd.set_option('display.unicode.ambiguous_as_wide',True)
pd.set_option('display.unicode.east_asian_width',True)

# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)


def get_spark(appName):
    # 也可以在 spark-defaults.conf 全局配置 使用Arrow pd_df spark_df提高转换速度
    # spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # hive.metastore.uris: 访问hive metastore 服务的地址    # .master('local')\
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName(appName)\
        .config('hive.metastore.uris', 'thrift://hadoop102:9083') \
        .config('spark.debug.maxToStringFields', '200') \
        .config('spark.sql.debug.maxToStringFields', '200') \
        .config('spark.sql.sources.partitionOverwriteMode', 'dynamic') \
        .config('hive.exec.dynamic.partition', 'true') \
        .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true') \
        .enableHiveSupport().getOrCreate()
    return spark


def get_trade_date():
    """获得交易日"""
    current_dt = date.today()
    # 新浪财经的股票交易日历数据
    df = ak.tool_trade_date_hist_sina()
    df = df[df['trade_date'] > pd.to_datetime(current_dt).date()].reset_index(drop=True)
    current_dt = df.iat[0, 0]  # 下一个交易日

def get_process_num():
    """
    获取建议进程数目

    对于I/O密集型任务，建议进程数目为CPU核数/(1-a)，a去0.8~0.9

    :return: 进程数目
    """

    # return min(60, int(os.cpu_count() / (1 - 0.9)))
    # akshare接口 不用time.sleep(1) 最大为6 获取请求就不会报错
    return min(6, int(os.cpu_count() / (1 - 0.9)))
    # akshare接口 用time.sleep(1) 最大为30 获取请求就不会报错
    # return min(30, int(os.cpu_count() / (1 - 0.9)))


def str_pre(s):
    '''
    股票代码加前缀
    '''
    s=str(s)
    ak_code = None
    if s.startswith('6'):
        ak_code = 'sh' + s
    elif s.startswith('8') or s.startswith('4') == True:
        ak_code = 'bj' + s
    else:
        ak_code = 'sz' + s
    return ak_code

def get_code_list():
    # 利用东财实时行情数据接口获取沪深京A股
    df = ak.stock_zh_a_spot_em()
    # 排除 京股
    # df = df[~df['代码'].str.startswith(('8', '4'))]
    # 筛选股票数据，上证和深证股票
    code_list = df[['代码', '名称']].values
    return code_list
    # 返回股票列表

def get_code_group(process_num, code_list):
    """
    获取代码分组，用于多进程计算，每个进程处理一组股票

    :param process_num: 进程数 多数调用均使用默认值为61
    :param stock_codes: 待处理的股票代码
    :return: 分组后的股票代码列表，列表的每个元素为一组股票代码的列表
    """
    # stock_codes = code_list[['代码']]
    # 创建空的分组
    # code_group = [[] for i in range(process_num)]
    code_group = [[] for i in range(process_num)]

    # 按余数为每个分组分配股票
    for index, codes in enumerate(code_list):
        # code_group[index % process_num].append([stock_code])
        code_group[index % process_num].append([codes[0],codes[1]])

    return code_group

def get_industry_plate_group(process_num):
    """
    获取代码分组，用于多进程计算，每个进程处理一组股票

    :param process_num: 进程数 多数调用均使用默认值为61
    :param stock_codes: 待处理的股票代码
    :return: 分组后的股票代码列表，列表的每个元素为一组股票代码的列表
    """
    stock_board_industry_name_em_df = ak.stock_board_industry_name_em()
    industry_plates = stock_board_industry_name_em_df['板块名称']

    code_group = [[] for i in range(process_num)]

    # 按余数为每个分组分配股票
    for index, industry_plate in enumerate(industry_plates):
        # code_group[index % process_num].append([stock_code])
        code_group[index % process_num].append(industry_plate)

    return code_group

def get_concept_plate_group(process_num):
    """
    获取代码分组，用于多进程计算，每个进程处理一组股票

    :param process_num: 进程数 多数调用均使用默认值为61
    :param stock_codes: 待处理的股票代码
    :return: 分组后的股票代码列表，列表的每个元素为一组股票代码的列表
    """
    stock_board_concept_name_em_df = ak.stock_board_concept_name_em()
    concept_plates = stock_board_concept_name_em_df['板块名称']

    code_group = [[] for i in range(process_num)]

    # 按余数为每个分组分配股票
    for index, concept_plate in enumerate(concept_plates):
        code_group[index % process_num].append(concept_plate)

    return code_group




def multiprocessing_func(func, args):
    """
    多进程调用函数

    :param func: 函数名
    :param args: func的参数，类型为元组，第0个元素为进程数，第1个元素为股票代码列表
    :return: 包含各子进程返回对象的列表
    """

    # 用于保存各子进程返回对象的列表
    results = []

    # 创建进程池
    with multiprocessing.Pool(processes=args[0]) as pool:
        # 多进程异步计算
        for codes in get_code_group(args[0], args[1])[0]:
            results.append(pool.apply_async(func, args=(codes, *args[2:],)))

        # 阻止后续任务提交到进程池
        pool.close()

        # 等待所有进程结束
        pool.join()

    return results


