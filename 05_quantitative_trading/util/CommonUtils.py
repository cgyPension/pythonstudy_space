import os

import pandas as pd
import warnings
import akshare as ak
import multiprocessing


warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows',None)
pd.set_option('max_columns',None)
pd.set_option('expand_frame_repr',False)
pd.set_option('display.unicode.ambiguous_as_wide',True)
pd.set_option('display.unicode.east_asian_width',True)

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

def get_code_list_v2():
    # 利用东财实时行情数据接口获取沪深京A股
    df = ak.stock_zh_a_spot_em()
    # 筛选股票数据，上证和深证股票
    code_list = df[['代码', '名称']].values
    return code_list
    # 返回股票列表

def get_code_group_v2(process_num, code_list):
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

def get_code_list():
    # 利用东财实时行情数据接口获取沪深京A股
    df = ak.stock_zh_a_spot_em()
    # 筛选股票数据，上证和深证股票
    # code_list = df[['代码', '名称']].values
    code_list = df['代码']
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
    for index, stock_code in enumerate(code_list):
        # code_group[index % process_num].append([stock_code])
        code_group[index % process_num].append(stock_code)

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
        for codes in get_code_group(args[0], args[1]):
            results.append(pool.apply_async(func, args=(codes, *args[2:],)))

        # 阻止后续任务提交到进程池
        pool.close()

        # 等待所有进程结束
        pool.join()

    return results


def get_code_list_mysql(engine):

    # 利用东财实时行情数据接口获取沪深京A股
    df = ak.stock_zh_a_spot_em()
    code_list = df[['代码', '名称']].values
    for ak_code,ak_name in code_list:

        if ak_code.startswith('6'):
            ak_code = ak_code + '.SH'
        elif ak_code.startswith('8') or ak_code.startswith('4') == True:
            ak_code = ak_code + '.BJ'
        else:
            ak_code = ak_code + '.SZ'

        result_df = pd.DataFrame([[ak_code, ak_name]], columns=['stock_code','stock_name'])
        print(result_df)
        result_df.to_sql(name='ods_dc_stock_code_list_df', con=engine,if_exists='append', index=None)