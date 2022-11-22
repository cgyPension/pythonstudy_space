# 使用AKSHARE + mysql 实现动态抓取个股的交易历史数据
# 同理外面再包一层循环就可以把所有的交易历史数据下载每个股票一个表。
# 后续下载历史数据并且定制下每天更新脚本这样历史交易数据就解决了。
#
# 后续就是弄个回测框架
#
# 添加宏观因素 再添加个股微观因素   再历史回测因素相关性
import time
from datetime import datetime

import pandas as pd
import warnings
from sqlalchemy import create_engine
import akshare as ak
import multiprocessing
warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows',None)
pd.set_option('max_columns',None)
pd.set_option('expand_frame_repr',False)
pd.set_option('display.unicode.ambiguous_as_wide',True)
pd.set_option('display.unicode.east_asian_width',True)

# myslq url
engine = create_engine('mysql+pymysql://root:123456@hadoop102:3306/stock?charset=utf8')

# 参数设置
# period="daily" # 周期 'daily', 'weekly', 'monthly'
# start_date="20220822" # 数据获取开始日期
# end_date="20220822" # 数据获取结束日期
# adjust="hfq" # 复权类型 qfq": "前复权", "hfq": "后复权", "": "不复权"

def get_code_list(date=None):
    # 利用东财实时行情数据接口获取沪深京A股
    df = ak.stock_zh_a_spot_em()
    # 筛选股票数据，上证和深证股票
    code_list = df[['代码', '名称']].values
    return code_list
    # 返回股票列表

def get_stock(code_list=get_code_list(),period="daily",start_date=None,end_date=None,adjust="hfq",engine=engine):
    """
    获取指定日期的A股数据写入mysql

    :param period: 周期 'daily', 'weekly', 'monthly'
    :param start_date: 数据获取开始日期
    :param end_date: 数据获取结束日期
    :param adjust: 复权类型 qfq": "前复权", "hfq": "后复权", "": "不复权"
    :param engine: myslq url
    :return: None
    """

    if start_date or end_date is None:
        # 今天 要下午3点后运行
        start_date = datetime.date.today().strftime('%Y%m%d')
        end_date = start_date

    # 利用东财实时行情数据接口获取沪深京A股
    # df = ak.stock_zh_a_spot_em()
    # # 筛选股票数据，上证和深证股票
    # code_list = df[['代码', '名称']].values
    # code_list = df[(df['代码'][0]!='8'|df['代码'][0]!='4')].values

    # stock_df = stock_df[(stock_df['code'] >= 'sh.600000') & (stock_df['code'] < 'sz.399000')]

    # 获取所有历史上股票的历史数据
    for i in range(len(code_list)):
        ak_code = code_list[i][0]
        ak_name = code_list[i][1]
        # print(f"已下载到{i+1}支股票，股票代码为：{ak_code}，股票名称为：{ak_name}")

        try:
            # 东财接口  没有市值 要从网易接口join 补充
            stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date, adjust=adjust)
        except Exception as e:
            print(e)

        try:
            # 网易接口 获取 总市值 流通市值
            stock_zh_a_hist_163_df = ak.stock_zh_a_hist_163(symbol=ak_code, start_date=start_date, end_date=end_date)
        except Exception as e:
            print(e)

        try:
            # 东方财富网-数据中心-特色数据-两市停复牌
            stock_tfp_em_df = ak.stock_tfp_em(date="20220523")
        except Exception as e:
            print(e)

        if stock_zh_a_hist_df.empty==False:
            # # 在股票代码前加上交易所简称
            if ak_code.startswith('6') == True:
                stock_zh_a_hist_df['股票代码'] = ak_code + '.SH'
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                stock_zh_a_hist_df['股票代码'] = ak_code + '.BJ'
            else:
                stock_zh_a_hist_df['股票代码'] = ak_code + '.SZ'

            stock_zh_a_hist_df['股票名称'] = ak_name
            stock_zh_a_hist_df.rename(columns={'日期': '交易日期', '开盘': '开盘价', '收盘': '收盘价', '最高': '最高价', '最低': '最低价'},
                                      inplace=True)
            stock_zh_a_hist_df = stock_zh_a_hist_df[
                ['交易日期', '股票代码', '股票名称', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]

            # 排序、去重
            stock_zh_a_hist_df.sort_values(by=['交易日期'], ascending=True, inplace=True)
            stock_zh_a_hist_df.drop_duplicates(subset=['股票代码','交易日期'],keep='first',inplace=True)
            stock_zh_a_hist_df.reset_index(drop=True, inplace=True)

            # 写入mysql 会自动建表
            stock_zh_a_hist_df.to_sql('ods_dc_stock_quotes_di', engine, chunksize=100000, index=None)
            # print('存入成功！')
            continue


def get_code_group(process_num, code_list):
    """
    获取代码分组，用于多进程计算，每个进程处理一组股票

    :param process_num: 进程数 多数调用均使用默认值为61
    :param stock_codes: 待处理的股票代码
    :return: 分组后的股票代码列表，列表的每个元素为一组股票代码的列表
    """
    stock_codes = code_list[['代码']]
    # 创建空的分组
    code_group = [[] for i in range(process_num)]

    # 按余数为每个分组分配股票
    for index, code in enumerate(stock_codes):
        code_group[index % process_num].append(code)

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

def create_data_mp(code_list, process_num=61):
    """
    使用多进程创建指定日期内，指定股票的日线数据，计算扩展因子

    :param stock_codes: 待创建数据的股票代码
    :param process_num: 进程数
    :param from_date: 日线开始日期
    :param to_date: 日线结束日期
    :param adjustflag: 复权选项 1：后复权  2：前复权  3：不复权  默认为前复权
    :return: None
    """

    multiprocessing_func(get_stock, (process_num, code_list))

if __name__ == '__main__':
    start_time = time.time()
    # stock_codes = get_stock_codes()
    # update_data_mp(stock_codes)
    # update_trade(stock_codes)
    end_time = time.time()
    print('程序运行时间：{}s'.format(end_time - start_time))