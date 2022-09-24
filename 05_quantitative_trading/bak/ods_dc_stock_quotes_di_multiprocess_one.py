import multiprocessing
import os
import sys
import time
from datetime import date, datetime

import numpy as np
import pandas as pd
import warnings


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.DBUtils import sqlalchemyUtil
from util.CommonUtils import get_code_list, get_process_num, multiprocessing_func, get_code_group, get_code_list_v2
import akshare as ak

warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)



# 要弄个标志获取历史数据量大的 基本每天更新的方法 all update 写好注释为什么 进程问题
# 程序内判断 根据日期来判断，一个股票获取5年数据约等于1500条 看看吧，把数据追加到一个空的df再写进去？
# 或者先试试，先不改吧 要是再不行，就变成类的方式运行先把结果存起来 测试下那个接口不传股票代码会不会获取全部股票这样就不用遍历了
# 日更根本不用多进程 至于历史5年以上可改可不改只是跑慢一点(要是历史很多，可以用股票代码分组的函数)
# 先跑20210101～到今年的数据 有回测再把历史补完


# 多进程 要是用类共享restul_df 多进程会有bug
def multiprocess_run(code_list,period,start_date, end_date, adjust, engine, process_num):
    result_list = []
    rl = []
    if start_date is None or end_date is None:
        # 今天 要下午3点后运行
        start_date = date.today().strftime('%Y%m%d')
        end_date = start_date
    else:
        start_date = start_date
        end_date = end_date

    # 创建进程池
    with multiprocessing.Pool(processes=process_num) as pool:
        # 多进程异步计算
        for i in range(len(code_list)):
            ak_code = code_list[i][0]
            ak_name = code_list[i][1]
            # 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
            result_list.append(pool.apply_async(get_data, args=(ak_code, ak_name, period, start_date, end_date, adjust,i,len(code_list),)))
        # 阻止后续任务提交到进程池
        pool.close()
        # 等待所有进程结束
        pool.join()

    for r in result_list:
        df=r.get()
        print('r：',np.array(df).tolist())
        if df.empty:
            continue

        rl.extend(np.array(df).tolist())
    print(rl)
    if rl:
        # 写入mysql append replace
        # 重复主键不插入
        engine.execute(
            """
           insert ignore into ods_dc_stock_quotes_di (trade_date, stock_code, stock_name, open_price, close_price, high_price, low_price,
                                       volume, turnover, amplitude, change_percent, change_amount, turnover_rate)
                                       values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, rl
        )
        sqlalchemyUtil().closeEngine()
    else:
        print('rl为空')





def get_data(ak_code,ak_name,period,start_date, end_date, adjust,i,total):
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
    # print('(启动{},父进程为{}：{}/{})正在处理{}...'.format(os.getpid(), os.getppid(), 1, 1, ak_name))
    print('(启动{},父进程为{}：{}/{})正在处理{}...'.format(os.getpid(), os.getppid(), i + 1, total, ak_name))
    for i in range(1):
        try:
            # 东财接口  没有市值 要从网易接口join 补充
            df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date,
                                    adjust=adjust)
            if df.empty:
                continue
            if ak_code.startswith('6'):
                df['stock_code'] = ak_code + '.SH'
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                df['stock_code'] = ak_code + '.BJ'
            else:
                df['stock_code'] = ak_code + '.SZ'

            df['stock_name'] = ak_name
            df.rename(columns={'日期': 'trade_date', '开盘': 'open_price', '收盘': 'close_price', '最高': 'high_price',
                               '最低': 'low_price', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
                               '涨跌幅': 'change_percent',
                               '涨跌额': 'change_amount', '换手率': 'turnover_rate'}, inplace=True)
            df = df[['trade_date', 'stock_code', 'stock_name', 'open_price', 'close_price', 'high_price', 'low_price',
                     'volume',
                     'turnover', 'amplitude', 'change_percent', 'change_amount', 'turnover_rate']]
            # print('get_data：',df)
            print(df)
            return df
        except Exception as e:
            print(e)
    return pd.DataFrame



# code_list = get_code_list_v2()
code_list = np.array(pd.DataFrame([['603182','N嘉华'],['300374','中铁装配'],['301033','迈普医学'],['600322','天房发展'],['300591','万里马'],['300135','宝利国际']], columns=('代码', '名称')))
period = 'daily'
start_date = '20220916'
end_date = '20220916'
adjust = "hfq"
engine = sqlalchemyUtil().engine
# target_table=os.path.splitext(os.path.basename(__file__))[0]
target_table = 'ods_dc_stock_quotes_di'
timeout = 10  # 函数运行限时超时时间
# process_num = get_process_num()
process_num = 3
if __name__ == '__main__':
    start_time = time.time()
    # common_run(code_list, period, start_date, end_date, adjust, engine)
    multiprocess_run(code_list, period, start_date, end_date, adjust, engine, process_num)
    print('程序运行时间：{}s'.format(time.time() - start_time))
