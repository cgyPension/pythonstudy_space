import baostock as bs
import datetime
import sys
import numpy as np
import pandas as pd
import multiprocessing
import sqlalchemy
import matplotlib.pyplot as plt
from pandas.plotting import table

# 可用日线数量约束
g_available_days_limit = 250

# BaoStock日线数据字段
g_baostock_data_fields = 'date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ, psTTM,pcfNcfTTM,isST'


def create_mysql_engine():
    """
    创建数据库引擎对象

    :return: 新创建的数据库引擎对象
    """

    # 引擎参数信息
    host = 'localhost'
    user = 'root'
    passwd = '111111'
    port = '3306'
    db = 'db_quant'

    # 创建数据库引擎对象
    mysql_engine = sqlalchemy.create_engine(
        'mysql+pymysql://{0}:{1}@{2}:{3}'.format(user, passwd, host, port),
        poolclass=sqlalchemy.pool.NullPool
    )

    # 如果不存在数据库db_quant则创建
    mysql_engine.execute("CREATE DATABASE IF NOT EXISTS {0} ".format(db))

    # 创建连接数据库db_quant的引擎对象
    db_engine = sqlalchemy.create_engine(
        'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db),
        poolclass=sqlalchemy.pool.NullPool
    )

    # 返回引擎对象
    return db_engine


def get_stock_codes(date=None, update=False):
    """
    获取指定日期的A股代码列表

    若参数update为False，表示从数据库中读取股票列表
    若数据库中不存在股票列表的表，或者update为True，则下载指定日期date的交易股票列表
    若参数date为空，则返回最近1个交易日的A股代码列表
    若参数date不为空，且为交易日，则返回date当日的A股代码列表
    若参数date不为空，但不为交易日，则打印提示非交易日信息，程序退出

    :param date: 日期，默认为None
    :param update: 是否更新股票列表，默认为False
    :return: A股代码的列表
    """

    # 创建数据库引擎对象
    engine = create_mysql_engine()

    # 数据库中股票代码的表名
    table_name = 'stock_codes'

    # 数据库中不存在股票代码表，或者需要更新股票代码表
    if table_name not in sqlalchemy.inspect(engine).get_table_names() or update:

        # 登录baostock
        bs.login()

        # 从BaoStock查询股票数据
        stock_df = bs.query_all_stock(date).get_data()

        # 如果获取数据长度为0，表示日期date非交易日
        if 0 == len(stock_df):

            # 如果设置了参数date，则打印信息提示date为非交易日
            if date is not None:
                print('当前选择日期为非交易日或尚无交易数据，请设置date为历史某交易日日期')
                sys.exit(0)

            # 未设置参数date，则向历史查找最近的交易日，当获取股票数据长度非0时，即找到最近交易日
            delta = 1
            while 0 == len(stock_df):
                stock_df = bs.query_all_stock(datetime.date.today() - datetime.timedelta(days=delta)).get_data()
                delta += 1

        # 注销登录
        bs.logout()

        # 筛选股票数据，上证和深证股票代码在sh.600000与sz.39900之间
        stock_df = stock_df[(stock_df['code'] >= 'sh.600000') & (stock_df['code'] < 'sz.399000')]

        # 将股票代码写入数据库
        stock_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, index_label=False)

        # 返回股票列表
        return stock_df['code'].tolist()

    # 从数据库中读取股票代码列表
    else:

        # 待执行的sql语句
        sql_cmd = 'SELECT {} FROM {}'.format('code', table_name)

        # 读取sql，返回股票列表
        return pd.read_sql(sql=sql_cmd, con=engine)['code'].tolist()


def create_data(stock_codes, from_date='1990-12-19', to_date=datetime.date.today().strftime('%Y-%m-%d'),
                adjustflag='2'):
    """
    下载指定日期内，指定股票的日线数据，计算扩展因子

    :param stock_codes: 待下载数据的股票代码
    :param from_date: 日线开始日期
    :param to_date: 日线结束日期
    :param adjustflag: 复权选项 1：后复权  2：前复权  3：不复权  默认为前复权
    :return: None
    """

    # 创建数据库引擎对象
    engine = create_mysql_engine()

    # 下载股票循环
    for code in stock_codes:
        print('正在下载{}...'.format(code))

        # 登录BaoStock
        bs.login()

        # 下载日线数据
        out_df = bs.query_history_k_data_plus(code, g_baostock_data_fields, start_date=from_date, end_date=to_date,
                                              frequency='d', adjustflag=adjustflag).get_data()

        # 剔除停盘数据
        if out_df.shape[0]:
            out_df = out_df[(out_df['volume'] != '0') & (out_df['volume'] != '')]

        # 如果数据为空，则不创建
        if not out_df.shape[0]:
            continue

        # 删除重复数据
        out_df.drop_duplicates(['date'], inplace=True)

        # 日线数据少于g_available_days_limit，则不创建
        if out_df.shape[0] < g_available_days_limit:
            continue

        # 将数值数据转为float型，便于后续处理
        convert_list = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
        out_df[convert_list] = out_df[convert_list].astype(float)

        # 重置索引
        out_df.reset_index(drop=True, inplace=True)

        # 计算扩展因子
        out_df = extend_factor(out_df)

        # 写入数据库
        table_name = '{}_{}'.format(code[3:], code[:2])
        out_df.to_sql(name=table_name, con=engine, if_exists='replace', index=True, index_label='id')


def get_code_group(process_num, stock_codes):
    """
    获取代码分组，用于多进程计算，每个进程处理一组股票

    :param process_num: 进程数
    :param stock_codes: 待处理的股票代码
    :return: 分组后的股票代码列表，列表的每个元素为一组股票代码的列表
    """

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


def create_data_mp(stock_codes, process_num=61,
                   from_date='1990-12-19', to_date=datetime.date.today().strftime('%Y-%m-%d'), adjustflag='2'):
    """
    使用多进程创建指定日期内，指定股票的日线数据，计算扩展因子

    :param stock_codes: 待创建数据的股票代码
    :param process_num: 进程数
    :param from_date: 日线开始日期
    :param to_date: 日线结束日期
    :param adjustflag: 复权选项 1：后复权  2：前复权  3：不复权  默认为前复权
    :return: None
    """

    multiprocessing_func(create_data, (process_num, stock_codes, from_date, to_date, adjustflag,))


def extend_factor(df):
    """
    计算扩展因子

    :param df: 待计算扩展因子的DataFrame
    :return: 包含扩展因子的DataFrame
    """

    # 使用pipe依次计算涨停、双神及是否为候选股票
    df = df.pipe(zt).pipe(ss, delta_days=30).pipe(candidate)

    return df


def zt(df):
    """
    计算涨停因子

    若涨停，则因子为True，否则为False
    以当日收盘价较前一日收盘价上涨9.8%及以上作为涨停判断标准

    :param df: 待计算扩展因子的DataFrame
    :return: 包含扩展因子的DataFrame
    """

    df['zt'] = np.where((df['close'].values >= 1.098 * df['preclose'].values), True, False)

    return df


def shift_i(df, factor_list, i, fill_value=0, suffix='a'):
    """
    计算移动因子，用于获取前i日或者后i日的因子

    :param df: 待计算扩展因子的DataFrame
    :param factor_list: 待移动的因子列表
    :param i: 移动的步数
    :param fill_value: 用于填充NA的值，默认为0
    :param suffix: 值为a(ago)时表示移动获得历史数据，用于计算指标；值为l(later)时表示获得未来数据，用于计算收益
    :return: 包含扩展因子的DataFrame
    """

    # 选取需要shift的列构成新的DataFrame，进行shift操作
    shift_df = df[factor_list].shift(i, fill_value=fill_value)

    # 对新的DataFrame列进行重命名
    shift_df.rename(columns={x: '{}_{}{}'.format(x, i, suffix) for x in factor_list}, inplace=True)

    # 将重命名后的DataFrame合并到原始DataFrame中
    df = pd.concat([df, shift_df], axis=1)

    return df


def shift_till_n(df, factor_list, n, fill_value=0, suffix='a'):
    """
    计算范围移动因子

    用于获取前/后n日内的相关因子，内部调用了shift_i

    :param df: 待计算扩展因子的DataFrame
    :param factor_list: 待移动的因子列表
    :param n: 移动的步数范围
    :param fill_value: 用于填充NA的值，默认为0
    :param suffix: 值为a(ago)时表示移动获得历史数据，用于计算指标；值为l(later)时表示获得未来数据，用于计算收益
    :return: 包含扩展因子的DataFrame
    """

    for i in range(n):
        df = shift_i(df, factor_list, i + 1, fill_value, suffix)
    return df


def ss(df, delta_days=30):
    """
    计算双神因子，即间隔的两个涨停

    若当日形成双神，则因子为True，否则为False

    :param df: 待计算扩展因子的DataFrame
    :param delta_days: 两根涨停间隔的时间不能超过该值，否则不判定为双神，默认值为30
    :return: 包含扩展因子的DataFrame
    """

    # 移动涨停因子，求取近delta_days天内的涨停情况，保存在一个临时DataFrame中
    temp_df = shift_till_n(df, ['zt'], delta_days, fill_value=False)

    # 生成列表，用于后续检索第2天前至第delta_days天前是否有涨停出现
    col_list = ['zt_{}a'.format(x) for x in range(2, delta_days + 1)]

    # 计算双神，需同时满足3个条件：
    # 1、第2天前至第delta_days天前，至少有1个涨停
    # 2、1天前不是涨停（否则就是连续涨停，不是间隔的涨停）
    # 3、当天是涨停
    df['ss'] = temp_df[col_list].any(axis=1) & ~temp_df['zt_1a'] & temp_df['zt']

    return df


def ma(df, n=5, factor='close'):
    """
    计算均线因子

    :param df: 待计算扩展因子的DataFrame
    :param n: 待计算均线的周期，默认计算5日均线
    :param factor: 待计算均线的因子，默认为收盘价
    :return: 包含扩展因子的DataFrame
    """

    # 均线名称，例如，收盘价的5日均线名称为ma_5，成交量的5日均线名称为volume_ma_5
    name = '{}ma_{}'.format('' if 'close' == factor else factor + '_', n)

    # 取待计算均线的因子列
    s = pd.Series(df[factor], name=name, index=df.index)

    # 利用rolling和mean计算均线数据
    s = s.rolling(center=False, window=n).mean()

    # 将均线数据添加到原始的DataFrame中
    df = df.join(s)

    # 均线数值保留两位小数
    df[name] = df[name].apply(lambda x: round(x + 0.001, 2))

    return df


def mas(df, ma_list, factor='close'):
    """
    计算多条均线因子，内部调用ma计算单条均线

    :param df: 待计算扩展因子的DataFrame
    :param ma_list: 待计算均线的周期列表
    :param factor: 待计算均线的因子，默认为收盘价
    :return: 包含扩展因子的DataFrame
    """

    for i in ma_list:
        df = ma(df, i, factor)
    return df


def cross_mas(df, ma_list):
    """
    计算穿均线因子

    若当日最低价不高于均线价格
    且当日收盘价不低于均线价格
    则当日穿均线因子值为True，否则为False

    :param df: 待计算扩展因子的DataFrame
    :param ma_list: 均线的周期列表
    :return: 包含扩展因子的DataFrame
    """

    for i in ma_list:
        df['cross_{}'.format(i)] = (df['low'] <= df['ma_{}'.format(i)]) & (
                df['ma_{}'.format(i)] <= df['close'])
    return df


def candidate(df):
    """
    计算是否为候选

    若当日日线同时穿过5、10、20、30日均线
    且30日均线在60日均线上方
    且当日形成双神
    则当日作为候选，该因子值为True，否则为False

    :param df: 待计算扩展因子的DataFrame
    :return: 包含扩展因子的DataFrame
    """

    # 均线周期列表
    ma_list = [5, 10, 20, 30, 60]

    # 计算均线的因子，保存到临时的DataFrame中
    temp_df = mas(df, ma_list)

    # 计算穿多线的因子，保存到临时的DataFrame中
    temp_df = cross_mas(temp_df, ma_list)

    # 穿多线因子的列名列表
    column_list = ['cross_{}'.format(x) for x in ma_list[:-1]]

    # 计算是否为候选
    df['candidate'] = temp_df[column_list].all(axis=1) & (temp_df['ma_30'] >= temp_df['ma_60']) & df['ss']

    return df


def profit_loss_statistic(stock_codes, hold_days=10):
    """
    盈亏分布统计，计算当日candidate为True，持仓hold_days天的收益分布

    :param stock_codes: 待分析的股票代码
    :param hold_days: 持仓天数
    :return: 筛选出符合买入条件的股票DataFrame
    """

    # 创建数据库引擎对象
    engine = create_mysql_engine()

    # 创建空的DataFrame
    candidate_df = pd.DataFrame()

    # 计算收益分布循环
    for index, code in enumerate(stock_codes):
        print('({}/{})正在处理{}...'.format(index + 1, len(stock_codes), code))

        # 股票数据在数据库中的表名
        table_name = '{}_{}'.format(code[3:], code[:2])

        # 如果数据库中没有该股票数据则跳过
        if table_name not in sqlalchemy.inspect(engine).get_table_names():
            continue

        # 从数据库读取特定字段数据
        cols = 'date, open, high, low, close, candidate'
        sql_cmd = 'SELECT {} FROM {} ORDER BY date DESC'.format(cols, table_name)
        df = pd.read_sql(sql=sql_cmd, con=engine)

        # 移动第2日开盘价、最低价，以及hold_days的最高价、最低价数据
        df = shift_i(df, ['open'], 1, suffix='l')
        df = shift_till_n(df, ['high', 'low'], hold_days, suffix='l')

        # 丢弃最近hold_days日的数据
        df = df.iloc[hold_days: df.shape[0] - g_available_days_limit, :]

        # 选取出现买点的股票
        df = df[(df['candidate'] > 0) & (df['low_1l'] <= df['close'])]

        # 将数据添加到候选池中
        if df.shape[0]:
            df['code'] = code
            candidate_df = candidate_df.append(df)

    if candidate_df.shape[0]:
        # 计算最大盈利
        # 买入当天无法卖出，因此计算最大收益时，从第2日开始
        cols = ['high_{}l'.format(x) for x in range(2, hold_days + 1)]
        candidate_df['max_high'] = candidate_df[cols].max(axis=1)
        candidate_df['max_profit'] = candidate_df['max_high'] / candidate_df[['open_1l', 'close']].min(axis=1) - 1

        # 计算最大亏损
        cols = ['low_{}l'.format(x) for x in range(2, hold_days + 1)]
        candidate_df['min_low'] = candidate_df[cols].min(axis=1)
        candidate_df['max_loss'] = candidate_df['min_low'] / candidate_df[['open_1l', 'close']].min(axis=1) - 1

    return candidate_df


def multiprocessing_func_df(func, args):
    """
    多进程调用函数，收集返回各子进程返回的DataFrame

    :param func: 函数名
    :param args: func的参数，类型为元组，第0个元素为进程数，第1个元素为股票代码列表
    :return: 包含各子进程返回值的DataFrame
    """

    # 多进程调用函数func，获取子进程返回对象的列表
    results = multiprocessing_func(func, args)

    # 构建空DataFrame
    df = pd.DataFrame()

    # 收集各进程返回值
    for i in results:
        df = df.append(i.get())

    return df


def profit_loss_statistic_mp(stock_codes, process_num=61, hold_days=10):
    """
    多进程盈亏分布统计，计算当日candidate为True，持仓hold_days天的收益分布
    输出数据分布表格及图片文件

    :param stock_codes: 待分析的股票代码
    :param process_num: 进程数
    :param hold_days: 持仓天数
    :return: None
    """

    # 多进程计算获得候选股票数据
    candidate_df = multiprocessing_func_df(profit_loss_statistic, (process_num, stock_codes, hold_days,))

    # 将收益分布数据保存到excel文件
    candidate_df[['date', 'code', 'max_profit', 'max_loss']].to_excel(
        'profit_loss_{}.xlsx'.format(hold_days), index=False, encoding='utf-8')

    # 重置索引，方便后面绘图
    candidate_df.reset_index(inplace=True)

    # 设置绘图格式并绘图
    fig, ax = plt.subplots(1, 1)
    table(ax, np.round(candidate_df[['max_profit', 'max_loss']].describe(), 4), loc='upper right',
          colWidths=[0.2, 0.2, 0.2])
    candidate_df[['max_profit', 'max_loss']].plot(ax=ax, legend=None)

    # 保存图表
    fig.savefig('profit_loss_{}.png'.format(hold_days))

    # 显示图表
    plt.show()


if __name__ == '__main__':
    stock_codes = get_stock_codes()
    profit_loss_statistic_mp(stock_codes)
