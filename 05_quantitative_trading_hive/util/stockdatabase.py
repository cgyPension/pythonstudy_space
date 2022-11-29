import time

import pandahouse as ph
from clickhouse_driver import Client

"""
别人封装的clickhouse
注意：该封装涉及到股票相关的表，必须以 date,code 作为排序值
查询某些列的时候，默认是返回 date
更新数据传入的dataframe，index 必须是date
"""

'''
数据的存储和获取
CREATE TABLE stock.stock_daily_price
(
    `date`   Date,
    `code`   String,
    `open`   Float32,
    `high`   Float32,
    `low`    Float32,
    `close`  Float32,
    `volume` Float64,
    `amount` Float64
--     `adj_factor` Int32,
--     `st_status` Int16,
--     `trade_status` Int16
) ENGINE = ReplacingMergeTree()
      ORDER BY (javaHash(code), date)
'''

'''
pandahouse 是通过http url 链接，端口号是8123
'''
connection = dict(database="stock",
                  host="http://localhost:8123",
                  user='default',
                  password='sykent')

'''
clickhouse_driver 是通过TCP链接，端口号是9000
'''
client = Client(database="stock",
                host='127.0.0.1',
                port='9000',
                user='default',
                password='sykent')


def to_table(data, table):
    """
    插入数据到表
    :param data:
    :param table:
    :return:
    """
    affected_rows = ph.to_clickhouse(data, table=table, connection=connection)
    return affected_rows


def from_table(sql):
    """
    查询表
    :param sql:
    :return: dataframe
    """
    last_time = time.time()
    df = ph.read_clickhouse(sql, connection=connection)
    print("db-> 耗时: {}  sql: {}".format((time.time() - last_time) * 1000, sql))
    return df


def stock_daily(code, start_time, end_time, use_col=None):
    """
    获取某股票，某时间段的日行情数据
    select *
    from stock_daily_price
    where code == '000001' and date between '2022-03-30' and '2022-07-29'
    :param code:
    :param start_time:
    :param end_time:
    :param use_col: list 需要返回的列，默认返回 'date' 并设置为 index
    :return:
    """
    if use_col is None:
        sql = "select * from stock.stock_daily_price where code == '{}' and date between '{}' and '{}'" \
            .format(code, start_time, end_time)
    else:
        # 组拼接查询对应列的sql语句
        columns = ','.join(map(lambda x: f'columns(\'{x}\')', use_col))
        columns = 'columns(\'date\'),' + columns
        sql = f"select {columns} from stock.stock_daily_price where code == '{code}' and date between '{start_time}' and '{end_time}'"
    df = from_table(sql)
    df.set_index('date', inplace=True)

    return df


def stock_indicator(code, start_time, end_time, use_col=None):
    """
    获取某股票，某时间段的日行情数据
    select *
    from stock_indicator
    where code == '000001' and date between '2022-03-30' and '2022-07-29'
    :param code:
    :param start_time:
    :param end_time:
    :param use_col: list 需要返回的列，默认返回 'date' 并设置为 index
    :return:
    """

    if use_col is None:
        sql = "select * from stock.stock_indicator where code == '{}' and date between '{}' and '{}'" \
            .format(code, start_time, end_time)
    else:
        # 组拼接查询对应列的sql语句
        columns = ','.join(map(lambda x: f'columns(\'{x}\')', use_col))
        columns = 'columns(\'date\'),' + columns
        sql = f"select {columns} from stock.stock_indicator where code == '{code}' and date between '{start_time}' and '{end_time}'"
    df = from_table(sql)
    df.set_index('date', inplace=True)

    return df


def all_stock_daily(start_time, end_time):
    """
    获取所有股票某时间段的日行情数据
    select *
    from stock.stock_daily_price
    where date between '2022-03-30' and '2022-07-29'
    :param start_time:
    :param end_time:
    :return:
    """
    sql = "select * from stock.stock_daily_price where date between '{}' and '{}'" \
        .format(start_time, end_time)
    return from_table(sql)


def pool_stock_daily(stock_pool, start_time, end_time):
    """
    获取股票池某时间段的日行情数据
    eg：pool_stock_daily(('000001', '000002'), '2021-01-01', '2022-09-30')
    :param stock_pool: 股票池 数据类型元组 eg:('000001', '000002')
    :param start_time: 开始时间
    :param end_time: 结束时间
    :return:
    """
    sql = f'select * from stock.stock_daily_price where date between \'{start_time}\' and \'{end_time}\' and code in {stock_pool}'
    return from_table(sql)


def create_daily_price_table():
    """
    创建日行情数据表
    :return:
    """
    columns = {'date': 'Date',
               'code': 'String',
               'open': 'Float32',
               'high': 'Float32',
               'low': 'Float32',
               'close': 'Float32',
               'volume': 'Float64',
               'amount': 'Float64'
               }
    creat_stock_related_table('stock_daily_price', **columns)


def create_indicator_table():
    """
    创建股票指标表，用于存储计算指标，所有指标只与自身相关
    ampXXX 自身震荡幅度指标
    maXXX  均值指标
    :return:
    """
    columns = {
        'date': 'Date',
        'code': 'String',
        'amp5': 'Float32',
        'amp10': 'Float32',
        'amp20': 'Float32',
        'amp50': 'Float32',
        'amp120': 'Float32',
        'amp250': 'Float32',
        'ma5': 'Float32',
        'ma10': 'Float32',
        'ma20': 'Float32',
        'ma50': 'Float32',
        'ma120': 'Float32',
        'ma250': 'Float32'
    }
    creat_stock_related_table('stock_indicator', **columns)


def to_indicator_table(data):
    to_table(data, 'stock_indicator')


def creat_stock_related_table(table_name, **kwargs):
    """
    创建股票相关的表
    注意：一定需要date,code这两列，作为排序值
    :param table_name: 表名
    :param kwargs: 列名
    :return:
    """
    columns_str = ''
    for key, value in kwargs.items():
        columns_str = columns_str + f'{key} {value},'
    columns_str = columns_str[:len(columns_str) - 1]
    if 'code' not in columns_str or 'date' not in columns_str:
        raise Exception('not column code date!!')
    sql = f'create table if not exists {table_name}({columns_str}) ' \
          f'engine=ReplacingMergeTree ORDER BY(javaHash(code), date)'
    print('创建表sql:', sql)
    client.execute(sql)


def creat_table(table_name,
                engine=None,
                order_by=None,
                **column_args):
    """ 创建表
    example:
    columns = {'date': 'Date',
               'code': 'String',
               'close': 'Float32',
               'open': 'Float32'}
    sdb.creat_table(table_name="name",
                    engine='ReplacingMergeTree',
                    order_by='(javaHash(code), date)',
                    **columns)
    :param table_name:表名
    :param engine:引擎
    :param order_by:排序值
    :param column_args:列参数
    :return:
    """
    columns_str = ''
    for key, value in column_args.items():
        columns_str = columns_str + f'{key} {value},'
    columns_str = columns_str[:len(columns_str) - 1]
    sql = f'create table if not exists {table_name}({columns_str})'
    if engine is not None:
        sql = sql + f' engine={engine} '
    if order_by is not None:
        sql = sql + f' ORDER BY{order_by}'
    print('创建表sql:', sql)
    client.execute(sql)


def stock_length(code):
    """
    查询股票上市最小日期
    :param code:
    :return:
    """
    sql = f'select count() from stock.stock_daily_price where code == \'{code}\''
    count = client.execute(sql)[0][0]
    print('stock_length sql：', sql, f'result count {count}')
    return count


def update_data_to_indicator_table(update_df, code, start_time, end_time):
    """
    更新单个股票的数据
    由于clickhouse并不支持实际的局部更新，所以采取的解决方案时先查数据出来，
    更新对应的数据，再整行 insert ,最后手动触发去重
    :param update_df: 需要更新的数据
    :param code: 对应的股票代码
    :param start_time: 
    :param end_time:
    :return:
    """
    src_df = stock_indicator(code, start_time, end_time)
    print(src_df)
    # 缺失的列
    lose_col = list(set(src_df.columns).difference(set(update_df.columns)))
    # 多余的列
    excess_col = list(set(update_df.columns).difference(set(src_df.columns)))

    # 补全缺失的列
    if len(lose_col) > 0:
        update_df[lose_col] = src_df[lose_col]
    # 删除多余的列
    if len(excess_col) > 0:
        update_df.drop(excess_col, axis=1, inplace=True)
    to_table(update_df, 'stock_indicator')


def drop_table(table_name):
    """
    删除表
    :param table_name:
    :return:
    """
    sql = f'drop table if exists {table_name}'
    client.execute(sql)
    print('删除表sql：', sql)


def optimize(table_name):
    """
    手动触发数据表去重操作
    场景: 在更新表后，由于重复的ReplacingMergeTree是不定时触发的，
    所以可以强制调用触发。
    :param table_name:
    :return:
    """
    sql = f'optimize table stock.{table_name}'
    client.execute(sql)


if __name__ == '__main__':
    # stock_length('000005')
    # columns = {
    #     'date': 'Date',
    #     'code': 'String',
    #     'amp5': 'Float32',
    #     'amp10': 'Float32',
    #     'amp20': 'Float32',
    # }
    #
    # creat_stock_related_table('test', **columns)
    # df = pd.DataFrame(columns=['date', 'code', 'amp5', 'amp10', 'amp20'])
    # df.loc[len(df), df.columns] = ('2022-10-10', '2555', 5.0, 4.1, 5.0)
    # df.set_index('date', inplace=True)
    # to_table(df, 'test')
    # df.reset_index(inplace=True)
    # df.loc[len(df), ['date', 'code', 'amp5']] = ('2022-10-10', '2555', 4.0)
    # df.set_index('date', inplace=True)
    # print(df)
    # to_table(df[['code', 'amp5']], 'test')

    # update_sql = 'alter table stock.test UPDATE amp5 =554.0 where code=\'2555\' and date = \'2022-10-10\''
    # client.execute(update_sql)
    # optimize_sql = 'optimize table stock.test'
    # client.execute(optimize_sql)
    # sql = "select * from stock.test"
    # print(from_table(sql))

    df = stock_daily('000001', '2021-01-01', '2022-09-30', use_col=['close', 'code'])
    print(df)
    pass
