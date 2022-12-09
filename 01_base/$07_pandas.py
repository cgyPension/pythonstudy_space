import datetime

import numpy
import numpy as np
import pandas as pd
import warnings
from sqlalchemy import create_engine
import akshare as ak

warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


# todo ====================================================================  apply  ==================================================================
def test_apply():
    """
     DataFrame的apply与Series的apply使用方法类似，但是需要指定函数是按列还是按行进行，
     通过axis参数进行设置，axis默认值为0，表示按列应用指定函数，axis为1时，表示按行应用指定函数

     假如我们想计算open、high、low、close列的均值，首先取出这4列，看一下数据类型：
    """
    df = ak.stock_zh_a_spot_em()

    print(type(df[['今开', '最高', '最低', '最新价']])) #<class 'pandas.core.frame.DataFrame'>

    # 这里调用numpy的mean函数来计算均值。最后我们就可以利用apply方法来计算各列的均值：
    print(df[['今开', '最高', '最低', '最新价']].apply(mean_value))

    # 上面的代码只是用于演示apply的使用方法，也可以使用以下代码实现相同的功能：
    print(df[['今开', '最高', '最低', '最新价']].mean())

    print(df[['今开', '最高', '最低', '最新价']].apply(mean_value_v2))

    print(df.apply(amp, axis=1))

    # 也可以通过以下两种lambda表达式来实现均可：
    print(df.apply(lambda x: (x.最高 - x.最低) / x.最新价, axis=1))
    print(df.apply(lambda x: (x['最高'] - x['最低']) / x['最新价'], axis=1))

    sr = df['代码'].apply(str_replace, c='xxx')
    print(sr)

    # 取1列数据
    print(type(df['代码']))  # <class 'pandas.core.series.Series'>

    # 取1行数据
    print(type(df.iloc[0]))  # <class 'pandas.core.series.Series'>

    print(df['代码'])

    # 对于列Series进行的操作，我们可以把计算结果保存为df的新列：
    df['new_date'] = df['代码'].apply(str_replace, c='xxx')
    print(df)

def str_replace(s, c=''):
    return s.replace('0', c)

# 我们定义计算均值的函数：
def mean_value(row):
    return numpy.mean(row)

"""
如果想在应用函数中访问到具体元素，需要用索引来访问。
我们的示例中，各行的索引是从0到9，假设我们想计算第0、2、5行的均值，可以这样改造代码：
"""
def mean_value_v2(row):
    return (row[0] + row[2] + row[5]) / 3


"""
按行应用可以实现DataFrame单列或者多列之间的运算。
我们以计算振幅=(high-low)/preclose为例：
"""
def amp(col):
    return (col['最高'] - col['最低']) / col['最新价']



# todo ====================================================================  shift  ==================================================================
def test_shift():
    '''
     period：表示移动的幅度，可以是正数，也可以是负数，默认值是1, 1
    就表示移动一次，注意这里移动的都是数据，而索引是不移动的，移动之后没有对应值的，就赋值为NaN。

    freq： DateOffset, timedelta, or time rule string，可选参数，默认值为None，只适用于时间序列，如果这个参数存在，那么会按照参数值移动时间索引，而数据值没有发生变化。

    axis： 轴向。
    '''
    df = pd.DataFrame(np.arange(16).reshape(4, 4), columns=['A', 'B', 'C', 'D'], index=['a', 'b', 'c', 'd'])
    print(df)
    # 当period为正时，默认是axis = 0轴的设定，向下移动
    print(df.shift(2))
    # 当axis=1，沿水平方向进行移动，正数向右移，负数向左移
    print(df.shift(2, axis=1))
    # 当period为负时，默认是axis = 0轴的设定，向上移动
    print(df.shift(-1))

    df2 = pd.DataFrame(np.arange(16).reshape(4, 4), columns=['AA', 'BB', 'CC', 'DD'],index=pd.date_range('6/1/2012', '6/4/2012'))
    print(df2)
    print(df2.shift(freq=datetime.timedelta(1)))
    print(df2.shift(freq=datetime.timedelta(-2)))


def shift_i(df, factor_list, i):
    shift_df = df[factor_list].shift(i)
    shift_df.rename(columns={x: '{}_{}d'.format(x, i) for x in factor_list}, inplace=True)
    df = pd.concat([df, shift_df], axis=1)
    return df

# todo ====================================================================  Pandas内置的聚合方法  ==================================================================
def text_jh():
    # count统计的是非空值的个数，如果我们将某个元素置为空值，统计结果也会发生变化。
    df2 = ak.stock_zh_a_hist(symbol='000609', period="daily", start_date="20220801", end_date="20220808", adjust="hfq")
    df = ak.stock_zh_a_hist(symbol='000609', period="daily", start_date="20220801", end_date="20220808", adjust="hfq")
    df2.loc[0, '最高'] = numpy.nan
    print(df2.count(axis=0))
    print(df.count(axis=1))

    # mean 求行或列的平均值，参数axis=0（默认值）表示按列统计，axis=1表示按行统计
    print(df2[['最高', '收盘']].mean(axis=1))

    # std 求行或列的标准差，参数axis=0（默认值）表示按列统计，axis=1表示按行统计
    print(df2[['最高', '收盘']].std())

    # quantile 取行或列的分位数，参数axis=0（默认值）表示按列统计，axis=1表示按行统计
    print(df2[['最高', '收盘']].quantile(q=0.25))
    print(df2[['最高', '收盘']].quantile(q=0.50))
    print(df2[['最高', '收盘']].quantile(q=0.75))

    # max 按行或列取最大值，参数axis=0（默认值）表示按列统计，axis=1表示按行统计
    print(df2[['最高', '收盘']].max(axis=1))

    # min 按行或列取最小值，参数axis=0（默认值）表示按列统计，axis=1表示按行统计
    print(df2[['最高', '收盘']].min(axis=1))

    # idxmax 按行或者列取最大值第一次出现时的索引，参数axis=0（默认值）表示按列统计，axis=1表示按行统计，skipna表示是否跳过空值
    print(df[['最高', '收盘']].idxmax(axis=0, skipna=True))

    # idxmin 按行或者列取最小值第一次出现时的索引，参数axis=0（默认值）表示按列统计，axis=1表示按行统计，skipna表示是否跳过空值
    print(df[['最高', '收盘']].idxmin(axis=0, skipna=True))

# todo ====================================================================  DataFrame按列内容取不同列的值生成新列  ==================================================================
def test_df():
    df = pd.DataFrame
    df3 = pd.DataFrame([[1, 2, 3, 'A'],
                       [4, 5, 6, 'C'],
                       [7, 8, 9, 'B']], columns=['A', 'B', 'C', 'D'])
    print(df3)

    #    A  B  C  D  E
    # 0  1  2  3  A  1
    # 1  4  5  6  C  6
    # 2  7  8  9  B  8

    """
    第1行，新建列E，赋值为None。如果没有这一步，会导致由于局部赋值，新列结果都为浮点型的情况。
    2~3行，循环对E列赋值。等号左侧df.loc[val == df['D'], 'E']类型为Series，取出的是所有D列值为val的行的E列；等号右侧df[val]类型也是Series，提取的是列名为val的列。
    
    以val=B为例，df.loc[val == df['D'], 'E']取出的是有D列值为B的行的E列
    """

    df3['E'] = None
    for val in df3['D'].unique():
        df3.loc[val == df['D'], 'E'] = df3[val]


# todo ====================================================================  操作mysql  ==================================================================
def test_mysql():
    engine = create_engine('mysql+pymysql://root:123456@hadoop102:3306/test?charset=utf8',
                                        pool_size=10 * 2, max_overflow=10 * 2, pool_timeout=50)

    result_df = pd.DataFrame([[7,7,7]],columns=['id','pro_id', 'price'])
    result_df.to_sql(name='price', con=engine, if_exists='append', index=False, index_label=False,chunksize=1000)

    engine.dispose()
    print('存入成功')

    result_df = pd.DataFrame([[4,'yy'],[2,'oo']],columns=['id', 'num'])
    result_df = np.array(result_df).tolist() # 转化为list
    print(result_df)

# todo ====================================================================  遍历日期区间  ==================================================================

def test_date_range():
    start_date = '20220101'
    end_date = '20221010'
    daterange = pd.date_range(start_date, end_date)
    # print(daterange)
    for single_date in daterange:
        print(single_date.strftime("%Y%m%d"))


    stock_a_indicator_df = ak.stock_a_lg_indicator(symbol="601398")
    print(stock_a_indicator_df[stock_a_indicator_df['trade_date']>datetime.datetime.strptime('20221103', '%Y%m%d').date()])
    print(stock_a_indicator_df[stock_a_indicator_df['trade_date']>pd.to_datetime("2022-11-03").date()])

# todo ====================================================================  空df追加  ==================================================================
def test_append():
    # 不会再原来df上追加 只会形成新的df
    df1 = pd.DataFrame()
    df3 = df1.append(df1)
    print(df3)

    data_dic = {'机型': ['小米12', '华为P40', 'IQOO8', 'iphone13'], '价格': [3999, 5000, 3899, 5999],
                '颜色': ['白色', '紫色', '金色', '白色']}
    df = pd.DataFrame(data_dic)

def test_multiply():
    a = pd.DataFrame({'A': [1, 2, 3]})
    print('乘法：',a.multiply(2))

def test_cumsum():
    # 返回DataFrame或Series轴上的累计和
    s = pd.Series([2, np.nan, 5, -1, 0])
    print(s.cumsum())
    # 要在操作中包含NA值，请使用 skipna=False
    print(s.cumsum(skipna=False))
    pass

def test_concat():
    df1 = pd.DataFrame([['a', 1], ['b', 2]],
                       columns=['A', 'B'])

    df2 = pd.DataFrame([['c', 3], ['d', 4]],
                       columns=['C', 'D'])

    df3 = pd.DataFrame([['d', 1], ['d', 2]],
                       columns=['A', 'B'])

    print(df1)
    print(df2)
    # 横向合并
    print(pd.concat([df1, df2],axis = 1))
    # 纵向合并
    print(pd.concat([df1, df2],axis = 0))
    print(pd.concat([df1, df3],axis = 0))


def test_median():
    '''返回一个 Series，其中包含每列的中值。'''
    data = [[1, 1, 2], [6, 4, 2], [4, 2, 1], [4, 2, 3]]

    df = pd.DataFrame(data)
    print(df)
    print(df.median())

def test_rank():
    '''rank函数中的参数method有四个取值：无参,"min","max","first
    "'''
    ser=pd.Series([3,2,0,3],index=list('abcd'))
    print(ser)
    # 无参相同排名下，取平均值进行排名
    ser = ser.rank()  # 默认为average
    print(ser)

    # 因为a与d的值相同，排名分别为3和4，取较小的排名作为它们的排名，所以a和b的排名为3。
    ser = ser.rank(method='min')
    print(ser)

    # 因为a与d的值相同，排名分别为3和4，取较大的排名作为它们的排名，所以a和b的排名为4。
    ser = ser.rank(method='max')
    print(ser)

    # 相同的值按照出现顺序排列，先出现的值排名靠前（The first value is ranked first），不允许并列排名
    ser = ser.rank(method='first')
    print(ser)

def test_cut():
    '''
    pd.cut()的作用，有点类似给成绩设定优良中差，比如：0-59分为差，60-70分为中，71-80分为优秀等等，在pandas中，也提供了这样一个方法来处理这些事儿。直接上代码
    '''
    np.random.seed(666)
    score_list = np.random.randint(25, 100, size=20)
    print(score_list)

    # 　指定多个区间
    bins = [0, 59, 70, 80, 100]

    score_cut = pd.cut(score_list, bins)
    print(type(score_cut))  # <class 'pandas.core.arrays.categorical.Categorical'>
    print(score_cut)
    print(pd.value_counts(score_cut))  # 统计每个区间人数

    df = pd.DataFrame()
    df['score'] = score_list
    df['student'] = [pd.util.testing.rands(3) for i in range(len(score_list))]
    print(df)

    # 使用cut方法进行分箱
    df['Categories'] = pd.cut(df['score'], bins)
    print(df)

    # 但是这样的方法不是很适合阅读，可以使用cut方法中的label参数
    # 为每个区间指定一个label
    df['label'] = pd.cut(df['score'], bins, labels=['low', 'middle', 'good', 'perfect'])
    print(df)

def test_reindex_like():
    ''' 顾名思义，用另一个 df 的索引来更新当前 df 的索引，原索引中不存在的默认填充 None
        pad/ffill：向前填充值；
        bfill/backfill：向后填充值；
        nearest：从距离最近的索引值开始填充。
    '''
    # reindex_like 要是df2比df1对了不同的字段 reindex_like会删除df2不同的字段
    df1 = pd.DataFrame([[24.3, 75.7, 'high'],
                        [31, 87.8, 'high'],
                        [22, 71.6, 'medium'],
                        [35, 95, 'medium']],
                       columns=['temp_celsius', 'temp_fahrenheit',
                                'windspeed'],
                       index=pd.date_range(start='2014-02-12',
                                           end='2014-02-15', freq='D'))
    print(df1)
    df2 = pd.DataFrame([[28, 'low'],
                        [30, 'low'],
                        [35.1, 'medium']],
                       columns=['temp_celsius', 'windspeed'],
                       index=pd.DatetimeIndex(['2014-02-12', '2014-02-13',
                                               '2014-02-15']))
    print(df2)

    # 未匹配的索引被填充为NaN值
    print(df2.reindex_like(df1))
    print(df2.reindex_like(df1,method='pad'))
    print(df2.reindex_like(df1).fillna(0))

    df3 = pd.DataFrame([[11, 12, 13],
                        [21, 22, 23],
                        [31, 32, 33],
                        [41, 42, 43],
                        [51, 52, 53]],
                       columns=['date','high', 'low'],
                       index=pd.date_range(start='2014-02-12',
                                           end='2014-02-16', freq='D'))
    print(df3)
    df4 = pd.DataFrame([[41, 42,1,42],
                        [51, 52,2,52],
                        [61, 62,3,62]],
                       columns=['high', 'low','rank','rank2'],
                       index=pd.DatetimeIndex(['2014-02-12', '2014-02-13',
                                               '2014-02-15']))
    print(df4)

    df3['rank'] = None
    df3['rank2'] = None
    df4 = df3.reindex_like(df3)
    print(df4)
    df4.loc[:, ['rank', 'rank2']] = df4.loc[:, ['rank', 'rank2']].fillna(9999)
    # 用后面下一日非缺失的填充
    df4.fillna(method='bfill', inplace=True)
    print(df4)

def test_merge():
    '''merge操作类似于数据库当中两张表的join'''
    df1 = pd.DataFrame({'id': [1, 2, 3, 3, 5, 7, 6], 'age': range(7)})
    df2 = pd.DataFrame({'id': [1, 2, 4, 4, 5, 6, 7], 'score': range(7)})
    print(df1)
    print(df2)
    # 这里虽然我们没有指定根据哪一列完成关联，但是pandas会自动寻找两个dataframe的名称相同列来进行关联。一般情况下我们不这么干
    print(pd.merge(df1,df2))
    print(pd.merge(df1,df2,on='id'))
    # 当两个dataframe当中的列名不一致
    df3 = pd.DataFrame({'id': [1, 2, 3, 3, 5, 7, 6], 'age': range(7)})
    df4 = pd.DataFrame({'number': [1, 2, 4, 4, 5, 6, 7], 'score': range(7)})
    print(pd.merge(df3,df4,left_on='id',right_on='number'))

def test_sum():
    ''''''
    idx = pd.MultiIndex.from_arrays([
        ['warm', 'warm', 'cold', 'cold'],
        ['dog', 'falcon', 'fish', 'spider']],
        names=['blooded', 'animal'])
    s = pd.Series([4, 2, 0, 8], name='legs', index=idx)
    print(s)
    print(s.sum())

    # 默认情况下，空系列或全 NA 系列的总和为0.
    print(pd.Series([], dtype="float64").sum())

    # 这可以通过min_count参数来控制。例如，如果您希望空序列的总和为 NaN，请传递min_count=1
    print(pd.Series([], dtype="float64").sum(min_count=1))

def test_cumprod():
    # 采用cumprod(axis = 0)函数可以找到到目前为止沿索引轴看到的值的累积乘积。
    df = pd.DataFrame({"A": [5, 3, 6, 4],
                       "B": [11, 2, 4, 3],
                       "C": [4, 3, 8, 5],
                       "D": [5, 4, 2, 8]})
    print(df)
    print(df.cumprod(axis = 0))

    # 采用cumprod(axis = 1)函数可查找到目前为止沿列轴看到的值的累积乘积。
    print(df.cumprod(axis = 1))

    # 采用cumprod()函数查找迄今在 DataFrame 中沿索引轴看到的值的累积乘积 会跳过NaN DataFrame 中存在的值
    df2 = pd.DataFrame({"A": [5, 3, None, 4],
                       "B": [None, 2, 4, 3],
                       "C": [4, 3, 8, 5],
                       "D": [5, 4, 2, None]})
    df2.cumprod(axis=0, skipna=True)

def test_sort_values():
    '''按任一轴上的值排序
    by -- 指定列名（axis=0或者'index'）或索引值（axis=1或者'columns'）
    axis -- 按行、按列，默认axis=0按指定列排序
    ascending -- 是否升序 默认为True
    inplace -- 是否修改原对象
    kind -- 排序算法 快排quicksort、归并mergesort、堆排序heapsort、稳定排序stable，默认快排
    na_position -- {'first', 'last'} 设定缺失值的显示位置
    ignore_index -- 排序后是否重置索引
    key -- 排序之前使用的函数 （version 1.1.0 后才有该参数）
    '''
    df = pd.DataFrame({'col1': ['A', 'A', 'B', np.nan, 'D', 'C'],
                       'col2': [2, 1, 9, 8, 7, 7],
                       'col3': [0, 1, 2, 9, 4, 8]})
    print(df)

    # 按列排序
    # 依据第一列排序 并将该列空值放在首位 默认字母排序 ASCII码
    print(df.sort_values(by='col1', na_position='first'))
    # 先转换为小写字母再排序
    print(df.sort_values(by='col1', key=lambda x: x.str.lower()))
    # 依据第二、三列倒序
    print(df.sort_values(by=['col2', 'col3'], ascending=False))
    # 替换原数据 索引重置
    print(df.sort_values(by='col1', inplace=True, ignore_index=True))
    # 按第一列降序 第二列升序排列
    print(df.sort_values(by=['col1', 'col2'], ascending=[False, True]))

    # 按行排序
    # 按照索引值为0的行 即第一行的值来降序
    x = pd.DataFrame({'x1': [1, 2, 2, 3],
                      'x2': [4, 3, 2, 1],
                      'x3': [3, 2, 4, 1]})
    print(x)
    print(x.sort_values(by=0, ascending=False, axis=1))

def test_score():
    '''机器学习：
    max-min离差标准化：将原有分布变换到[0,1]：data2 = (data-data.min())/(data.max()-data.min()
    当数据的极差过大时，离差标准化趋于0；
    当数据发生更改后要重新确定[min，max]范围，以免引起系统报错

    z-score标准差标准化：将数据从原分布映射到一个均值为0、标准差为1的新分布：data2 = (data-data.mean())/(data.std())
    '''
    df = pd.DataFrame({'x1': [1, 2, 2, 3],
                      'x2': [4, 3, 2, 1],
                      'x3': [3, 2, 4, 1]})

    df['x1_max-min'] = (df['x1'] - df['x1'].min()) / (df['x1'].max() - df['x1'].min())
    df['x1_z-score'] = (df['x1'] - df['x1'].mean()) / df['x1'].std()
    print(df)

def test_get_dummies():
    '''将离散的变量转化为向量化'''
    df = pd.DataFrame({'name': ['jgf', 'sf', 'sg', 'ljh'],
                      'xb': ['男','女','男','女'],
                      'age': [4, 3, 2, 1]})

    df_cat = pd.get_dummies(df['xb', 'age'])
    hdf = pd.concat([df, df_cat], axis=1)
    print(df_cat)


def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

if __name__ == '__main__':
    test_concat()