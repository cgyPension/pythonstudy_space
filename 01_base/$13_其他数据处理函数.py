import numpy as np
import pandas as pd

def test_stack():
    # 列旋转函数
    # stack函数是根据index标签进行了分类，把列转换为行。
    # unstack:把行转换为列。
    # pivot(index='trade_date', columns='stock_code',values='close_price') 根据列名转换
    f = {'id': pd.Series(['Amy', 'Bob', 'Cathy', 'David', 'Harry'], index=[1, 2, 3, 4, 5]),
         'age': pd.Series([22, 21, 24, 26], index=[1, 2, 3, 4]),
         }
    df1 = pd.DataFrame(f)
    print(df1)

    a=df1.stack()
    print(a)

    # unstack('order_id') 可以指定列
    b=df1.unstack(level=0)
    print(type(b),b)

    c=df1.unstack(level=-1)
    print(type(c),c)

    print(df1.unstack().unstack())

def test_pct_change():
    '''表示当前元素与先前元素的相差百分比，当然指定periods=n,表示当前元素与先前n 个元素的相差百分比
       第一行会为 NaN
    '''
    df = pd.DataFrame({'FR': [4, 4, 4], 'GR': [3, 5, 4], 'IT': [4, 4, 4]},
                      index=['1980-01-01', '1980-02-01', '1980-03-01'])
    print(df)
    # 按行计算 (5-3)/3
    print(df.pct_change())
    # 按列计算
    print(df.pct_change(axis='columns'))

    s = pd.Series([90, 91, 85])
    s.pct_change(periods=2)  # 表示当前元素与先前两个元素百分比
    # （85-90）/90=-0.055556

def test_dropna():
    '''通过dropna()剔除 空值 缺失数据'''
    # se1 = pd.Series([4, NaN, 8, NaN, 5])
    se1 = pd.Series([4, None, 8, None, 5])
    print(se1)
    print(se1.dropna())

    df1 = pd.DataFrame([[1, 2, 3], [None, None, 2], [None, None, None], [8, 8, None]])
    print(df1)
    print('默认剔除除所有包含NaN',df1.dropna())
    print('传入how=‘all’滤除全为NaN的行',df1.dropna(how='all'))
    df1[3] = None
    print(df1)
    print('剔除全为NaN的列',df1.dropna(axis=1,how="all"))
    print('传入thresh=n保留至少有n个非NaN数据的行：',df1.dropna(thresh=1))

def test_rolling():
    '''
    rolling滑动窗口默认是从右往左，每次滑行并不是区间整块的滑行
    mean() 计算均值
    3歩长 个数取一个均值。index 0,1 为NaN，是因为它们前面都不够3个数，等到index2 的时候，它的值是怎么算的呢，就是（index0+index1+index2 ）/3
    index3 的值就是（ index1+index2+index3）/ 3

    window： 也可以省略不写。表示时间窗的大小，注意有两种形式（int or offset）。如果使用int，则数值表示计算统计量的观测值的数量即向前几个数据。如果是offset类型，表示时间窗的大小。offset详解
    min_periods：每个窗口最少包含的观测值数量，小于这个值的窗口结果为NA。值可以是int，默认None。offset情况下，默认为1。
    center: 把窗口的标签设置为居中。布尔型，默认False，居右
    win_type: 窗口的类型。截取窗的各种函数。字符串类型，默认为None。各种类型
    on: 可选参数。对于dataframe而言，指定要计算滚动窗口的列。值为列名。
    axis: int、字符串，默认为0，即对列进行计算
    closed：定义区间的开闭，支持int类型的window。对于offset类型默认是左开右闭的即默认为right。可以根据情况指定为left both等。
    '''
    s = [1, 2, 3, 5, 6, 10, 12, 14, 12, 30]
    a = pd.Series(s).rolling(window=3).mean()
    print(a)
    b = pd.Series(s).rolling(3,min_periods=2).mean()
    print(b)

def test_std():
    '''
    numpy.std() 求标准差的时候默认是除以 n 的，即是有偏的，np.std无偏样本标准差方式为加入参数 ddof = 1；
    pandas.std() 默认是除以n-1 的，即是无偏的，如果想和numpy.std() 一样有偏，需要加上参数ddof=0 ，即pandas.std(ddof=0) ；

    公式意义 ：所有数减去平均值,它的平方和除以数的个数（或个数减一),再把所得值开根号,就是1/2次方,得到的数就是这组数的标准差。
    '''
    a = np.array([1,2,3,4])
    # 计算float64中的标准偏差更为准确
    print(np.std(a, dtype=np.float64))
    print(np.std(a,  ddof=1,dtype=np.float64))

def test_shift():
    '''shift函数是对数据进行移动的操作 索引不变'''
    df = pd.DataFrame({"Col1": [10, 20, 15, 30, 45],
                       "Col2": [13, 23, 18, 33, 48],
                       "Col3": [17, 27, 22, 37, 52]},
                      index=pd.date_range("2020-01-01", "2020-01-05"))
    print('df:',df)
    print(df.shift())



def test_fillna():
    '''
    填充缺失数据
    inplace参数的取值：True、False
    True：直接修改原对象
    False：创建一个副本，修改副本，原对象不变（缺省默认）
    method参数的取值 ： {‘pad’, ‘ffill’,‘backfill’, ‘bfill’, None}, default None
    pad/ffill：用前一个非缺失值去填充该缺失值
    backfill/bfill：用下一个非缺失值填充该缺失值
    None：指定一个值去替换缺失值（缺省默认这种方式）
    limit参数：限制填充个数
    axis参数：修改填充方向
    :return:
    '''
    df = pd.DataFrame([[1, 2, 3], [None, None, 2], [None, None, None], [8, 8, None]])
    print(df)
    print(df.fillna(100))
    #用字典填充
    print(df.fillna({0: 10, 1: 20, 2: 30}))

    df2 = pd.DataFrame(np.random.randint(0, 10, (5, 5)))
    df2.iloc[1:4, 3] = None
    df2.iloc[2:4, 4] = None
    print(df2)
    print(df2.fillna(method='ffill'))
    print(df2.fillna(method='bfill'))
    print(df2.fillna(method='bfill', limit=2))
    print(df2.fillna(method="ffill", limit=1, axis=1))




def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

if __name__ == '__main__':
    test_stack()



































