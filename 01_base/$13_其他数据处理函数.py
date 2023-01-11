import numpy as np
import pandas as pd
import statsmodels
import statsmodels.api as sm

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

def test_std():
    '''
    numpy.std() 求标准差的时候默认是除以 n 的，即是有偏的，np.std无偏样本标准差方式为加入参数 ddof = 1；
    pandas.std() 默认是除以n-1 的，即是无偏的，如果想和numpy.std() 一样有偏，需要加上参数ddof=0 ，即pandas.std(ddof=0) ；
    hive是除以n
    sparksql是除以n-1的

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




def test_eval():
    '''eval() 函数用来执行一个字符串表达式，并返回表达式的值。
       eval() 执行完要返回结果，而 exec() 执行完不返回结果'''
    x = 7
    print(eval('3 * x'))
    print(eval('pow(2,2)'))

def test_OLS():
    '''
    OLS就是用样本数据拟合出最小二乘最小的系数组合
    自变量和因变量都只能是数字
    第一个输入 endog 是回归模型中的因变量, 输入是一个维向量。第二个输入 exog 是自变量，即个样本点构成的维数组
    missing：可用的选项是“无”、“下降”和“加注”。如果“无”，则不进行 nan 检查。如果 'drop'，任何带有 nans 的观察结果都会被丢弃。如果 'raise'，则会引发错误。默认为“无”。
    hasconst：指示 RHS 是否包含用户提供的常量。如果为 True，则不检查常量并将 k_constant 设置为 1，并且计算所有结果统计信息，就好像存在常量一样。如果为 False，则不检查常量并将 k_constant 设置为 0

    对于行业回归，只能用多个行业做字段0 和1
    '''
    df = pd.DataFrame({'x1': [1, 2, 2, 3],
                      'x2': [4, 3, 2, 1],
                      'x3': [3, 2, 4, 1],
                      'x4': ['电气', '银行', '食品', '光伏']})
    print(df)
    # model = sm.OLS(df['x1'],df['x4'])
    # 将分类变量转变为虚拟变量 方式一  prefix='x4' 可以加前缀
    # dummies = pd.get_dummies(df['x4'],prefix='x4')
    # df = df.drop(['x4'],axis=1).join(dummies)
    # print(df)
    # 将分类变量转变为虚拟变量 方式二
    # a = sm.categorical(df['x4'],drop=True)
    # df = df.drop(['x4'], axis=1).join(a)
    # print(df)

    model = sm.OLS(df['x1'],df['x2'],hasconst=False,missing='drop')
    results = model.fit()
    print(results.resid)


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
    test_OLS()


































