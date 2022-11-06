import numpy
import numpy as np
import pandas as pd
import warnings
from sqlalchemy import create_engine
import akshare as ak

warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

df = ak.stock_zh_a_spot_em()

# 取1列数据
print(type(df['代码'])) #<class 'pandas.core.series.Series'>

# 取1行数据
print(type(df.iloc[0])) #<class 'pandas.core.series.Series'>

print(df['代码'])

def str_replace(s, c=''):
    return s.replace('0', c)

sr = df['代码'].apply(str_replace,c='xxx')
print(sr)

# 对于列Series进行的操作，我们可以把计算结果保存为df的新列：
df['new_date'] = df['代码'].apply(str_replace,c='xxx')
print(df)


# todo ====================================================================  apply  ==================================================================
"""
 DataFrame的apply与Series的apply使用方法类似，但是需要指定函数是按列还是按行进行，
 通过axis参数进行设置，axis默认值为0，表示按列应用指定函数，axis为1时，表示按行应用指定函数
 
 假如我们想计算open、high、low、close列的均值，首先取出这4列，看一下数据类型：
"""
print(type(df[['今开', '最高', '最低', '最新价']])) #<class 'pandas.core.frame.DataFrame'>

# 我们定义计算均值的函数：
def mean_value(row):
    return numpy.mean(row)

# 这里调用numpy的mean函数来计算均值。最后我们就可以利用apply方法来计算各列的均值：
print(df[['今开', '最高', '最低', '最新价']].apply(mean_value))

# 上面的代码只是用于演示apply的使用方法，也可以使用以下代码实现相同的功能：
print(df[['今开', '最高', '最低', '最新价']].mean())

"""
如果想在应用函数中访问到具体元素，需要用索引来访问。
我们的示例中，各行的索引是从0到9，假设我们想计算第0、2、5行的均值，可以这样改造代码：
"""
def mean_value_v2(row):
    return (row[0] + row[2] + row[5]) / 3

print(df[['今开', '最高', '最低', '最新价']].apply(mean_value_v2))

"""
按行应用可以实现DataFrame单列或者多列之间的运算。
我们以计算振幅=(high-low)/preclose为例：
"""
def amp(col):
    return (col['最高'] - col['最低']) / col['最新价']

print(df.apply(amp, axis=1))

# 也可以通过以下两种lambda表达式来实现均可：
print(df.apply(lambda x: (x.最高 - x.最低) / x.最新价, axis=1))
print(df.apply(lambda x: (x['最高'] - x['最低']) / x['最新价'], axis=1))

# todo ====================================================================  shift  ==================================================================
df2 = ak.stock_zh_a_hist(symbol='000609', period="daily", start_date="20220801", end_date="20220808", adjust="hfq")

"""
Series的shift方法返回的结果仍为Series，参数表示移动的步数，默认值为1，
上面的例子中，数据向下移动了1步，我们也可以传入负数，控制数据向上移动：
"""
df2['最低_1d'] = df2['最低'].shift(1)
print(df2)

"""
DataFrame的shift方法与Series的shift方法应用方式类似，
只是需要用参数axis来控制数据移动的方向，axis默认值为0，表示按列应用，axis为1时，表示按行应用。
"""
# 按列应用 所有列均向下移动1步
print(df2.shift(1, axis=0))

# 假如我们想将股票前1日的最高价和收盘价都添加到对应的行中，可以按如下方式实现：
"""
shift_i函数实现了对df中的factor_list的所有因子的shift操作，并且修改新列的列名，例如将high向下移动1步的得到数据的列名修改为high_1a，
这样对2021-01-05这一行来看，high_1a列的值就是它前1交易日的最高价。
"""
def shift_i(df, factor_list, i):
    shift_df = df[factor_list].shift(i)
    shift_df.rename(columns={x: '{}_{}d'.format(x, i) for x in factor_list}, inplace=True)
    df = pd.concat([df, shift_df], axis=1)
    return df

df2 = shift_i(df2, ['最高', '收盘'], 1)
print(df2)

# 按行应用所有行均向右移动1步。目前我们在处理股票数据时，还没涉及到按行应用shift
print(df2.shift(1, axis=1))

# todo ====================================================================  Pandas内置的聚合方法  ==================================================================
# count统计的是非空值的个数，如果我们将某个元素置为空值，统计结果也会发生变化。
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

start_date = '20220101'
end_date = '20221010'
daterange = pd.date_range(start_date, end_date)
# print(daterange)
for single_date in daterange:
    print(single_date.strftime("%Y%m%d"))