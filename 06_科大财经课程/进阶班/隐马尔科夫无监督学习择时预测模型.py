from hmmlearn.hmm import GMMHMM, GaussianHMM
import datetime
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import cm
from matplotlib import pyplot
import matplotlib.pyplot as plt
import time
import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
#matplotlib中文显示设置
plt.rcParams['font.sans-serif']=['FangSong']   #中文仿宋
plt.rcParams['font.sans-serif']=['SimHei']     #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False       #用来正常显示负号

'''
隐马尔可夫模型**（Hidden Markov Model，HMM）从可观察的参数中确定该过程的隐含参数状态

金融领域
可观测到的序列股价，成交量，资金净额等等。每一个可观测值的产生对应着市场状态序列（Z1,...,Zn）
通过HMM模型，可以用简单的输入，来得出对目前市场状态的判断，从而帮助我们进行择时选择。因为市场状态不是显性可观测的，属于隐藏状态，我们通过对可观测变量的处理来进行推测。

基于隐马尔科夫金融量化模型**
模型的设定如下：
1. 隐藏状态数目：5
2. 输入变量：当日对数收益率，五日对数收益率，当日对数高低价差（其他备选因素成交量、成交额等大家可以自行尝试）
3. 混合高斯分布成分数目：1（为了简便，假定对数收益率服从单一高斯分布）
'''
def work():
    '''这个模型每次运行的 上涨下跌对应状态编号都不一样'''
    # 全A
    df = pd.read_pickle('HMM.pkl')
    # print(df.head(10))

    # 训练集
    train = df.loc[:'2022-05-30']
    # 数据合成
    open_ = train['open']  # 2012-06-08
    close_ = train['close']  # 2012-06-08
    high_ = train['high']
    low_ = train['low']
    volume_ = train['volume']
    money_ = train['turnover']
    datelist = pd.to_datetime(close_.index[5:])  # 2012-06-08 #2012-06-15
    logreturn = (np.log(np.array(close_[1:])) - np.log(np.array(close_[:-1])))[4:]  # 2012-06-11-2012-06-11
    logreturn5 = np.log(np.array(close_[5:])) - np.log(np.array(close_[:-5]))
    diffreturn = (np.log(np.array(high_)) - np.log(np.array(low_)))[5:]
    closeidx = close_[5:]
    X = np.column_stack([logreturn, diffreturn, logreturn5])  # 观测变量

    # n_components : 隐藏状态数目 covariance_type: 协方差矩阵的类型 n_iter : 最大迭代次数 random_state : 随机数种子
    hmm = GaussianHMM(n_components=3, covariance_type='diag', n_iter=10000, random_state=11).fit(X)
    latent_states_sequence = hmm.predict(X)
    # 上涨 下跌 横盘
    print('状态型号:', latent_states_sequence)  # 状态信号
    print('\n初始概率：', hmm.startprob_)
    print('\n转移矩阵：', hmm.transmat_)

    # 画图
    sns.set_style('white')
    plt.figure(figsize=(15, 8))
    for i in range(hmm.n_components):
        state = (latent_states_sequence == i)
        print(i, len(state), len(datelist[state]), len(closeidx[state]))
        plt.plot(datelist[state],
                 closeidx[state],
                 '.',
                 label='latent state %d' % i,
                 lw=1
                 )
        plt.legend()
        plt.grid(1)
    plt.show()
    # 我们看到了3个状态的HMM模型输出的市场状态序列。需要注意的是：HMM模型只是能分离出不同的状态，具体对每个状态赋予现实的市场意义，是需要人为来辨别和观察的。
    # 下面我们来用简单的timming策略来识别3种latent_state所带来的效果。

    data = pd.DataFrame({
        'datelist': datelist,
        'logreturn': logreturn,
        'state': latent_states_sequence
    }).set_index('datelist')

    plt.figure(figsize=(15, 8))
    for i in range(hmm.n_components):
        state = (latent_states_sequence == i)
        # 加0是为了实现第二天买入
        idx = np.append(0, state[:-1])
        # 增加三列状态列
        data['state %d_return' % i] = data.logreturn.multiply(idx, axis=0)
        # 对数化收益这里可以做累加
        plt.plot(np.exp(data['state %d_return' % i].cumsum()),
                 label='latent_state %d' % i)
        plt.legend()
        plt.grid(1)
    plt.show()

    print(data.tail())
    print(data.state.value_counts())
    # 先取行 再取列
    # data[data.state == 1].iloc[:, 2:-1]

    buy = (latent_states_sequence == 2)
    buy = np.append(0, buy[:-1])
    data['backtest_return'] = data.logreturn.multiply(buy, axis=0)
    results = pd.DataFrame((data['backtest_return'] + 1).cumprod())
    # benchmark
    results['close'] = df['close'] / df['close'][0]
    results.plot(figsize=(15, 8))

    # 验证集
    test = df.loc['2022-05-24':, ]
    # 数据合成
    open_ = test['open']
    close_ = test['close']
    high_ = test['high']
    low_ = test['low']
    volume_ = test['volume']
    money_ = test['turnover']
    datelist = pd.to_datetime(close_.index[5:])
    logreturn = (np.log(np.array(close_[1:])) - np.log(np.array(close_[:-1])))[4:]  # 过去一天收益率
    logreturn5 = np.log(np.array(close_[5:])) - np.log(np.array(close_[:-5]))  # 过去五天收益率
    diffreturn = (np.log(np.array(high_)) - np.log(np.array(low_)))[5:]
    closeidx = close_[5:]
    X_test = np.column_stack([logreturn, diffreturn, logreturn5])
    # 训练模型
    latent_states_sequence_test = hmm.predict(X_test)
    print(latent_states_sequence_test,len(latent_states_sequence_test))

    data = pd.DataFrame({
        'datelist': datelist,
        'logreturn': logreturn,
        'state': latent_states_sequence_test
    }).set_index('datelist')
    plt.figure(figsize=(15, 8))
    for i in range(hmm.n_components):
        state = (latent_states_sequence_test == i)
        idx = np.append(0, state[:-1])
        data['state %d_return' % i] = data.logreturn.multiply(idx, axis=0)
        plt.plot(np.exp(data['state %d_return' % i].cumsum()),
                 label='latent_state %d' % i)
        plt.legend()
        plt.grid(1)
    plt.show()

    buy = (latent_states_sequence_test == 2)
    buy = np.append(0, buy[:-1])
    data['backtest_return'] = data.logreturn.multiply(buy, axis=0)
    results = pd.DataFrame((data['backtest_return'] + 1).cumprod())
    # benchmark
    results['close'] = df['close']
    results['close'] = results['close'] / results['close'][0]
    results.plot(figsize=(15, 8))
    plt.show()

if __name__ == '__main__':
    work()