import backtrader as bt
import datetime
import pandas as pd


class PandasData_more(bt.feeds.PandasData):
    lines = ('ROE', 'EP',)  # 要添加的线
    # 设置 line 在数据源上的列位置
    params = dict(
        ROE=-1,  # 设置新增指标的位置，-1表示自动按列明匹配数据
        EP=-1
    )


class StockSelectStrategy(bt.Strategy):
    params = dict(
        selnum=30,  # 设置持仓股数在总的股票池中的占比，如买入表现最好的前30只股票
        rperiod=1,  # 计算收益率的周期
        vperiod=6,  # 计算波动率的周期，过去6个月的波动率
        mperiod=2,  # 计算动量的周期，如过去2个月的收益
        reserve=0.05  # 5% 为了避免出现资金不足的情况，每次调仓都预留 5% 的资金不用于交易
    )

    def log(self, arg):
        print('{} {}'.format(self.datetime.date(), arg))

    def __init__(self):
        # 计算持仓权重，等权
        self.perctarget = (1.0 - self.p.reserve) / self.p.selnum
        # 循环计算每只股票的收益波动率因子
        self.rs = {d: bt.ind.PctChange(d, period=self.p.rperiod) for d in self.datas}
        self.vs = {d: 1 / (bt.ind.StdDev(ret, period=self.p.vperiod) + 0.000001) for d, ret in self.rs.items()}
        # 循环计算每只股票的动量因子
        self.ms = {d: bt.ind.ROC(d, period=self.p.mperiod) for d in self.datas}
        # 将 ep 和 roe 因子进行匹配
        self.EP = {d: d.lines.EP for d in self.datas}
        self.ROE = {d: d.lines.ROE for d in self.datas}
        self.all_factors = [self.rs, self.vs, self.ms, self.EP, self.ROE]

    def next(self):
        # 在每个横截面上计算所有因子的综合排名
        stocks = list(self.datas)
        ranks = {d: 0 for d in stocks}
        # 计算每个因子的rank，并进行求和
        for factor in self.all_factors:
            stocks.sort(key=lambda x: factor[x][0], reverse=True)
            # print({x._name:factor[x][0] for x in stocks})
            ranks = {d: i + ranks[d] for d, i in zip(stocks, range(1, len(stocks) + 1))}
            # print({d._name:rank for d,rank in ranks.items()})

        # 对各因子rank求和后的综合值进行最后的排序,最大综合值排最前面
        # 买入 动量、ep、roe 高；波动率低的股票
        ranks = sorted(ranks.items(), key=lambda x: x[1], reverse=False)
        # print({i._name:rank for (i,rank) in ranks})

        # 选取前 self.p.selnum 只股票作为持仓股
        rtop = dict(ranks[:self.p.selnum])

        # 剩余股票将从持仓中剔除（如果在持仓里的话）
        rbot = dict(ranks[self.p.selnum:])

        # 提取有仓位的股票
        posdata = [d for d, pos in self.getpositions().items() if pos]

        # 删除不在继续持有的股票，进而释放资金用于买入新的股票
        for d in (d for d in posdata if d not in rtop):
            self.log('Leave {} - Rank {:.2f}'.format(d._name, rbot[d]))
            self.order_target_percent(d, target=0.0)

        # 对下一期继续持有的股票，进行仓位调整
        for d in (d for d in posdata if d in rtop):
            self.log('Rebal {} - Rank {:.2f}'.format(d._name, rtop[d]))
            self.order_target_percent(d, target=self.perctarget)
            del rtop[d]

        # 买入当前持仓中没有的股票
        for d in rtop:
            self.log('Enter {} - Rank {:.2f}'.format(d._name, rtop[d]))
            self.order_target_percent(d, target=self.perctarget)


# 实例化 cerebro
cerebro = bt.Cerebro()
# 读取行情数据
month_price = pd.read_csv("./data/month_price.csv", parse_dates=['datetime'])
month_price = month_price.set_index(['datetime']).sort_index()  # 将datetime设置成index
# 按股票代码，依次循环传入数据
for stock in month_price['sec_code'].unique():
    # 日期对齐
    data = pd.DataFrame(index=month_price.index.unique())  # 获取回测区间内所有交易日
    df = month_price.query(f"sec_code=='{stock}'")[['open', 'high', 'low', 'close', 'volume', 'openinterest']]
    data_ = pd.merge(data, df, left_index=True, right_index=True, how='left')
    # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充
    data_.loc[:, ['volume', 'openinterest']] = data_.loc[:, ['volume', 'openinterest']].fillna(0)
    data_.loc[:, ['open', 'high', 'low', 'close', 'EP', 'ROE']] = data_.loc[:, ['open', 'high', 'low', 'close']].fillna(
        method='pad')
    data_.loc[:, ['open', 'high', 'low', 'close', 'EP', 'ROE']] = data_.loc[:, ['open', 'high', 'low', 'close']].fillna(
        0.0000001)
    # 导入数据
    datafeed = PandasData_more(dataname=data_,
                               fromdate=datetime.datetime(2019, 1, 31),
                               todate=datetime.datetime(2021, 8, 31),
                               timeframe=bt.TimeFrame.Months)  # 将数据的时间周期设置为月度

    cerebro.adddata(datafeed, name=stock)  # 通过 name 实现数据集与股票的一一对应
    print(f"{stock} Done !")
# 初始资金 100,000,000
cerebro.broker.setcash(100000000.0)
# 佣金，双边各 0.0003
cerebro.broker.setcommission(commission=0.0003)
# 滑点：双边各 0.0001
cerebro.broker.set_slippage_perc(perc=0.0001)
# 将编写的策略添加给大脑，别忘了 ！
cerebro.addstrategy(StockSelectStrategy)
# 返回收益率时序
cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')
result = cerebro.run()
# 得到收益率时序
ret = pd.Series(result[0].analyzers._TimeReturn.get_analysis())

######### 注意 #########
# PyFolio 分析器返回的收益也是月度收益，但是绘制的各种收益分析图形会有问题，有些图绘制不出来