import backtrader as bt
import akshare as ak
import pandas as pd
import backtrader
from backtrader import indicators as btind
from datetime import datetime
import math
#matplotlib安装3.2.2版本
import matplotlib.pyplot as plt
'''
数据包括 date,close,low,open,high,volume,自定义数据，0一直指向最新数据
'''
class user_def_data(backtrader.feeds.PandasData):
    #添加一条线，名称
    lines = ('money',)  # 要添加的列名
    # 设置 line 在数据源上新增的位置
    params = (
        ('money', -1),  # turnover对应传入数据的列名，这个-1会自动匹配backtrader的数据类与原有pandas文件的列名
        # 如果是个大于等于0的数，比如8，那么backtrader会将原始数据下标8(第9列，下标从0开始)的列认为是turnover这一列
    )
#建立交易类
class TestStrategy(bt.Strategy):
    #记录函数 dt时间
    def log(self,txt,dt=None):
        #最近的时间
        dt=dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))
    #初始化数据
    def __init__(self):
        #收盘价
        self.dataclose=self.datas[0].close
        #开盘价
        self.dataopen=self.datas[0].open
        #最低价
        self.datalow=self.datas[0].low
        #最高价
        self.datahigh=self.datas[0].high
        #加入资金数据
        self.money=self.datas[0].money
        self.macd=bt.indicators.MACD(self.dataclose)
        self.volume=self.datas[0].volume
        self.sma_5 = bt.indicators.MovingAverageSimple(self.datas[0].close, period=5)
        self.sma_10 = bt.indicators.MovingAverageSimple(self.datas[0].close, period=10)
        #添加技术按指标
        self.exp=bt.indicators.ExponentialMovingAverage(self.datas[0].close,period=1)
        self.wma=bt.indicators.WeightedMovingAverage(self.datas[0],period=1,subplot=True)
        self.macdh=bt.indicators.MACDHisto(self.datas[0])
        self.rsi=bt.indicators.RSI(self.datas[0])
        self.rsi_value=self.rsi.rsi
    def notify_order(self, order):
        if order.status in [order.Submitted,order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('买入的价格 %.2f 买入的成本 %.2f 交易的手续费 %.2f' %
                         (
                             order.executed.price,
                             order.executed.value,
                             order.executed.comm
                         ))
            else:
                self.log('卖出的价格 %.2f 卖出的成本 %.2f 交易的手续费 %.2f' %
                         (
                             order.executed.price,
                             order.executed.value,
                             order.executed.comm
                         ))
        elif order.status in [order.Canceled,order.Margin,order.Rejected]:
            self.log('交易取消/交易保证金不足/交易拒绝')
        self.order=None
        #交易成功后通知
    def notify_trader(self,trader):
        #如果交易没有完成
        if not trader.iscloaes:
            return
        self.log('交易利润，交易的净利润 %.2f %.2f' %(trader.pnl,tarder.pnlcomm))
    def next(self):
        #检测是不是在市场
        if not self.position:
            #收盘价大于5日均线
            #如果主力资金流入占比大于5，买入
            if self.money[0]>5 or self.sma_5[0]>self.sma_10[0]:
                self.log('主力资金流入占比 %.2f,买入的价格 %.2f' % (self.money[0],self.dataclose[0]))
                #保持交易状态，避免二次交易,买入全部
                self.order=self.order_target_percent(target=1.0)
        else:
            #如果主力资金流出大于5
            if self.money[0]<-10 or self.sma_5[0]<self.sma_10[0]:
                self.log('主力资金流出占比 %.2f,卖出的价格 %.2f' % (self.money[0],self.dataclose[0]))
                #卖出全部
                self.order=self.order_target_percent(target=0.0)
#程序人口
if __name__=='__main__':
    #建立大脑
    cerebro=bt.Cerebro()
    #添加回测策略
    cerebro.addstrategy(strategy=TestStrategy)
    #添加数据
    df=ak.stock_zh_a_daily(symbol='sz002603')[-100:]
    #资金流入数据101
    '''
    主力净流入-净额  主力净流入-净占比  超大单净流入-净额  超大单净流入-净占比  大单净流入-净额  
    大单净流入-净占比  中单净流入-净额  中单净流入-净占比  小单净流入-净额  小单净流入-净占比
    '''
    df1=ak.stock_individual_fund_flow(stock='002603',market='sz')
    #合并数据
    df['money']=df1['主力净流入-净占比'][-100:].astype(float).tolist()
    #转化日期
    df['date']=pd.to_datetime(df['date'])
    #设置时间索引
    df.index=df['date']
    #输入数据
    #dataname数据，fromdate开始时间，todate结束时间，没有就是默认全部数据
    #需要自己使用自定义的数据类
    data=user_def_data(dataname=df)#fromdate=datetime(2022,1,1),todate=datetime(2022,11,17))
    #加入数据
    cerebro.adddata(data=data)
    #设置开始资金
    cerebro.broker.set_cash(1000000000.0)
    #设置交易费用
    cerebro.broker.setcommission(commission=0.0)
    print('开始账户价值 %.2f' % cerebro.broker.getvalue())
    #必须放在运行前
    cerebro.addwriter(bt.WriterFile, csv=True, out=r'交易数据.csv')
    '''
    out输出的文件路径
    close_out关闭输出
    csv文件格式
    '''
    #运行
    cerebro.run()
    print('最后账户价值 %.2f' %cerebro.broker.getvalue())
    cerebro.plot(style='candle')