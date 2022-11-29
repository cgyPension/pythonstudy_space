
import backtrader as bt
import akshare as ak
import pandas as pd
import backtrader
from backtrader import indicators as btind
from datetime import datetime
import math
import stock_data
import trader_func
#matplotlib安装3.2.2版本
import matplotlib.pyplot as plt
import analysis
from backtrader import analyzers
'''
数据包括 date,close,low,open,high,volume,自定义数据，0一直指向最新数据
'''
trader_func_name=None
#一个自定义参数的例子
class user_def_data(backtrader.feeds.PandasData):
    #添加一条线，名称
    lines = ('index_close',)  # 要添加的列名
    # 设置 line 在数据源上新增的位置
    params = (
        ('index_close', -1),  # turnover对应传入数据的列名，这个-1会自动匹配backtrader的数据类与原有pandas文件的列名
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
        #self.money=self.datas[0].money
        self.macd=bt.indicators.MACD(self.dataclose)
        self.trader_sigin=btind.CrossOver(self.macd.macd,self.macd.signal)
        self.volume=self.datas[0].volume
        self.macd_line=self.macd.macd
        self.sigin=self.macd.signal
        self.trader_data=pd.DataFrame()
        self.trader_log=None
        '''
        self.sma_5 = bt.indicators.MovingAverageSimple(self.datas[0].close, period=5)
        self.sma_10 = bt.indicators.MovingAverageSimple(self.datas[0].close, period=10)
        #添加技术按指标
        self.exp=bt.indicators.ExponentialMovingAverage(self.datas[0].close,period=1)
        self.wma=bt.indicators.WeightedMovingAverage(self.datas[0],period=1,subplot=True)
        self.macdh=bt.indicators.MACDHisto(self.datas[0])
        self.rsi=bt.indicators.RSI(self.datas[0])
        self.rsi_value=self.rsi.rsi
        '''
    def notify_order(self, order):
        if order.status in [order.Submitted,order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                pass
                '''
                self.log('买入的价格 %.2f 买入的成本 %.2f 交易的手续费 %.2f' %
                         (
                             order.executed.price,
                             order.executed.value,
                             order.executed.comm
                         ))
                '''
            else:
                pass
                '''
                self.log('卖出的价格 %.2f 卖出的成本 %.2f 交易的手续费 %.2f' %
                         (
                             order.executed.price,
                             order.executed.value,
                             order.executed.comm
                         ))
                '''
        elif order.status in [order.Canceled,order.Margin,order.Rejected]:
            self.log('交易取消/交易保证金不足/交易拒绝')
        self.order=None
        #交易成功后通知
    def next(self):
       func=eval('trader_func.trader_func_{}(self)'.format(trader_func_name))
    def notify_trader(self, trader):
        # 如果交易没有完成
        if not trader.iscloaes:
            return
        else:
            self.log('交易利润，交易的净利润 %.2f %.2f' % (trader.pnl, tarder.pnlcomm))
#程序人口
def run_data(code='sh600111',start_date='20210101',end_date='20221122',start_cash=10000000,comm=0.03,mount=100):
        #建立大脑
        cerebro=bt.Cerebro()
        #添加回测策略
        cerebro.addstrategy(strategy=TestStrategy)
        #添加数据
        df=ak.stock_zh_a_daily(symbol=code,start_date=start_date,end_date=end_date)
        #资金流入数据101
        '''
        主力净流入-净额  主力净流入-净占比  超大单净流入-净额  超大单净流入-净占比  大单净流入-净额  
        大单净流入-净占比  中单净流入-净额  中单净流入-净占比  小单净流入-净额  小单净流入-净占比
        '''
        #合并数据
        #df['money']=df1['主力净流入-净占比'][-len(df['close'].tolist()):].astype(float).tolist()
        #转化日期
        df['date']=pd.to_datetime(df['date'])
        #设置时间索引
        df.index=df['date']
        df_index=stock_data.get_index_daily(start_date=start_date,end_date=end_date)
        df1=df[:min(len(df['close'].tolist()),len(df_index['close'].tolist()))]
        df1['index_close'] = df_index['close'].tolist()
        #交易手数
        cerebro.addsizer(bt.sizers.FixedSize,stake=mount)
        #输入数据
        #dataname数据，fromdate开始时间，todate结束时间，没有就是默认全部数据
        #需要自己使用自定义的数据类
        data=user_def_data(dataname=df1)#fromdate=datetime(2022,1,1),todate=datetime(2022,11,17))
        #加入数据
        cerebro.adddata(data=data)
        #设置开始资金
        cerebro.broker.set_cash(start_cash)
        #设置交易费用
        cerebro.broker.setcommission(commission=0.0)
        print('开始账户价值 %.2f' % cerebro.broker.getvalue())
        #必须放在运行前
        cerebro.addwriter(bt.WriterFile, csv=True, out=r'分析数据\交易数据.csv')
        '''
        out输出的文件路径
        close_out关闭输出
        csv文件格式
        '''
        #添加分析
        def add_ananlsis_indictor(cerebro=cerebro):
            cerebro.addanalyzer(analyzers.PyFolio, _name='pyfolio')
            # 年华收益
            cerebro.addanalyzer(analyzers.AnnualReturn, _name='nhsy')
            # 卡尔马
            cerebro.addanalyzer(analyzers.Calmar, _name='kem')
            # 回撤
            cerebro.addanalyzer(analyzers.DrawDown, _name='hc')
            '''
            drawdown- 回撤值为 0.xx %
            moneydown- 以货币单位计算的回撤值
            len- 回撤长度
            max.drawdown- 最大回撤值为 0.xx %
            max.moneydown- 以货币单位表示的最大回撤值
            max.len- 最大回撤长度
            '''
            # 时间回撤
            cerebro.addanalyzer(analyzers.TimeDrawDown, _name='time_hc')
            '''
            drawdown- 回撤值为 0.xx %
            maxdrawdown- 以货币单位计算的回撤值
            maxdrawdownperiod- 回撤长度
            '''
            # 总杠杆
            cerebro.addanalyzer(analyzers.GrossLeverage, _name='zgg')
            # 仓位价值
            cerebro.addanalyzer(analyzers.PositionsValue, _name='position_value')
            # 日志返回滚动
            cerebro.addanalyzer(analyzers.LogReturnsRolling, _name='rzgd')
            # 周期统计
            cerebro.addanalyzer(analyzers.PeriodStats, _name='zqtj')
            '''
            get_analysis返回包含键的字典：
            average
            stddev
            positive
            negative
            nochange
            best
            worst
            '''
            # 回报
            cerebro.addanalyzer(analyzers.Returns, _name='hb')
            '''
            rtot： 总复合回报
            ravg：整个时期的平均回报（特定时间范围）
            rnorm： 年化/标准化回报
            rnorm100：年化/标准化回报率以100%表示
            '''
            # 夏普比率
            cerebro.addanalyzer(analyzers.SharpeRatio, _name='xpbl')
            # 分类中队
            cerebro.addanalyzer(analyzers.SQN, _name='SQN')
            '''
            1.6 - 1.9 低于平均水平
            2.0 - 2.4 平均
            2.5 - 2.9 好
            3.0 - 5.0 超赞
            5.1 - 6.9 超赞
            7.0 - 圣杯
            '''
            # 时间返回
            cerebro.addanalyzer(analyzers.TimeReturn, _name='simple_return')
            # 交易分析器
            cerebro.addanalyzer(analyzers.TradeAnalyzer, _name='trader_analyzer')
            # 交易活动
            cerebro.addanalyzer(analyzers.Transactions, _name='trader_action')
            # 可变性加权回报：更好的夏普比率与对数回报
            cerebro.addanalyzer(analyzers.VWR, _name='VWR')
        add_ananlsis_indictor()
        #运行
        results=cerebro.run()
        start = results[0]
        def get_analysis_indictor(start=start):
            pyfoliozer = start.analyzers.getbyname('pyfolio')
            returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
            #print('年回报', start.analyzers.nhsy.get_analysis())
            analysis.analysis_analysis_data(text=start.analyzers.nhsy.get_analysis(),to_excel='分析数据\年回报.xlsx')
            #print('卡尔马', start.analyzers.kem.get_analysis())
            #analysis.analysis_analysis_data(text=start.analyzers.kem.get_analysis(), to_excel='分析数据\卡尔马.xlsx')
            #print('回撤', start.analyzers.hc.get_analysis())
            #analysis.analysis_analysis_data(text=start.analyzers.hc.get_analysis(), to_excel='分析数据\回撤.xlsx')
            #print('时间回撤', start.analyzers.time_hc.get_analysis())
            analysis.analysis_analysis_data(text=start.analyzers.time_hc.get_analysis(), to_excel='分析数据\时间回撤.xlsx')
            #print('总杠杆', start.analyzers.zgg.get_analysis())
            analysis.analysis_analysis_data(text=start.analyzers.zgg.get_analysis(), to_excel='分析数据\总杠杆.xlsx')
            #print('仓位价值', start.analyzers.position_value.get_analysis())
            analysis.analysis_analysis_data(text=start.analyzers.position_value.get_analysis(), to_excel='分析数据\仓位价值.xlsx')
            #print('日志返回滚动', start.analyzers.rzgd.get_analysis())
            ''''
            analysis.analysis_analysis_data(text=start.analyzers.rzgd.get_analysis(),to_excel='分析数据\日志返回滚动.xlsx')
            #print('周期统计', start.analyzers.zqtj.get_analysis())
            analysis.analysis_analysis_data(text=start.analyzers.zqtj.get_analysis(), to_excel='分析数据\周期统计.xlsx')
            #print('回报', start.analyzers.hb.get_analysis())
            #analysis.analysis_analysis_data(text=start.analyzers.hb.get_analysis(), to_excel='分析数据\回报.xlsx')
            #print('夏普比率', start.analyzers.xpbl.get_analysis())
            analysis.analysis_analysis_data(text=start.analyzers.xpbl.get_analysis(), to_excel='分析数据\夏普比率.xlsx')
            #print('期间评级', start.analyzers.SQN.get_analysis())
            #analysis.analysis_analysis_data(text=start.analyzers.SQN.get_analysis(), to_excel='分析数据\期间评级.xlsx')
            #print('简单回报', start.analyzers.simple_return.get_analysis())
            #analysis.analysis_analysis_data(text=start.analyzers.simple_return.get_analysis(), to_excel='分析数据\简单回报.xlsx')
            #print('交易分析', start.analyzers.trader_analyzer.get_analysis())
            analysis.analysis_analysis_data(text=start.analyzers.trader_analyzer.get_analysis(),to_excel='分析数据\交易分析.xlsx')
            #print('交易活动', start.analyzers.trader_action.get_analysis())
            analysis.analysis_analysis_data(text=start.analyzers.trader_action.get_analysis(),to_excel='分析数据\交易活动.xlsx')
            #print('可变性加权回报：更好的夏普比率与对数回报', start.analyzers.VWR.get_analysis())
            analysis.analysis_analysis_data(text=start.analyzers.VWR.get_analysis(),to_excel='分析数据\可变性加权回报更好的夏普比率与对数回报.xlsx')
            '''
        get_analysis_indictor()
        print('最后账户价值 %.2f' %cerebro.broker.getvalue())
        # 处理数据
        analysis.del_trader_data(idx=len(df['open'].tolist()), amount=10000000000000000000000)
        fig=cerebro.plot(style='candle')
        show = fig[0][0]
        show.savefig(r"结果.jpg", width=16, height=9, dpi=300)