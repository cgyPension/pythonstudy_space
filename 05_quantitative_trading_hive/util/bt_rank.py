import os
import sys
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import backtrader as bt
from datetime import date, datetime
import datetime
import time
import pandas as pd
import akshare as ak
import matplotlib.pyplot as plt  # 由于 Backtrader 的问题，此处要求 pip install matplotlib==3.2.2
# 在linux会识别不了包 所以要加临时搜索目录
from util import btUtils
from util.CommonUtils import get_spark
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
# matplotlib中文显示设置
plt.rcParams['font.sans-serif'] = ['FangSong']  # 中文仿宋
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

class MyCustomdata(bt.feeds.PandasData):
    '''增加字段 本身必须包含规则的7个字段 末尾,不能去掉
    '''
    lines = (
        'stock_strategy_ranking',
             )
    # -1表示自动按列明匹配数据
    params = (
        ('stock_strategy_ranking',-1),
              )

class stockStrategy(bt.Strategy):
    '''多因子选股策略 - 直接指标选股
       下一交易日开盘价买入 结束日期收盘价卖出
    '''
    def __init__(self, end_date,hold_day=2,hold_n=3):
        '''策略中各类指标的批量计算或是批量生成交易信号都可以写在这里'''
        '''__init__() 函数在回测过程中只会在最开始的时候调用一次，而 next() 会每个交易日依次循环调用多次；
            line 在 __init__() 中侧重于对整条 line 的操作，而在 next() 中侧重于站在当前回测时点，对单个数据点进行操作，所以对索引 [ ] 做了简化
           Backtrader 默认情况下是：在 t 日运行下单函数，然后在  t+1 日以开盘价成交；
        '''
        self.end_date = end_date
        self.hold_day=hold_day-1
        self.hold_n=hold_n
        self.holdlist={}

    def next(self):
        '''在这里根据交易信号进行买卖下单操作'''
        """
        主逻辑 
        """
        # print('next方法每天总资产：',self.datas[0].datetime.date(),self.broker.getvalue())
        hold_now = 0
        if hold_now >= self.hold_n:
            return
        # 回测如果是最后一天，则不进行买卖
        # if self.datas[0].datetime.date() == self.end_date:
        #     return

        for data in self.datas:
            # 6分1仓位
            # money = self.broker.get_cash() / self.hold_n*2 - hold_now
            money = self.broker.getvalue() / (self.hold_n*(self.hold_day+1))
            #没有持仓，则可以在下一交易日开盘价买 不要在股票数据最后周期进行买入
            if self.getposition(data).size == 0 and data.stock_strategy_ranking[0] <= self.hold_n and data.datetime.date()<=self.end_date-datetime.timedelta(self.hold_day+1):
                    # 如果可用现金小于成本则跳过这个循环
                    size = int(money / data.close[0] / 100) * 100
                    self.order = self.buy(data=data, size=size, exectype=bt.Order.Market)
                    # 6分1仓位
                    # self.order = self.order_target_percent(data=data, target=0.16, exectype=bt.Order.Market)
                    self.holdlist[data._name] = 1
                    hold_now = hold_now + 1
            # 如果有持仓 在下一交易日收盘价 平仓
            elif self.getposition(data).size > 0 and self.holdlist[data._name] == self.hold_day:
                self.order = self.sell(data=data, size=self.getposition(data).size,exectype=bt.Order.Close)
                # 卖出了后再重新计算该字典
                del self.holdlist[data._name]
            elif self.getposition(data).size > 0:
                self.holdlist[data._name] = self.holdlist[data._name] + 1

    # 可以不要，但如果你数据未对齐，需要在这里检验
    def prenext(self):
        print('prenext执行 数据没有对齐:', self.datetime.date(), self.getdatabyname(self.data._name), self.getdatabyname(self.data._name).close[0])

    def log(self, txt, dt=None, do_print=False):
        """构建策略打印日志的函数：可用于打印订单记录或交易记录等"""
        # 以第一个数据data0，即指数作为时间基准
        if do_print:
            dt = dt or self.datas[0].datetime.date()
            print('%s, %s' % (dt.isoformat(), txt))


    def notify_order(self, order):
        """通知订单信息"""
        order_status = ['Created', 'Submitted', 'Accepted', 'Partial','Completed', 'Canceled', 'Expired', 'Margin', 'Rejected']
        # 未被处理的订单 order 为 submitted/accepted
        if order.status in [order.Submitted, order.Accepted]:
            # self.log('未被处理的订单：ref:%.0f, name: %s, Order: %s' % (order.ref,
            #                                             order.data._name,
            #                                             order_status[order.status]))
            return

        # 已经处理的订单 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, ref:%s, Stock: %s, Price: %s, Cost: %s, Comm %s, Size: %s' %
                    (order.ref,  # 订单编号
                     order.data._name, # 股票名称
                     order.executed.price,  # 成交价
                     order.executed.value,  # 成交额
                     order.executed.comm,  # 佣金
                     order.executed.size))  # 成交量
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log('SELL EXECUTED, ref:%s, Stock: %s, Price: %s, Cost: %s, Comm %s, Size: %s' %
                         (order.ref,
                          order.data._name,
                          order.executed.price,
                          order.executed.value,
                          order.executed.comm,
                          order.executed.size))
            self.bar_executed = len(self)

            # # 订单未完成 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            self.log('交易失败：ref:%s, name: %s, status: %s' % (order.ref, order.data._name, order_status[order.status]),do_print=True)
        self.order = None

    def notify_trade(self, trade):
        """通知交易信息"""
        # if not trade.isclosed:
        #     return
        # self.log(f"策略收益：毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}")
        # 交易刚打开时
        if trade.justopened:
            self.log('Trade Opened, Stock: %s, Size: %.2f,Price: %.2f' % (
                trade.getdataname(), trade.size, trade.price))
        # 交易结束 平仓
        elif trade.isclosed:
            self.log('Trade Closed, Stock: %s, GROSS %.2f, NET %.2f, Comm %.2f' % (
                trade.getdataname(), trade.pnl, trade.pnlcomm, trade.commission))
        # 更新交易状态
        else:
            self.log('Trade Updated, Stock: %s, Size: %.2f,Price: %.2f' % (
                trade.getdataname(), trade.size, trade.price))

    # def stop(self):
    #     """回测结束后输出结果"""
    #     self.log("期末总资金 %s" % (self.broker.getvalue()), do_print=True)


def hc(pd_df,stockStrategy,start_date,end_date,end_date_n,strategy_name='xxx',start_cash=100000, stake=800, commission_fee=0.001,perc=0.0001,benchmark_code='sh000300'):
    # 添加业绩基准时，需要事先将业绩基准的数据添加给 cerebro 沪深300 指数字段是 date open close high low volume
    benchmark_df = ak.stock_zh_index_daily(symbol=benchmark_code)
    benchmark_df = benchmark_df[(benchmark_df['date'] >= start_date) & (benchmark_df['date'] <= end_date_n)]
    benchmark_df = benchmark_df.set_index(pd.to_datetime(benchmark_df['date'])).sort_index()
    # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充 要加上pd_df没有的字段
    benchmark_df = benchmark_df[['open', 'close', 'high', 'low', 'volume']]
    benchmark_df['stock_strategy_ranking'] = 9999

    cerebro = bt.Cerebro()
    # banchdata = MyCustomdata(dataname=benchmark_df)
    # cerebro.adddata(banchdata, name='沪深300',fromdate=pd.to_datetime(start_date), todate=pd.to_datetime(end_date))
    # cerebro.addobserver(bt.observers.Benchmark, data=banchdata)

    # 按股票代码，依次循环传入数据
    for stock in pd_df['stock_code'].unique():
        df = pd_df.query(f"stock_code=='{stock}'")[['open', 'high', 'low', 'close', 'volume','stock_strategy_ranking']]
        # 缺失值处理 每个股票与指数匹配日期
        df = df.reindex_like(benchmark_df)
        # 排序字段要特别填充
        df.loc[:, ['stock_strategy_ranking']] = df.loc[:, ['stock_strategy_ranking']].fillna(9999)
        # 用后面下一日非缺失的填充 pad为前一日
        df.fillna(method='bfill', inplace=True)
        df.fillna(0, inplace=True)
        # 通过 name 实现数据集与股票的一一对应
        # 增加字段 规范化数据格式
        # datafeed = MyCustomdata(dataname=df,
        #                            fromdate=pd.to_datetime(start_date),
        #                            todate=pd.to_datetime(end_date),
        #                            timeframe=bt.TimeFrame.Days)  # 将数据的时间周期设置为日
        datafeed = MyCustomdata(dataname=df,
                                fromdate=pd.to_datetime(start_date),
                                todate=pd.to_datetime(end_date)
                                )
        # 将数据加载至回测系统 通过 name 实现数据集与股票的一一对应
        cerebro.adddata(datafeed, name=stock)
        # print(f"{stock} Done !")

    # 设置初始资金
    cerebro.broker.setcash(start_cash)
    # 防止下单时现金不够被拒绝 只在执行时检查现金够不够
    cerebro.broker.set_checksubmit(False)
    # 设计手续费 交易佣金，双边各
    cerebro.broker.setcommission(commission=commission_fee)
    # 滑点：双边各 0.0001 使交易更真实 x * (1+ n%) 由于网络等实际成交价格会有编差
    # cerebro.broker.set_slippage_perc(perc=perc)
    # 实例化 自定义股票交易费用
    mycomm = btUtils.StockCommission(stamp_duty=0.001, commission=0.001)
    # 添加进 broker
    cerebro.broker.addcommissioninfo(mycomm)

    # 添加策略至回测系统！
    cerebro.addstrategy(stockStrategy,end_date=end_date)
    # 添加分析指标
    btUtils.add_ananlsis_indictor(cerebro)
    # 参数优化器 只有指标哪些在bt计算才好用，比如比较同一个策略 不同的均线 最终的收益率
    # cerebro.optstrategy(TestStrategy, period1=range(5, 25, 5), period2=range(10, 41, 10))

    # print("期初总资金: %s" % cerebro.broker.getvalue())
    results = cerebro.run(tradehistory=True) # 启动回测
    print('{} 回测系统 运行完毕!!!'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    end_cash = round(cerebro.broker.getvalue(),2)
    # print("期末总资金: %s" % cerebro.broker.getvalue())
    start = results[0]
    # 得到分析指标数据
    zx_df,cc_df,analyzer_df,tl_df = btUtils.get_analysis_indictor(start,benchmark_df[benchmark_df.index <= pd.to_datetime(end_date)])
    print('{} 分析器 运行完毕!!!'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    # 可视化回测结果
    print('{} 开始可视化!!!'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    btUtils.run_cerebro_dash(zx_df,cc_df,analyzer_df,tl_df,strategy_name,start_date,end_date,start_cash,end_cash)

# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/dim/bt_rank.py
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dim/bt_rank.py
if __name__ == '__main__':
    start_date = '20221101'
    end_date = '20221118'
    start_time = time.time()
    hc(stockStrategy,start_date,end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))




