import os
import sys
import backtrader as bt
from datetime import date, datetime
import time
import warnings
import pandas as pd
import matplotlib.pyplot as plt  # 由于 Backtrader 的问题，此处要求 pip install matplotlib==3.2.2
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import get_spark
warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
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
        'stock_code',
        'change_percent',
        'turnover_rate',
             )
    # -1表示自动按列明匹配数据
    params = (
        ('stock_code',-1),
        ('change_percent',-1),
        ('turnover_rate',-1),
              )

class stockStrategy(bt.Strategy):
    '''多因子选股策略 - 直接指标选股'''
    # 可选，设置回测的可变参数：如移动均线的周期
    params = (("maperiod", 20),
              ('printlog', False),)  # 全局设定交易策略的参数, maperiod 是 MA 均值的长度

    def __init__(self):
        '''策略中各类指标的批量计算或是批量生成交易信号都可以写在这里'''
        '''__init__() 函数在回测过程中只会在最开始的时候调用一次，而 next() 会每个交易日依次循环调用多次；
           Backtrader 默认情况下是：在 t 日运行下单函数，然后在  t+1 日以开盘价成交；
        '''
        # print(self.datas)
        # print(self.data0)
        # print(self.data1)
        # print(self.datas[0])
        # # print(self.getdatabyname('sz000516_国际医学'))

        self.data_close = self.datas[0].close  # 指定价格序列
        # 初始化交易指令、买卖价格和手续费
        self.order = None
        self.buy_price = None
        self.buy_comm = None
        # 添加移动均线指标
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod
        )


    def next(self):
        '''在这里根据交易信号进行买卖下单操作'''
        """
        主逻辑
        """
        # self.log(f'收盘价, {data_close[0]}')  # 记录收盘价
        if self.order:  # 检查是否有指令等待执行
            return
        # 检查是否持仓
        if not self.position:  # 没有持仓
            # 执行买入条件判断：收盘价格上涨突破15日均线
            if self.data_close[0] > self.sma[0]:
                self.log("BUY CREATE, %.2f" % self.data_close[0])
                # 执行买入
                self.order = self.buy()
        else:
            # 执行卖出条件判断：收盘价格跌破15日均线
            if self.data_close[0] < self.sma[0]:
                self.log("SELL CREATE, %.2f" % self.data_close[0])
                # 执行卖出
                self.order = self.sell()

    def log(self, txt, dt=None, do_print=False):
        """
        可选，构建策略打印日志的函数：可用于打印订单记录或交易记录等
        """
        # 以第一个数据data0，即指数作为时间基准
        if self.params.printlog or do_print:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        """
        记录交易执行情况
        """
        # 如果 order 为 submitted/accepted,返回空
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    f"买入:\n价格:{order.executed.price},\
                成本:{order.executed.value},\
                手续费:{order.executed.comm}"
                )
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(
                    f"卖出:\n价格：{order.executed.price},\
                成本: {order.executed.value},\
                手续费{order.executed.comm}"
                )
            self.bar_executed = len(self)

            # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("交易失败")
        self.order = None

    def notify_trade(self, trade):
        """
        记录交易收益情况
        """
        if not trade.isclosed:
            return
        self.log(f"策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}")

    def stop(self):
        """
        回测结束后输出结果
        """
        self.log("(MA均线： %2d日) 期末总资金 %.2f" % (self.params.maperiod, self.broker.getvalue()), do_print=True)

def hc(stockStrategy,sql,start_date,end_date,start_cash=1000000, stake=100, commission_fee=0.001,perc=0.0001):
    appName = os.path.basename(__file__)
    # 本地模式
    spark = get_spark(appName)
    start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()

    # 导入到bt 不能有str类型的字段
    sql = """
select trade_date,
      -- stock_code||'_'||stock_name as stock_code,
      cast(substr(stock_code,3) as int) as stock_code,
       stock_name,
       open_price as open,
       close_price as close,
       high_price as high,
       low_price as low,
       volume,
       change_percent,
       turnover_rate
from stock.ods_dc_stock_quotes_di
where td between '%s' and '%s'
    """ % (start_date,end_date)

    # 读取数据
    spark_df = spark.sql(sql)
    pd_df = spark_df.toPandas().set_index(['trade_date']).sort_index() # 将trade_date设置成index
    # pd_df = spark_df.toPandas().set_index(pd.to_datetime(['trade_date'])).sort_index() # 将trade_date设置成index
    # 无未平仓量列.(openinterest是期货交易使用的)
    # pd_df['openinterest'] = 0


    # cerebro = bt.Cerebro()
    # 禁止默认观察者 提高速度
    cerebro = bt.Cerebro(stdstats=False)
    # 按股票代码，依次循环传入数据
    # for stock in pd_df['stock_code'].unique():
    #     # 日期对齐
    #     # df = pd_df.query(f"stock_code=='{stock}'")[['open', 'high', 'low', 'close', 'volume', 'openinterest']]
    #     df = pd_df.query(f"stock_code=='{stock}'")
    #
    #     # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充
    #     # data = pd.DataFrame(index=pd_df.index.unique())  # 获取回测区间内所有交易日
    #     # data_ = pd.merge(data, df, left_index=True, right_index=True, how='left')
    #     # data_.loc[:, ['volume', 'openinterest']] = data_.loc[:, ['volume', 'openinterest']].fillna(0)
    #     # data_.loc[:, ['open', 'high', 'low', 'close', 'EP', 'ROE']] = data_.loc[:,
    #     #                                                               ['open', 'high', 'low', 'close']].fillna(
    #     #     method='pad')
    #     # data_.loc[:, ['open', 'high', 'low', 'close', 'EP', 'ROE']] = data_.loc[:,
    #     #                                                               ['open', 'high', 'low', 'close']].fillna(
    #     #     0.0000001)
    #
    #     # 规范化数据格式 及加上自定义字段
    #     # datafeed = MyCustomdata(dataname=data_,
    #     #                            fromdate=start_date,
    #     #                            todate=end_date,
    #     #                            timeframe=bt.TimeFrame.Weeks)  # 将数据的时间周期设置为月度
    #
    #     datafeed = MyCustomdata(dataname=df,
    #                                fromdate=start_date,
    #                                todate=end_date,
    #                                timeframe=bt.TimeFrame.Weeks)  # 将数据的时间周期设置为月度
    #
    #     cerebro.adddata(datafeed, name=stock)  # 通过 name 实现数据集与股票的一一对应
    #     print(f"{stock} Done !")


    # 可能要循环导入股票对应的 股票名字
    # 通过 name 实现数据集与股票的一一对应
    # 增加字段 规范化数据格式
    data = MyCustomdata(dataname=pd_df, fromdate=start_date, todate=end_date,timeframe=bt.TimeFrame.Weeks)
    # 将数据加载至回测系统
    cerebro.adddata(data,name=data.lines.stock_code)

    # 设置初始资金
    cerebro.broker.setcash(start_cash)
    # 设计手续费 交易佣金，双边各
    cerebro.broker.setcommission(commission=commission_fee)
    # 滑点：双边各 0.0001
    cerebro.broker.set_slippage_perc(perc=perc)
    # 添加策略至回测系统！
    cerebro.addstrategy(stockStrategy)
    # 添加策略分析指标
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn') # 返回收益率时序
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')  # 年化收益率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio')  # 夏普比率
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')  # 回撤
    # 设置单笔交易买入数量
    # cerebro.addsizer(bt.sizers.FixedSize, stake=stake)
    print("期初总资金: %.2f" % cerebro.broker.getvalue())
    # 启动回测
    result = cerebro.run()
    print("期末总资金: %.2f" % cerebro.broker.getvalue())
    # 可视化回测结果
    # cerebro.plot()
    # 得到收益率时序
    ret = pd.Series(result[0].analyzers._TimeReturn.get_analysis())

    ######### 注意 #########
    # PyFolio 分析器返回的收益也是月度收益，但是绘制的各种收益分析图形会有问题，有些图绘制不出来

# spark-submit /opt/code/05_quantitative_trading_hive/util/backtraderUtil.py
# nohup backtraderUtil.py >> my.log 2>&1 &
# python backtraderUtil.py
if __name__ == '__main__':
    # start_date = date.today().strftime('%Y%m%d')
    # end_date = start_date
    # if len(sys.argv) == 1:
    #     print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    # elif len(sys.argv) == 2:
    #     run_type = sys.argv[1]
    #     if run_type == 'all':
    #         start_date = '20210101'
    #         end_date
    #     else:
    #         start_date
    #         end_date
    # elif len(sys.argv) == 4:
    #     run_type = sys.argv[1]
    #     start_date = sys.argv[2]
    #     end_date = sys.argv[3]

    start_date = '20221101'
    end_date = '20221118'
    start_time = time.time()
    hc(stockStrategy,'',start_date,end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))




