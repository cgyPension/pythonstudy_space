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

        # 0号是指数，不进入选股池，从1号往后进入股票池
        # self.stocks = self.datas[1:]
        # 循环计算每只股票的指标 排除第一个 沪深300指数
        # todo stock_strategy_ranking 是自己在sql里面计算好的排序字段
        # self.buy_sig = {x: self.getdatabyname(x).stock_strategy_ranking <= self.p.ranking for x in self.getdatanames()[1:]}
        # print('self.buy_sig：',self.buy_sig.values(),type(self.buy_sig.values()))
        # for i, d in enumerate(self.stocks):
        #     self.buy_sig =

        # print('self.lines.getlinealiases()：',self.datas[0].lines.getlinealiases())

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
        if self.datas[0].datetime.date() == self.end_date:
            return

        # 基准不进行买卖 不传入基准了
        # for data in self.datas[1:]:
        for data in self.datas:
            # 6分1仓位
            # money = self.broker.get_cash() / self.hold_n*2 - hold_now
            money = self.broker.getvalue() / 6
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
            # elif self.getposition(data)>0:
                # self.close(data=d)
                # elf.getposition(data)>0 这里应该设置成当日？或者直接改成else卖出
                # self.close(data=data,exectype=bt.Order.Close)
                # 应该以当日收盘价卖出
                # self.order = self.sell()
                self.order = self.sell(data=data, size=self.getposition(data).size,exectype=bt.Order.Close)
                # 卖出了后再重新计算该字典
                del self.holdlist[data._name]
            elif self.getposition(data).size > 0:
                self.holdlist[data._name] = self.holdlist[data._name] + 1

    # 可以不要，但如果你数据未对齐，需要在这里检验
    def prenext(self):
        print('prenext执行 数据没有对齐:', self.datetime.date(), self.getdatabyname(self.data._name), self.getdatabyname(self.data._name).close[0])

    def log(self, txt, dt=None, do_print=False):
        """
        可选，构建策略打印日志的函数：可用于打印订单记录或交易记录等
        """
        # 以第一个数据data0，即指数作为时间基准
        if do_print:
            dt = dt or self.datas[0].datetime.date()
            print('%s, %s' % (dt.isoformat(), txt))


    def notify_order(self, order):
        """
        通知订单信息
        """
        order_status = ['Created', 'Submitted', 'Accepted', 'Partial',
                        'Completed', 'Canceled', 'Expired', 'Margin', 'Rejected']
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
        """
        通知交易信息
        """
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
    #     """
    #     回测结束后输出结果
    #     """
    #     self.log("期末总资金 %s" % (self.broker.getvalue()), do_print=True)


def hc(stockStrategy,start_date,end_date,start_cash=100000, stake=800, commission_fee=0.001,perc=0.0001,benchmark_code='sh000300'):
    appName = os.path.basename(__file__)
    # 本地模式
    spark = get_spark(appName)
    start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()

    # 导入到bt 不能有str类型的字段
    sql = """
with tmp_ads_01 as (
select *,
       dense_rank()over(partition by td order by total_market_value) as dr_tmv,
       dense_rank()over(partition by td order by turnover_rate) as dr_turnover_rate,
       dense_rank()over(partition by td order by pe_ttm) as dr_pe_ttm
from stock.dwd_stock_quotes_di
where td between '%s' and '%s'
        and stock_code in ('sh601988','sz300364','sz300659','sh603709','sz300981')
),
tmp_ads_02 as (
               select *,
                      '小市值+换手率+市盈率TTM' as stock_strategy_name,
                      dense_rank()over(partition by td order by dr_tmv+dr_turnover_rate+dr_pe_ttm,volume) as stock_strategy_ranking
               from tmp_ads_01
               where suspension_time is null
                       or estimated_resumption_time < date_add('%s',1)
--                        or pe_ttm is null
--                        or pe_ttm <=30
               order by stock_strategy_ranking
)
select trade_date,
       stock_code||'_'||stock_name as stock_code,
       open_price as open,
       close_price as close,
       high_price as high,
       low_price as low,
       volume,
       stock_strategy_ranking
from tmp_ads_02
-- where stock_strategy_ranking <=10
    """ % (start_date,end_date,end_date)

    # 读取数据
    spark_df = spark.sql(sql)
    pd_df = spark_df.toPandas()
    # 将trade_date设置成index
    pd_df = pd_df.set_index(pd.to_datetime(pd_df['trade_date'])).sort_index()


    # 添加业绩基准时，需要事先将业绩基准的数据添加给 cerebro 沪深300 指数字段是 date open close high low volume
    benchmark_df = ak.stock_zh_index_daily(symbol=benchmark_code)
    benchmark_df = benchmark_df[(benchmark_df['date'] >= start_date) & (benchmark_df['date'] <= end_date)]
    benchmark_df = benchmark_df.set_index(pd.to_datetime(benchmark_df['date'])).sort_index()
    # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充 要加上pd_df没有的字段
    benchmark_df = benchmark_df[['open', 'close', 'high', 'low', 'volume']]
    benchmark_df['stock_strategy_ranking'] = 9999

    cerebro = bt.Cerebro()
    # banchdata = MyCustomdata(dataname=benchmark_df)
    # cerebro.adddata(banchdata, name='沪深300')
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
        # 通过 name 实现数据集与股票的一一对应
        # 增加字段 规范化数据格式
        # datafeed = MyCustomdata(dataname=df,
        #                            fromdate=pd.to_datetime(start_date),
        #                            todate=pd.to_datetime(end_date),
        #                            timeframe=bt.TimeFrame.Days)  # 将数据的时间周期设置为日
        datafeed = MyCustomdata(dataname=df)
        # 将数据加载至回测系统
        # 通过 name 实现数据集与股票的一一对应
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

    print("期初总资金: %s" % cerebro.broker.getvalue())
    # 必须放在运行前
    # cerebro.addwriter(bt.WriterFile, csv=True, out=r'交易数据.csv')
    results = cerebro.run(tradehistory=True) # 启动回测
    end_cash = round(cerebro.broker.getvalue(),2)
    print("期末总资金: %s" % cerebro.broker.getvalue())
    start = results[0]
    # 得到分析指标数据
    analyzer_df,tl_df = btUtils.get_analysis_indictor(start,benchmark_df)
    btUtils.run_cerebro_dash(analyzer_df,tl_df,'小市值+市盈率TTM+换手率',start_date,end_date,start_cash,end_cash)
    # 可视化回测结果
    # cerebro.plot()

    ######### 注意 #########
    # PyFolio 分析器返回的收益也是月度收益，但是绘制的各种收益分析图形会有问题，有些图绘制不出来

# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/dim/bt_rank.py
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dim/bt_rank.py
# nohup bt_rank.py >> my.log 2>&1 &
# python bt_rank.py all
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
    hc(stockStrategy,start_date,end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))




