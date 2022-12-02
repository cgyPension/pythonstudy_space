import os
import sys
import backtrader as bt
from datetime import date, datetime
import time
import pandas as pd
import akshare as ak
import matplotlib.pyplot as plt  # 由于 Backtrader 的问题，此处要求 pip install matplotlib==3.2.2
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
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
        'change_percent',
        'turnover_rate',
        'stock_strategy_ranking',
             )
    # -1表示自动按列明匹配数据
    params = (
        ('change_percent',-1),
        ('turnover_rate',-1),
        ('stock_strategy_ranking',-1),
              )


class StockCommission(bt.CommInfoBase):
    '''自定义股票交易费用'''
    params = (
        ('stocklike', True),  # 指定为股票模式
        ('commtype', bt.CommInfoBase.COMM_PERC),  # 使用百分比费用模式
        ('percabs', True),  # commission 不以 % 为单位
        ('stamp_duty', 0.001),)  # 印花税默认为 0.1%
    # 自定义费用计算公式
    def _getcommission(self, size, price, pseudoexec):
        if size > 0:  # 买入时，只考虑佣金
            return abs(size) * price * self.p.commission
        elif size < 0:  # 卖出时，同时考虑佣金和印花税
            return abs(size) * price * (self.p.commission + self.p.stamp_duty)
        else:
            return 0

class stockStrategy(bt.Strategy):
    '''多因子选股策略 - 直接指标选股'''
    # 全局设定交易策略的参数
    params = (("ranking", 5),)

    def __init__(self):
        '''策略中各类指标的批量计算或是批量生成交易信号都可以写在这里'''
        '''__init__() 函数在回测过程中只会在最开始的时候调用一次，而 next() 会每个交易日依次循环调用多次；
            line 在 __init__() 中侧重于对整条 line 的操作，而在 next() 中侧重于站在当前回测时点，对单个数据点进行操作，所以对索引 [ ] 做了简化
           Backtrader 默认情况下是：在 t 日运行下单函数，然后在  t+1 日以开盘价成交；
        '''
        # 初始化交易指令、买卖价格和手续费
        self.order = None
        self.buy_lst = []
        self.buy_price = None
        self.buy_comm = None
        # 按目标百分比下单
        # self.order = self.order_target_percent(target=percent)
        # self.order = self.order_target_percent(target=0.33)
        # Backtrader 默认是 “当日收盘后下单，次日以开盘价成交”
        # 1. Order.Market  市价单，回测时条件达到将以下一个 bar 的开盘价执行的市价单 ；
        # 2. Order.Close  市价单，回测时条件达到将以下一个 bar 的收盘价执行的市价单；
        # self.order = self.buy(exectype=bt.Order.Market)
        # self.order = self.sell(exectype=bt.Order.Close)

        # 0号是指数，不进入选股池，从1号往后进入股票池
        self.stocks = self.datas[1:]
        # 循环计算每只股票的指标 排除第一个 沪深300指数
        # todo stock_strategy_ranking 是自己在sql里面计算好的排序字段
        self.buy_sig = {x: self.getdatabyname(x).stock_strategy_ranking <= self.p.ranking for x in self.getdatanames()[1:]}
        # print('self.buy_sig：',self.buy_sig.values(),type(self.buy_sig.values()))
        # for i, d in enumerate(self.stocks):
        #     self.buy_sig =



    def next(self):
        '''在这里根据交易信号进行买卖下单操作'''
        """
        主逻辑 
        """
        # 检查是否有指令等待执行
        if self.order:
            return
        # 回测最后一天不进行买卖
        if self.stocks[0].datetime.date(0) == end_date:
            return

        # print('self.datas[0]:', self.datas[0].datetime.date(0))

        for i, d in enumerate(self.stocks):
            print(self.buy_sig)
            # 没有持仓，则可以在下一交易日开盘价买 不要在股票数据最后前一天进行买入
            if not len(self.getposition(d)) and self.stocks.datetime.date(1) < end_date:
                if self.buy_sig == 1:  # 达到买入条件 买
                    # self.buy(data=d)
                    self.order = self.order_target_percent(target=0.33)
                    self.order = self.buy(data=d,exectype=bt.Order.Market)
            # 如果有持仓 在下一交易日收盘价 平仓
            elif len(self.getposition(d)) and self.stocks.datetime.date(1) < end_date:
                # self.close(data=d)
                self.close(exectype=bt.Order.Close)


    # 可以不要，但如果你数据未对齐，需要在这里检验
    def prenext(self):
        print('prenext执行 数据没有对齐:', self.datetime.date(), self.getdatabyname()._name, self.getdatabyname().close[0])
        pass

    def log(self, txt, dt=None, do_print=False):
        """
        可选，构建策略打印日志的函数：可用于打印订单记录或交易记录等
        """
        # 以第一个数据data0，即指数作为时间基准
        if self.params.printlog or do_print:
            dt = dt or self.datetime.date()
            print('%s, %s' % (dt.isoformat(), txt))


    def notify_order(self, order):
        """
        通知订单信息
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
        通知交易信息
        """
        if not trade.isclosed:
            return
        self.log(f"策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}")

    # def stop(self):
    #     """
    #     回测结束后输出结果
    #     """
    #     self.log("期末总资金 %.2f" % (self.broker.getvalue()), do_print=True)

def hc(stockStrategy,start_date,end_date,start_cash=100000, stake=800, commission_fee=0.001,perc=0.0001):
    appName = os.path.basename(__file__)
    # 本地模式
    spark = get_spark(appName)
    start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()

    cerebro = bt.Cerebro()
    # 添加业绩基准时，需要事先将业绩基准的数据添加给 cerebro 沪深300
    stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sh000300")
    stock_zh_index_daily_df = stock_zh_index_daily_df.set_index(pd.to_datetime(stock_zh_index_daily_df['date'])).sort_index()
    banchdata = bt.feeds.PandasData(dataname=stock_zh_index_daily_df, fromdate=start_date, todate=end_date)
    cerebro.adddata(banchdata, name='沪深300')
    cerebro.addobserver(bt.observers.Benchmark, data=banchdata)

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
        -- 剔除京股
        -- and substr(stock_code,1,2) != 'bj'
        -- 剔除涨停 涨幅<5
--         and change_percent <5
--         and turnover_rate between 1 and 30
--         and stock_label_names rlike '小市值'
),
tmp_ads_02 as (
               select *,
                      '小市值+换手率+市盈率TTM' as stock_strategy_name,
                      dense_rank()over(partition by td order by dr_tmv+dr_turnover_rate+dr_pe_ttm) as stock_strategy_ranking
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
       change_percent,
       turnover_rate,
       stock_strategy_ranking
from tmp_ads_02
-- where stock_strategy_ranking <=10
    """ % (start_date,end_date,end_date)

    # 读取数据
    spark_df = spark.sql(sql)
    pd_df = spark_df.toPandas()
    # 将trade_date设置成index
    pd_df = pd_df.set_index(pd.to_datetime(pd_df['trade_date'])).sort_index()


    # 按股票代码，依次循环传入数据
    for stock in pd_df['stock_code'].unique():
        df = pd_df.query(f"stock_code=='{stock}'")[['open', 'high', 'low', 'close', 'volume','change_percent','turnover_rate','stock_strategy_ranking']]
        # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充
        # data = pd.DataFrame(index=pd_df.index.unique())  # 获取回测区间内所有交易日
        # data_ = pd.merge(data, df, left_index=True, right_index=True, how='left')
        # data_.loc[:, ['volume', 'openinterest']] = data_.loc[:, ['volume', 'openinterest']].fillna(0)
        # data_.loc[:, ['open', 'high', 'low', 'close', 'EP', 'ROE']] = data_.loc[:,
        #                                                               ['open', 'high', 'low', 'close']].fillna(
        #     method='pad')
        # data_.loc[:, ['open', 'high', 'low', 'close', 'EP', 'ROE']] = data_.loc[:,
        #                                                               ['open', 'high', 'low', 'close']].fillna(
        #     0.0000001)

        # 通过 name 实现数据集与股票的一一对应
        # 增加字段 规范化数据格式
        datafeed = MyCustomdata(dataname=df,
                                   fromdate=start_date,
                                   todate=end_date,
                                   timeframe=bt.TimeFrame.Days)  # 将数据的时间周期设置为日
        # datafeed = MyCustomdata(dataname=df,
        #                            fromdate=start_date,
        #                            todate=end_date)
        # 将数据加载至回测系统
        # 通过 name 实现数据集与股票的一一对应
        cerebro.adddata(datafeed, name=stock)
        print(f"{stock} Done !")


    # 设置初始资金
    cerebro.broker.setcash(start_cash)
    # 防止下单时现金不够被拒绝 只在执行时检查现金够不够
    cerebro.broker.set_checksubmit(False)
    # 设计手续费 交易佣金，双边各
    cerebro.broker.setcommission(commission=commission_fee)
    # 滑点：双边各 0.0001 使交易更真实 x * (1+ n%) 由于网络等实际成交价格会有编差
    cerebro.broker.set_slippage_perc(perc=perc)
    # 实例化 自定义股票交易费用
    mycomm = StockCommission(stamp_duty=0.001, commission=0.001)
    # 添加进 broker
    cerebro.broker.addcommissioninfo(mycomm)
    # 设置单笔交易买入数量 每次固定交易stake股
    cerebro.addsizer(bt.sizers.FixedSize, stake=stake)
    # 固定最大成交量
    # cerebro.broker.set_filler(bt.broker.fillers.FixedSize(size=3000))

    # 添加策略至回测系统！
    cerebro.addstrategy(stockStrategy)

    # 添加分析指标
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn') # 返回收益率时序
    # cerebro.addanalyzer(bt.analyzers.Returns, _name='_Returns', tann=252) # 计算年化收益：日度收益
    # cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')  # 返回年初至年末的年度收益率 年化收益率
    # cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio')  # 夏普比率
    # 计算年化夏普比率：日度收益
    # cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio', timeframe=bt.TimeFrame.Days, annualize=True,riskfreerate=0)  # 计算夏普比率
    # cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='_SharpeRatio_A')
    # cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')  # 计算最大回撤相关指标

    # 参数优化器 只有指标哪些在bt计算才好用，比如比较同一个策略 不同的均线 最终的收益率
    # cerebro.optstrategy(TestStrategy, period1=range(5, 25, 5), period2=range(10, 41, 10))
    print("期初总资金: %.2f" % cerebro.broker.getvalue())
    # 启动回测
    result = cerebro.run()
    # result = cerebro.run(runonce=False) # 日期对齐 sql计算好导入指标的不能用这个 要自己在sql对齐 除非在bt计算指标
    print("期末总资金: %.2f" % cerebro.broker.getvalue())
    # 可视化回测结果
    cerebro.plot()
    # 得到收益率时序 通过name提取结果
    # ret = pd.Series(result[0].analyzers._TimeReturn.get_analysis())

    ######### 注意 #########
    # PyFolio 分析器返回的收益也是月度收益，但是绘制的各种收益分析图形会有问题，有些图绘制不出来

# spark-submit /opt/pythonstudy_space/code/05_quantitative_trading_hive/dim/bt_rank.py
# python /opt/pythonstudy_space/code/05_quantitative_trading_hive/dim/bt_rank.py
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




