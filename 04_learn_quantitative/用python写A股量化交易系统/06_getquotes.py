# -*- coding: utf-8 -*-
# 视频6：从网页获取报价
# http://qt.gtimg.cn/q=sh600519
# str = string

# tuple?不想轻易修改用元组 否则用list
# float, int, str->datetime?
# how to get out of while loop?

import requests
from time import sleep
from dateutil import parser
from datetime import datetime, time,timedelta

# 函数， function
# 参数， variable
# 类， class
    # variables -> attribute， 属性
    # function -> method， 方法
def getTick(symbol):# def function
    '''
    # go to gtime to get last tick info
    parameter: symbol, without suffix or prefix, such as 600519
    return: tick (tuple format) with datetime and last price
    '''
    symbol = str(symbol)
    symbol = 'sh' + symbol if symbol[0] in ['5', '6'] else 'sz' + symbol

    page = requests.get('http://qt.gtimg.cn/q=' + symbol)
    full_tick = page.text.split('~')
    symbol_name = full_tick[1]
    trade_datetime = parser.parse(full_tick[30])
    # trade_datetime2 = parser.parse(full_tick[30]).strftime("%Y-%m-%d %H:%M:%S")
    open_tick = float(full_tick[5])
    high_tick = float(full_tick[33])
    low_tick = float(full_tick[34])
    close_tick = float(full_tick[3])
    tick = (close_tick,trade_datetime)

    return tick

# __init__, 构造，初始化，实例化
# self, this, that, ATraderX
class AstockTrading(object):
    def __init__(self, stock_code):
        self.stock_code = stock_code
        self.Dt = 0  # ast1.Dt
        self.Open = 0
        self.High = 0
        self.Low = 0
        self.Close = 0
        self.Volume = 0
    def get_history_data_from_local_machine(self):
        self.Open = [1, 2, 3]
        self.High = [2, 3, 4]

    # 面向过程
    # 面向对象

    # how save and import history data?
    def bar_generator(self, tick):
        # assume we have history data already,
        # 1、 update 5 minutes calculate 5 minutes ma20, not daily data
        # 2、 compare last and ma20 -> buy or sell or pass
        # assume we have history data, Open, High, Low, Close, Dt
        # Dt = [datetime(2020, 11, 27, 14, 55),
        #       datetime(2020, 11, 27, 14, 50),
        #       datetime(2020, 11, 27, 14, 45)]
        # Open = [45.79, 45.66, 45.72]
        # High = []
        # Low = []
        # Close = []
        # tick[0] insert into Dt at index 0
        # tick[1] insert into Open, High, Low, Close

        # 9:30
        # 9:31
        # 9:32
        # ...
        # 9:35:00
        # 9:35:03
        # 9:35:06

        # 5, 10, 15, 20, 30 minutes, 60 minutes?
        last_bar_start_minute = None

        if tick[0].minute % 5 == 0 and \
            tick[0].minute != last_bar_start_minute:
            # create a new bar
           last_bar_start_minute = tick[0].minute
           self.Open.insert(0, tick[1])
           self.High.insert(0, tick[1])
           self.Low.insert(0, tick[1])
           self.Close.insert(0, tick[1])
           self.Dt.insert(0, tick[0])
        else:
            # update current bar
            self.High[0] = max(self.High[0], tick[1])
            self.Low = min(self.Low[0], tick[1])
            self.Close[0] = tick[1]
            self.Dt[0] = tick[0]

    def buy(self):
        pass

    def sell(self):
        pass

    # def strategy(self):
    #     # last < 0.95 * ma20, long, last > ma20 * 1.05, sell
    #     # ma20 = Close[:19].sum()/20
    #     if new new is created:
    #         sum_ = 0
    #         for item in self.Close[1:21]:
    #             sum_ = sum_ + item
    #         ma20 = sum_ / 20
    #
    #     if self.Close[0] < 0.95 * ma20:
    #         self.buy()
    #     elif Close[0] > ma20 * 1.05:
    #         if I have long position:
    #             self.sell()
    #         else:
    #             pass
    #     else:
    #         # Close[0] in between 0.95 * ma20 and 1.05 * ma20, do nothing
    #         pass



# ----------------------------------------------------------------------------------------------------------------------
ast = AstockTrading('600036')  # 类的实例化
ast.get_history_data_from_local_machine()

if __name__ == "__main__":
    trade_time = time(9, 25)
    while time(9) < trade_time < time(15, 2):
        last_tick = getTick(600519)# 招行
        ast.bar_generator(last_tick)
        ast.strategy()
        trade_time = last_tick[1].time()
        print(last_tick)
        sleep(3)
        print('job done!')
