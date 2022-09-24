# -*- coding: utf-8 -*-
# 视频15：利用bar数据模拟tick数据
# http://qt.gtimg.cn/q=sh600519
# F9
# str = string

# tuple?
# float, int, str->datetime?
# how to get out of while loop?

import requests
from time import sleep
from datetime import datetime, time, timedelta
from dateutil import parser


class AstockTrading(object):
    def __init__(self, strategy_name):
        # attributes
        self._strategy_name = strategy_name
        self._Open = []
        self._High = []
        self._Low = []
        self._Close = []
        self._Dt = []
        self._Volume = []
        self._tick = None  # or tuple
        self._last_bar_start_minute = None

        self._isNewBar = False
        self._ma20 = None

        # dict, 字典
        self._current_orders = {}
        self._history_orders = {}
        self._order_number = 0

    # def getTick(self):  # def method
    #     # go to sina to get last tick info
    #     page = requests.get("http://hq.sinajs.cn/?format=text&list=sh600519")
    #     stock_info = page.text
    #     mt_info = stock_info.split(",")
    #
    #     last = float(mt_info[1])
    #     trade_datetime = parser.parse(mt_info[30] + ' ' + mt_info[31])
    #
    #     self._tick = (trade_datetime, last)

    def getTick(self):
        '''
        # go to gtime to get last tick info
        parameter: symbol, without suffix or prefix, such as 600519
        return: tick (tuple format) with datetime and last price
        '''
        # symbol = str(symbol)
        # symbol = 'sh' + symbol if symbol[0] in ['5', '6'] else 'sz' + symbol

        page = requests.get('http://qt.gtimg.cn/q==sh600519')
        full_tick = page.text.split('~')
        self._tick = (float(full_tick[3]), parser.parse(full_tick[30]))

    def get_history_data_from_local_machine(self):
        # some code here
        # self._Open = [1,2,3]
        # self._High = []
        self._Open = []
        self._High = []

    # how save and import history data?
    def bar_generator(self):
        # last < 0.95 * ma20, long, last > ma20 * 1.05, sell
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
        # last_bar_start_minute = None

        if self._tick[0].minute % 5 == 0 and \
                self._tick[0].minute != self._last_bar_start_minute:
            # create a new bar
            self._last_bar_start_minute = self._tick[0].minute
            self._Open.insert(0, self._tick[1])
            self._High.insert(0, self._tick[1])
            self._Low.insert(0, self._tick[1])
            self._Close.insert(0, self._tick[1])
            self._Dt.insert(0, self._tick[0])

            self._isNewBar = True

        else:
            # update current bar
            self._High[0] = max(self._High[0], self._tick[1])
            self._Low = min(self._Low[0], self._tick[1])
            self._Close[0] = self._tick[1]
            self._Dt[0] = self._tick[0]
            self._isNewBar = False

    def _buy(self, price, volume):
        # create an order
        self._order_number += 1
        # {key: value}
        'order1'
        key = 'order' + str(self._order_number)
        self._current_orders[key] = {
            'open_datetime': self._Dt[0],
            'open_price': price,
            'volume': volume
        }

    def _sell(self, key, price):
        self._current_orders[key]['close_price'] = price
        self._current_orders[key]['close_datetime'] = self._Dt[0]
        # move order from current orders to history orders
        self._history_orders[key] = self._current_orders.pop(key)

    def strategy(self):
        # last < 0.95 * ma20, long, last > ma20 * 1.05, sell
        # ma20 = Close[:19].sum()/20
        if self._isNewBar:  # _isNewBar == True:
            sum_ = 0
            for item in self._Close[1:21]:
                sum_ = sum_ + item
            self._ma20 = sum_ / 20

        if 0 == len(self._current_orders):
            if self._Close[0] < 0.95 * self._ma20:
                # 100000/44.28 = 2258
                volume = int(100000 / self._Close[0] / 100) * 100  # 2200 shares
                self._buy(self._Close[0] + 0.01, volume)

        elif 1 == len(self.current_orders):  # have long position
            if self._Close[0] > self._ma20 * 1.05:
                key = self._current_orders.keys()[0]
                self._sell(key, self._Close[0] - 0.01)
        else:
            raise ValueError("we have more than 1 current orders!")


# ----------------------------------------------------------------------------
# Dt, Open, High, Low, Close, Volume= \
#     get_history_data_from_local_machine()

# trade_time = time(9, 25)
# while time(9) < trade_time < time(15, 2):
#     last_tick = getTick()
#     Dt, Open, High, Low, Close, Volume = \
#         bar_generator(last_tick, Dt, Open, High, Low, Close, Volume)
#     strategy(Dt, Open, High, Low, Close, Volume)
#     # trade_time = parser.parse(last_tick[1].time())

#     # wait for 3 second
#     sleep(3)
# print('job done!')

ma = AstockTrading('ma')
ma.get_history_data_from_local_machine()

while time(9, 26) < datetime.now().time() < time(11, 32) or \
        time(13) < datetime.now().time() < time(15, 2):
    ma.getTick()
    ma.bar_generator()
    ma.strategy()

# backtesting
# history data
# 15 minute bar -> tick data
# pandas? baidu, bing -> what is pandas 
import pandas as pd
import os
import numpy as np
from dateutil import parser


# 利器之一: Ndarray
# 利器之二: 切片和索引
# read csv

# pandas ??
def get_ticks_for_backtesting():
    tick_path = 'd:\\python_stock\\stock_data\\600036_ticks.csv'
    bar_path = 'd:\\python_stock\\stock_data\\600036_5m.csv'

    # generate tick data, and save it.
    # if we have tick data, use it. or, we create tick data by bar data.
    if os.path.exists(tick_path):
        ticks = pd.read_csv(
            tick_path,
            parser_dates=['datetime'],
            index_col='datetime'
        )
    else:
        bar_5m = pd.read_csv(bar_path)
        ticks = []

        for index, row in bar_5m.iterrows():
            if row['open'] < 30:
                step = 0.01
            elif row['open'] < 60:
                step = 0.03
            elif row['open'] < 90:
                step = 0.05
            else:
                step = 0.1
            arr = np.arange(row['open'], row['high'], step)
            arr = np.append(arr, row['high'])
            arr = np.append(arr, np.arange(row['open'] - step, row['low'], -step))
            arr = np.append(arr, row['row'])
            arr = np.append(arr, row['close'])

            i = 0
            dt = parser(row['datetime']) - timedelta(minutes=5)
            for item in arr:
                ticks.append(dt + timedelta(seconds=0.1 * i), item)
                i += 1

        tick_df = pd.DataFrame(ticks, columns=['datetime', 'price'])
        tick_df.to_csv(tick_path, index=0)

    return ticks