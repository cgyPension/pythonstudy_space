# -*- coding: utf-8 -*-
# 视频22：升级MA策略
# F9
# str = string

# tuple?
# float, int, str->datetime?
# how to get out of while loop?

import requests
from time import sleep
from datetime import datetime, time, timedelta
from dateutil import parser
import pandas as pd
import os
import numpy as np


# ----------------------------------------------------------------------------------------------------------------------
def get_ticks_for_backtesting(tick_path, bar_path):
    '''
    # func: get ticks for backtesting, need two params
    # param1 tick_path: csv file with tick data, 
    #   when there is no tick data,
    #   use bar_path to create tick data
    # param2 bar_path: csv file with bar data,
    #   use in creating tick data.
    # tick_path example: 'd:\\python_stock\\stock_data\\600036_ticks.csv'
    # bar_path example: 'd:\\python_stock\\stock_data\\600036_5m.csv'
    # return: ticks in list with tuples in it, such as
    # [(datetime, last_price), (datetime, last_price)]
    '''
    # generate tick data, and save it.
    # if we have tick data, use it. or, we create tick data by bar data.    
    if os.path.exists(tick_path):
        ticks = pd.read_csv(
            tick_path, 
            parse_dates=['datetime'], 
            index_col='datetime'
        )
        
        tick_list = []
        for index, row in ticks.iterrows():
            tick_list.append((index, row[0]))
            
        # ticks = np.array(tick_list)
        ticks = tick_list
        
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
            # in case of np.arange(30, 30.11, 0.02), (open, high, step)
            # we will not have 30.11 as the highest price,
            # we might not catch high when step is more than 0.01
            # that is why we need arr = np.append(arr, row['high']) and
            # arr = np.append(arr, row['low'])
            arr = np.arange(row['open'], row['high'], step)
            arr = np.append(arr, row['high'])
            arr = np.append(arr, np.arange(row['open']-step, row['low'], -step))
            arr = np.append(arr, row['row'])
            arr = np.append(arr, row['close']) 
        
            i = 0
            dt = parser(row['datetime']) - timedelta(minutes=5)
            for  item in arr:
                ticks.append(dt + timedelta(seconds = 0.1 * i), item)
                i += 1
    
        tick_df = pd.DataFrame(ticks, columns=['datetime', 'price'])
        tick_df.to_csv(tick_path, index=0)
        
    return ticks


# ------------------------------------------------------------------------------------------------------------------
class AstockTrading(object):
    '''
    # class: A stock trading platform, needs one param, 
    #   It has backtesting, paper trading, and real trading.
    # param1: strategy_name: strategy name
    '''
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
        
        self._is_new_bar = False
        self._ma20 = []
        self._ma20.insert(0, None)
        self._close_minus_ma20 = np.zeros(20)

        # dict, 字典
        self._current_orders = {}
        self._history_orders = {}
        self._order_number = 0
        self._init = False

    # ------------------------------------------------------------------------------------------------------------------
    def get_tick(self):
        '''
        # func: for paper or real trading, not for backtesting
        #   It goes to sina to get last tick into,
        #   http://qt.gtimg.cn/q==sh600519
        #   'sh600519' needs to be changed.
        #   A股的开盘时间是9:15，9:15-9:25是集合竞价->开盘价，9:25
        #   9:25-9:30不交易， 时间>9:30， 交易开始。
        #   start this method after 9:25.
        #   tick info is orginized in tuple,
        #   such as (trade_time, last_price),
        #   tick info saved in self._tick
        # param: no param
        # return: None
        '''
        # page = requests.get("http://hq.sinajs.cn/?format=text&list=sh600519")
        # stock_info = page.text
        # mt_info = stock_info.split(",")
        #
        # last = float(mt_info[1])
        # trade_datetime = parser.parse(mt_info[30] + ' ' + mt_info[31])
        #
        # self._tick = (trade_datetime, last)

        page = requests.get('http://qt.gtimg.cn/q==sh600519')
        full_tick = page.text.split('~')
        self._tick = (float(full_tick[3]), parser.parse(full_tick[30]))

    # ------------------------------------------------------------------------------------------------------------------
    def get_history_data_from_local_machine(self):
        '''
        not done yet.
        :return:
        '''
        self._Open = []
        self._High = []
        self._Low = []
        self._Close = []
        self._Dt = []

    # ------------------------------------------------------------------------------------------------------------------
    def bar_generator(self):
        '''
        not done yet.
        how save and import history data?
        :return:
        '''
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
            
            self._is_new_bar = True
            
        else:
            # update current bar
            self._High[0] = max(self._High[0], self._tick[1])
            self._Low = min(self._Low[0], self._tick[1])
            self._Close[0] = self._tick[1]
            self._Dt[0] = self._tick[0]
            self._is_new_bar = False

    # ------------------------------------------------------------------------------------------------------------------
    def _buy(self, price, volume):
        '''
        create a long order
        needs two params
        param1 price: buying price
        param2 volume: buying volume
        return None
        '''
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

    # ------------------------------------------------------------------------------------------------------------------
    def _sell(self, key, price):
        '''
        close a long order(s)
        It needs two params
        param1 key: long order's key
        param2 price: sell price
        return None
        '''
        self._current_orders[key]['close_price'] = price
        self._current_orders[key]['close_datetime'] = self._Dt[0]
        
        # calculate pnl
        volume = self._current_orders[key]['volume']
        open_price = self._current_orders[key]['open_price']
        # minus stamp duty and commission
        self._current_orders[key]['pnl'] = \
            (price - open_price) * volume \
                - price * volume * 1/1000 \
                - (price + open_price) * volume * 3/10000
        
        # move order from current orders to history orders
        self._history_orders[key] = self._current_orders.pop(key)

    # ------------------------------------------------------------------------------------------------------------------
    def strategy(self):
        '''

        :return:
        '''
        # last < 0.95 * ma20, long, last > ma20 * 1.05, sell
        # ma20 = Close[:19].sum()/20
        if self._is_new_bar:  # _is_new_bar == True:
            # sum_ = 0
            # for item in self._Close[1:21]:
            #     sum_ = sum_ + item
            self._ma20.insert(1, sum(self._Close[1:21]) / 20)
            self._close_minus_ma20[2:] = self._close_minus_ma20[1:len(self._close_minus_ma20)-1]
            self._close_minus_ma20[1] = self._Close[1] - self._ma20[1]

        if 0 == len(self._current_orders):
            if self._Close[0] < 0.98 * self._ma20[1]:
                if (self._close_minus_ma20 < 0).sum() > 10 \
                        and self._close_minus_ma20.sum() / self._Close[1] < -0.02:
                    # 100000/44.28 = 2258
                    volume = int(100000/self._Close[0]/100) * 100  # 2200 shares
                    self._buy(self._Close[0]+0.01, volume)
                
        elif 1 == len(self._current_orders):  # have long position
            if self._Close[0] > self._ma20[1] * 1.02:
                key = list(self._current_orders.keys())[0]
                if self._Dt[0].date() != self._current_orders[key]['open_datetime'].date():
                    self._sell(key, self._Close[0]-0.01)
                    print('open date is %s, close date is %s.'
                          % (self._history_orders[key]['open_datetime'].date(),
                             self._Dt[0].date()))
                else:
                    # if same dates, sell order aborted due to T+0 limit
                    print('sell order aborted due to T+0 limit ')
        else:
            raise ValueError("we have more than 1 current orders!")

    # ------------------------------------------------------------------------------------------------------------------
    def bar_generator_for_backtesting(self, tick):
        '''
        for backtesting only,
        used to update _Open, _High, etc.
        It needs just one parameter,
        param tick: tick info in tuple, (datetime, price)
        return: None
        '''
        if tick[0].minute % 5 == 0 and \
                tick[0].minute != self._last_bar_start_minute:
            # create a new bar
            self._last_bar_start_minute = tick[0].minute
            self._Open.insert(0, tick[1])
            self._High.insert(0, tick[1])
            self._Low.insert(0, tick[1])
            self._Close.insert(0, tick[1])
            self._Dt.insert(0, tick[0])
            self._is_new_bar = True
        else:
            # update current bar
            self._High[0] = max(self._High[0], tick[1])
            self._Low[0] = min(self._Low[0], tick[1])
            self._Close[0] = tick[1]
            self._Dt[0] = tick[0]
            self._is_new_bar = False

    # ------------------------------------------------------------------------------------------------------------------
    def run_backtesting(self, ticks):
        '''
        ticks will be used to generate bars,
        when bars is long enough, call strategy()
        parameters
        -----------
        ticks: list with (datetime, price) in the list
        return: None
        '''
        for tick in ticks:
            self.bar_generator_for_backtesting(tick)
            # create first bar
            if self._init:
                self.strategy()
            else:
                if len(self._Open) >= 100:
                    self._init = True
                    self.strategy()
            

# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    tick_path = 'f:\\python_stock\\stock_data\\600036_ticks.csv'
    bar_path = 'f:\\python_stock\\stock_data\\600036_5m.csv'
    ticks = get_ticks_for_backtesting(tick_path, bar_path)
    ast = AstockTrading('ma')
    ast.run_backtesting(ticks)

    ast._current_orders
    ast._history_orders

    profit_orders = 0
    loss_orders = 0
    orders = ast._history_orders
    for key in orders.keys():
        if orders[key]['pnl'] >= 0:
            profit_orders += 1
        else:
            loss_orders += 1

    win_rate = profit_orders / len(orders)
    loss_rate = loss_orders / len(orders)

    orders_df = pd.DataFrame(orders).T
    orders_df.loc[:, 'pnl'].plot.bar()

    # date2num
    from mplfinance.original_flavor import candlestick_ohlc
    import matplotlib.pyplot as plt
    from matplotlib.dates import date2num

    bar5 = pd.read_csv(bar_path, parse_dates=['datetime'])

    # [x for x in iterable] 
    bar5.loc[:, 'datetime'] = [date2num(x) for x in bar5.loc[:, 'datetime']]

    fig, ax = plt.subplots()
    candlestick_ohlc(
        ax,
        bar5.values,
        width=0.2,
        colorup='r',
        colordown='green',
        alpha=1.0
    )

    # put orders on candle stick
    for index, row in orders_df.iterrows():
        ax.plot(
            [row['open_datetime'], row['close_datetime']],
            [row['open_price'], row['close_price']],
            color='darkblue',
            marker='o')
    plt.show()
