import pandas as pd

import akshare as ak

import tushare as ts

import ffn

import empyrical

import pyautogui

import pywinauto

import matplotlib.pyplot as plt

import seaborn as sns

import statsmodels.api as sm

from finta import TA

import mplfinance as mpf

import time

import pyttsx3

import PySimpleGUI as sg

import easyquotation

import quantstats as qs


# 登录同花顺函数

def log():
    # 启动同花顺

    pywinauto.application.Application(backend='uia').start(r'C:\同花顺软件\同花顺\hexin.exe')

    # 等待缓冲

    time.sleep(15)

    # 登录

    # 按下f12快速登录

    # 我们选择模拟交易

    pyautogui.press('f12')

    # 等待缓冲

    time.sleep(15)

    print('登录成功')

    pyttsx3.speak('登录成功')

    time.sleep(5)

    pyautogui.click(x=1370, y=46)

    time.sleep(5)

    pyautogui.click(x=56, y=122)


# 直接启动同花顺交易程序

def log_trader():
    pyautogui.click(x=1831, y=898)

    pyautogui.doubleClick(x=1831, y=898)

    pyttsx3.speak('启动程序')

    time.sleep(5)


# 买入交易

def buy_stock(stock='600111', num='100'):
    '''

    stock股票代码，price买入价格，num买入数量

    '''

    pyautogui.click(x=56, y=122)

    # time.sleep(1)

    pyautogui.press('f1')

    # time.sleep(1)

    # 输入股票代码

    pyautogui.click(x=424, y=158)

    pyautogui.typewrite(str(stock))

    # time.sleep(1)

    # 输入交易价格

    pyautogui.click(x=448, y=224)

    # time.sleep(1)

    # 输入交易数量

    pyautogui.click(x=448, y=315)

    pyautogui.typewrite(num)

    # time.sleep(1)

    # 点击买入

    pyautogui.click(x=467, y=357)

    # time.sleep(1)

    pyautogui.press('y')

    print('买入成功')

    pyttsx3.speak('买入成功')


# 批量买入

def buy_all_stock(stock1, num='500'):
    '''

    stock股票代码,数据类型为列表，price买入价格，num买入数量

    '''

    for stock in stock1:
        pyautogui.click(x=56, y=122)

        # time.sleep(1)

        pyautogui.press('f1')

        # time.sleep(1)

        # 输入股票代码

        pyautogui.click(x=424, y=158)

        pyautogui.typewrite(str(stock))

        # time.sleep(1)

        # 输入交易价格

        pyautogui.click(x=448, y=224)

        # time.sleep(1)

        # 输入交易数量

        pyautogui.click(x=448, y=315)

        pyautogui.typewrite(num)

        # time.sleep(1)

        # 点击买入

        pyautogui.click(x=467, y=357)

        # time.sleep(1)

        pyautogui.press('y')

        print('买入成功')

        pyttsx3.speak('买入成功')

        time.sleep(2)


# 卖出股票

def sell_stock(stock='600111', num='100'):
    '''

    stock股票代码，price买入价格，num买入数量

    '''

    # 交易买入按下快捷键f2

    pyautogui.click(x=56, y=122)

    pyautogui.press('f2')

    # time.sleep(1)

    # 输入股票代码

    pyautogui.click(x=424, y=158)

    pyautogui.typewrite(str(stock))

    # time.sleep(1)

    # 输入交易价格

    pyautogui.click(x=448, y=224)

    # time.sleep(1)

    # 输入交易数量

    pyautogui.click(x=448, y=315)

    pyautogui.typewrite(num)

    # time.sleep(1)

    # 点击买入

    pyautogui.click(x=467, y=357)

    # time.sleep(1)

    pyautogui.press('y')

    print('卖出成功')

    pyttsx3.speak('卖出成功')

    time.sleep(1)

    pyautogui.click(x=956, y=631)


# 批量卖出

def sell_all_stock(stock1, num='500'):
    '''

    stock1股票代码，price买入价格，num买入数量

    '''

    # 交易买入按下快捷键f2

    for stock in stock1:
        pyautogui.click(x=56, y=122)

        pyautogui.press('f2')

        # time.sleep(1)

        # 输入股票代码

        pyautogui.click(x=424, y=158)

        pyautogui.typewrite(str(stock))

        # time.sleep(1)

        # 输入交易价格

        pyautogui.click(x=448, y=224)

        # time.sleep(1)

        # 输入交易数量

        pyautogui.click(x=448, y=315)

        pyautogui.typewrite(num)

        # time.sleep(1)

        # 点击买入

        pyautogui.click(x=467, y=357)

        # time.sleep(1)

        pyautogui.press('y')

        print('卖出成功')

        pyttsx3.speak('卖出成功')

        time.sleep(2)


# 撤回买单

def cancel_buy():
    '''

    撤回买单

    '''

    # 按下f4

    pyautogui.click(x=56, y=122)

    pyautogui.press('f3')

    time.sleep(1)

    # 撤回买单

    pyautogui.press('x')

    time.sleep(1)

    pyautogui.press('y')

    print('撤回买单成功')

    pyttsx3.speak('撤回买单成功')


# 撤回卖单单

def cancel_sell():
    '''

    撤回卖单

    '''

    # 按下f4

    pyautogui.click(x=56, y=122)

    pyautogui.press('f3')

    time.sleep(1)

    # 撤回买单

    pyautogui.press('c')

    time.sleep(1)

    pyautogui.press('y')

    print('撤回卖单成功')

    pyttsx3.speak('撤回卖单成功')


# 撤回全部单

def cancel_all():
    '''

    撤回全部单

    '''

    # 按下f4

    pyautogui.click(x=56, y=122)

    pyautogui.press('f3')

    # time.sleep(1)

    # 撤回买单

    pyautogui.press('x')

    time.sleep(1)

    pyautogui.press('y')

    print('撤回全部单成功')

    pyttsx3.speak('撤回全部单成功')


# 获取账户数据

def save_account_data():
    # 进入账户

    pyautogui.click(x=56, y=122)

    pyautogui.press('f4')

    # time.sleep(1)

    # 保存数据

    pyautogui.hotkey('ctrl', 's')

    # time.sleep(1)

    # 保存数据名称

    pyautogui.typewrite('account_data.xlsx')

    # 保存

    pyautogui.click(x=1299, y=712)

    pyttsx3.speak('账户数据保存成功')

    # time.sleep(1)

    pyautogui.press('y')


# 保存交易数据

def save_trader_data():
    # 进入账户

    pyautogui.click(x=56, y=122)

    pyautogui.press('f4')

    # time.sleep(1)

    pyautogui.click(x=71, y=452)

    # time.sleep(1)

    # 保存数据

    pyautogui.hotkey('ctrl', 's')

    time.sleep(1)

    # 保存数据名称

    pyautogui.typewrite('tradert_data.xlsx')

    # 保存

    pyautogui.click(x=1299, y=712)

    pyttsx3.speak('历史交易数据保存成功')

    # time.sleep(1)

    pyautogui.press('y')


# 获取股票实时数据，数据来源腾讯财经

nane = []

code = []

now = []

Open = []

close = []

low = []

high = []

volume = []

bid1 = []

ask1 = []

ask1_volume = []

bid1_volume = []

date = []

zdf = []

zt = []

dt = []


# 获取股票实时数据分析

def get_stock_data(stock='600111'):
    df = easyquotation.use('qq')

    if str(stock)[0] == '6':

        stock1 = 'sh' + str(stock)

    else:

        stock1 = 'sz' + str(stock)

    df1 = df.real(stock1)

    stock = str(stock)

    date.append(df1[stock]['name'])

    code.append(df1[stock]['code'])

    nane.append(df1[stock]['name'])

    now.append(df1[stock]['now'])

    Open.append(df1[stock]['open'])

    close.append(df1[stock]['close'])

    low.append(df1[stock]['low'])

    high.append(df1[stock]['high'])

    volume.append(df1[stock]['volume'])

    bid1.append(df1[stock]['bid1'])

    ask1.append(df1[stock]['ask1'])

    ask1_volume.append(df1[stock]['ask1_volume'])

    bid1_volume.append(df1[stock]['bid1_volume'])

    zdf.append(df1[stock]['涨跌'])

    zt.append(df1[stock]['涨停价'])

    dt.append(df1[stock]['跌停价'])

    stock_data = pd.DataFrame(
        {'date': date, '股票名称': nane, '股票代码': code, '现价': now, 'open': Open, 'close': close, 'low': low, 'high': high,

         'volume': volume, 'bid1': bid1, 'ask1': ask1, 'sk1_volume': ask1_volume, 'bid1_volume': bid1_volume,
         'zdf': zdf, 'zt': zt, 'dt': dt})

    stock_data.to_excel(r'C:\Users\Administrator\Desktop\交易数据\股票实时数据.xlsx')

    pyttsx3.speak('数据保存成功')


# 技术分析

def indicator_analysis_df(x):
    if x >= 0:

        return '买入'

    else:

        return '卖出'


def indicator_analysis(stock='600111'):
    # 默认macd 分析

    if stock[0] == '6':

        stock = 'sh' + stock

    else:

        stock = 'sz' + stock

    df = ak.stock_zh_a_daily(symbol=stock, start_date='20210601')

    df.to_excel(r'C:\Users\Administrator\Desktop\交易数据\个股数据.xlsx')

    macd_data = TA.MACD(df)

    macd_data['MACD-SIGNAL'] = macd_data['MACD'] - macd_data['SIGNAL']

    macd_data['交易'] = macd_data['MACD-SIGNAL'].apply(indicator_analysis_df)

    macd_data.to_excel(r'C:\Users\Administrator\Desktop\交易数据\技术分析.xlsx')


# 昨日资金流入排行

def pre_cash_flow_on():
    df = ak.stock_individual_fund_flow_rank(indicator='今日')

    # 昨日资金流入

    df.to_excel(r'C:\Users\Administrator\Desktop\交易数据\昨日资金流入.xlsx')

    # 昨日资金流入前五

    df[:5].to_excel(r'C:\Users\Administrator\Desktop\交易数据\昨日资金流入前五.xlsx')


# 今日资金流入排行

def today_cash_flow_on():
    df = ak.stock_individual_fund_flow_rank(indicator='今日')

    # 今日资金流入

    df.to_excel(r'C:\Users\Administrator\Desktop\交易数据\今日资金流入.xlsx')

    # 今日资金流入前五

    df[:5].to_excel(r'C:\Users\Administrator\Desktop\交易数据\今日资金流入前五.xlsx')


# 实时交易数据分析

# 实时交易数据分析

def jx(x):
    if x >= 0:

        return '买入'

    else:

        return '卖出'


def real_time_trader_now_analysis():
    df = pd.read_excel(r'C:\Users\Administrator\Desktop\交易数据\股票实时数据.xlsx')

    df['sma3'] = df['现价'].rolling(window=3).mean()

    df['sma5'] = df['现价'].rolling(window=5).mean()

    df['sma10'] = df['现价'].rolling(window=5).mean()

    df['sma3-sma5'] = df['sma3'] - df['sma5']

    df['sma5-sma10'] = df['sma5'] - df['sma10']

    df['sma3-sma5交易记录'] = df['sma3-sma5'].apply(jx)

    df.to_excel(r'C:\Users\Administrator\Desktop\交易数据\实时交易数据分析.xlsx')


# 个股量化指标分析

def stock_quant_analysis():
    df = pd.read_excel(r'C:\Users\Administrator\Desktop\交易数据\个股数据.xlsx')

    qs.extend_pandas()

    reports = qs.reports.metrics(df['close'])

    print(reports)


# 交易策略

# 登录我们可以采用电脑自动启动程序，也可以手动打开程序

# 我们根据资金流入排行进行买卖，我们选取资金流入的前五进行买入，

# 每天清仓昨天资金流入买入的股票,九点半到10点清仓

# 我们选取资金流入第一的进行实时交易

# 数据准备,用来运行一次程序，资金想长期持有的股票

Count = 0

while True:

    start_run_time = time.localtime()

    day = start_run_time.tm_mday

    h = start_run_time.tm_hour

    m = start_run_time.tm_min

    # 如果时间在9点45到10点清仓昨天买入的股票

    # 给开盘15给形成资金流入排行数600111600111据，5分钟用来清仓库

    if h == 9 and m > 45 and m < 50:

        pre_cash_flow_on()

        pre = pd.read_excel(r'C:\Users\Administrator\Desktop\交易数据\昨日资金流入前五.xlsx')

        # 卖100手

        sell_all_stock(stock1=pre['代码'][:5].tolist(), num='1000')

    # 五分钟交易

    elif h == 9 and m >= 50 and m < 55:

        # 买入今日资金流入前五的股票

        today_cash_flow_on()

        today = pd.read_excel(r'C:\Users\Administrator\Desktop\交易数据\今日资金流入前五.xlsx')

        buy_all_stock(stock1=today['代码'][:5].tolist())

    else:

        if h == 9 and m >= 50:

            # 大买入,运行一次买入一次

            if Count == 0:

                # 获取个股数据

                indicator_analysis(stock='600111')

                indi_data = pd.read_excel(r'C:\Users\Administrator\Desktop\交易数据\技术分析.xlsx')

                if indi_data['交易'].tolist()[-1] == '买入':

                    buy_stock(stock='600111', num='1000')

                    pyttsx3.speak('大买入')

                elif indi_data['交易'].tolist()[-1] == '卖出':

                    sell_stock(stock='600111', num='1000')

                    pyttsx3.speak('大卖出')

                # MACD死叉600111

                elif indi_data['MACD'].tolist()[-2] >= indi_data['SIGNAL'].tolist()[-2] and indi_data['MACD'].tolist()[
                    -1] > indi_data['SIGNAL'].tolist()[-1]:

                    sell_stock(stock='600111', num='1000')

                    pyttsx3.speak('大卖出')

                # macd金叉买入

                elif indi_data['MACD'].tolist()[-2] <= indi_data['SIGNAL'].tolist()[-2] and indi_data['MACD'].tolist()[
                    -1] < indi_data['SIGNAL'].tolist()[-1]:

                    buy_stock(stock='600111', num='1000')

                    pyttsx3.speak100('大买入')

            # 一天中实时不间断交易

        else:

            today_5 = pd.read_excel(r'C:\Users\Administrator\Desktop\交易数据\今日资金流入前五.xlsx')

            pre_5 = pd.read_excel(r'C:\Users\Administrator\Desktop\交易数据\昨日资金流入前五.xlsx')

            while True:

                get_stock_data(stock=today_5['代码'].tolist()[0])

                run_time = time.localtime()

                tm_day = start_run_time.tm_mday

                tm_h = start_run_time.tm_hour

                tm_m = start_run_time.tm_min

                # 如果在交易时间，进行交易

                if (tm_h >= 9 and tm_h < 11) or (tm_day >= 13 and tm_h < 15):

                    # 买入资金流入第一的

                    get_stock_data(stock=today_5['代码'].tolist()[0])

                    # 进行数据实时分析

                    real_time_trader_now_analysis()

                    # 读取股票实时数据分析

                    real_data = pd.read_excel(r'C:\Users\Administrator\Desktop\交易数据\实时交易数据分析.xlsx')

                    # 买入

                    if real_data['sma3-sma5交易记录'].tolist()[-1] == '买入':

                        buy_stock(stock=today_5['代码'].tolist()[0])

                        pyttsx3.speak('小买入')

                    # 卖出

                    elif real_data['sma3-sma5交易记录'].tolist()[-1] == '卖出':

                        sell_stock(stock=today_5['代码'].tolist()[0])

                        pyttsx3.speak('小卖出')

                    # 均线金叉买入

                    elif real_data['sma3'].tolist()[-2:-2] <= real_data['sma5'].tolist()[-2:-2] and real_data[
                                                                                                        'sma3'].tolist()[
                                                                                                    -1:-1] > real_data[
                                                                                                                 'sma3'].tolist()[
                                                                                                             -1:-1]:

                        buy_stock(stock=today_5['代码'].tolist()[0], num='500')

                        pyttsx3.speak('均线金叉买入')

                    # 均线死叉卖出

                    elif real_data['sma3'].tolist()[-2:-2] >= real_data['sma5'].tolist()[-2:-2] and real_data[
                                                                                                        'sma3'].tolist()[
                                                                                                    -1:-2] < real_data[
                                                                                                                 'sma3'].tolist()[
                                                                                                             -1:-2]:

                        sell_stock(stock=today_5['代码'].tolist()[0], num='500')

                        pyttsx3.speak('均线死叉卖出')

                    # 快要收盘交易处理,最后3分钟处理

                    elif tm_h == 14 and tm_m >= 57:

                        # 保存交易数据

                        save_trader_data()

                        # 保存账户数据

                        save_account_data()

                        # 没有成交的全部撤回

                        cancel_all()

                        # 间隔15秒交易一次

                time.sleep(1)

    Count += 8