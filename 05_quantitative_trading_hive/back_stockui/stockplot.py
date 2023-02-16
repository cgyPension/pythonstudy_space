import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import matplotlib as mpl
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import pandas as pd
import talib as ta

# 下载的字体路径
# zhfont = mpl.font_manager.FontProperties(fname=sc.text_font)
color_list = ['steelblue', 'darkorange', 'green', 'slategray', 'hotpink', 'purple']
# 用来正常显示中文标签
plt.rcParams['font.sans-serif'] = ['FangSong']


def ind_text_font(color='black'):
    # 'fontproperties': zhfont,
    font = {'size': '12',
            'color': f'{color}',
            'va': 'bottom',
            'ha': 'right'}
    return font


class StockPlot:
    """
    绘制单股可视化图
    可动态添加指标子视图，通过 ax_sub_size 个数自动计算画布比例
    y_label 作为 key 存储 ax
    """

    def __init__(self, ax_sub_size=1):
        """
        画布会通过子 ax_sub_size 的个数来创建相对应的比例的 figsize
        :param ax_sub_size: 子指标的个数
        :param dataframe: 数据集，列名如果不同请修改
        """

        self.ax_sub_size = ax_sub_size

        self.df = pd.DataFrame()

        # 设置marketcolors
        # up:设置K线线柱颜色，up意为收盘价大于等于开盘价
        # down:与up相反，这样设置与国内K线颜色标准相符
        # edge:K线线柱边缘颜色(i代表继承自up和down的颜色)，下同。详见官方文档)
        # wick:灯芯(上下影线)颜色
        # volume:成交量直方图的颜色
        # inherit:是否继承，选填
        self.mc = mpf.make_marketcolors(
            up='red',
            down='green',
            edge='in',
            wick='in',
            volume='in',
            inherit=True)

        # 设置图形风格
        # gridaxis:设置网格线位置
        # gridstyle:设置网格线线型
        # y_on_right:设置y轴位置是否在右
        self.m_style = mpf.make_mpf_style(
            gridaxis='both', gridstyle='-.',
            figcolor='(0.82, 0.83, 0.85)',
            gridcolor='(0.82, 0.83, 0.85)',
            y_on_right=False,
            marketcolors=self.mc)

        # 0.6 为主图的高的比例，每个指标子图比例为 0.1
        self.main_h = 0.6
        self.sub_h = 0.1
        # 主图宽
        self.ax_w = 0.88
        self.fig_size = (12, 12 * (self.main_h + self.ax_sub_size * self.sub_h))
        # 保证主图宽高比不变
        if self.ax_sub_size > 2:
            self.main_h = 0.6 * 8 / self.fig_size[1]

        # 左右边界，ax_w+2*left_margin = 1
        self.left_margin = 0.06
        # 底部边界
        self.bottom_margin = 0.05
        self.face_color = (0.82, 0.83, 0.85)
        # 用于 addplot 的列表存储
        self.ap = []
        self.ax_sub_dict = dict()
        self.volume_key = 'volume'

        # 设置图片边缘距离
        # plt.subplots_adjust(left=0.08, right=0.92, top=0.92, bottom=0.08)
        plt.subplots_adjust(left=0.0, right=1.0, top=1.0, bottom=0)
        # 获取figure对象，以便对Axes对象和figure对象的自由控制
        self.fig = mpf.figure(style=self.m_style, figsize=self.fig_size, facecolor=self.face_color)

    def set_data(self, dataframe=None):
        if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
            raise Exception('DataFrame can not empty!!!')

        # 重名名列
        self.df = dataframe.rename(columns={
            'date': 'Date', 'open': 'Open',
            'high': 'High', 'low': 'Low',
            'close': 'Close', 'volume': 'Volume'})
        self.df.index.name = 'Date'
        # 均线需要替换0值
        self.df.replace({'ma05': 0, 'ma10': 0, 'ma20': 0,
                         'ma50': 0, 'ma120': 0, 'ma250': 0}, np.nan, inplace=True)

    def before_draw(self):
        plt.clf()  # 清除之前画的图
        # plt.cla()  # 清除axes
        self.fig.clear()  # 重用之前需要先清除子图
        self.ap = []
        self.ax_sub_dict.clear()

    def main_plot(self):
        # # 主图相对figure 底部 0.06，0.25，宽（0.88）、高（0.60）
        rect = [self.left_margin, self.bottom_margin + self.sub_h * self.ax_sub_size, self.ax_w, self.main_h]
        self.ax_main = self.fig.add_axes(rect)
        self.ax_main.set_ylabel('price')
        self.rect_main = Rect(rect[0], rect[1] + rect[3], rect[0] + rect[2], rect[1])

    def ema_plot(self, ema_list, calc_in_ta=True):
        """
        添加均线指标
        :param ema_list: eg [5, 10, 20, 60]
        :param calc_in_ta talib 计算
        :return:
        """
        # 通过金融库 talib 生成移动平均线，为了生成没空白的，使用原数据
        index = 0
        for i in ema_list:
            color = color_list[index]
            key = f'MA{str(i)}'
            if calc_in_ta:
                self.df[key] = ta.EMA(self.df['Close'], timeperiod=i)
            else:
                df_src_key = 'ma05' if i == 5 else f'ma{i}'
                self.df.rename(columns={df_src_key: key}, inplace=True)
            # 通过ax=axmain参数指定把新的线条添加到axmain中，与K线图重叠
            self.ap.append(mpf.make_addplot(self.df[key], color=color, ax=self.ax_main))

            # 绘制文字
            font = ind_text_font(color)
            left, top = self.rect_main.left + 0.05 * (index + 1), self.rect_main.top - 0.008
            self.fig.text(left, top, key, **font)
            index += 1

    def volume_addplot(self, y_label='volume'):
        """
        添加成交额指标
        :return:
        """
        self.volume_key = y_label
        self.__add_sub(y_label)

    def macd_addplot(self, y_label='macd'):
        """
        添加macd指标
        :param y_label:
        :return:
        """
        # if self.ax_sub_dict.get(y_label, None):
        #     raise Exception(f'has label {y_label}')

        ax, rect = self.__add_sub(y_label)
        # 通过金融库 talib 生成MACD指标，
        short_win = 12  # 短期EMA平滑天数
        long_win = 26  # 长期EMA平滑天数
        macd_win = 9  # DEA线平滑天数
        macd_tmp = ta.MACD(self.df['Close'], fastperiod=short_win,
                           slowperiod=long_win, signalperiod=macd_win)
        DIF = macd_tmp[0]
        DEA = macd_tmp[1]
        MACD = macd_tmp[2]

        self.ap.append(mpf.make_addplot(DIF, color=color_list[0], ax=ax))
        self.ap.append(mpf.make_addplot(DEA, color=color_list[1], ax=ax))
        # 使用柱状图绘制快线和慢线的差值，根据差值的数值大小，分别用红色和绿色填充
        # 红色和绿色部分需要分别填充，因此先生成两组数据，分别包含大于零和小于等于零的数据
        bar_r = np.where(MACD > 0, MACD, 0)
        bar_g = np.where(MACD <= 0, MACD, 0)
        # 使用柱状图填充（type='bar')，设置颜色分别为红色和绿色
        self.ap.append(mpf.make_addplot(bar_r, type='bar', color='red', ax=ax))
        self.ap.append(mpf.make_addplot(bar_g, type='bar', color='green', ax=ax))

        # 绘制文字
        left, top = rect.left + 0.05 * 1, rect.top - 0.01
        font = ind_text_font(color_list[0])
        self.fig.text(left, top, 'DIF', **font)

        left, top = rect.left + 0.05 * 2, rect.top - 0.01
        font = ind_text_font(color_list[1])
        self.fig.text(left, top, 'DEA', **font)

    def rps_addplot(self, columns, warning_value=87, y_label='rps'):
        """
        绘制rps指标
        :param columns: eg['rps10', 'rps20', 'rps50'] ,值为df列名
        :param warning_value: 提醒值，超过就显示红
        :param y_label:
        :return:
        """
        ax, rect = self.__add_sub(y_label)
        # 设置y轴范围
        ax.set_ylim(-4, 104)
        index = 0
        for key in columns:
            if not self.df[key].empty:
                color = color_list[index]
                self.ap.append(mpf.make_addplot(self.df[key], color=color, ax=ax))
                # 添加红色警戒线
                tmp_df = np.where(self.df[key] > warning_value, self.df[key], np.nan)
                self.ap.append(mpf.make_addplot(tmp_df, color='red', ax=ax))
                # 绘制文字
                font = ind_text_font(color)
                left, top = rect.left + 0.05 * (index + 1), rect.top - 0.008
                self.fig.text(left, top, key, **font)
                index += 1

    def other_rps_addplot(self, data, columns, warning_value=87, y_label='rps'):
        """
        绘制rps另外一个指标
        :param data:
        :param columns: eg['rps10', 'rps20', 'rps50'] ,值为df列名
        :param warning_value: 提醒值，超过就显示红
        :param y_label:
        :return:
        """
        ax, rect = self.__add_sub(y_label)
        # 设置y轴范围
        ax.set_ylim(-4, 104)
        index = 0
        for key in columns:
            if not data[key].empty:
                color = color_list[index]
                self.ap.append(mpf.make_addplot(data[key], color=color, ax=ax))
                # 添加红色警戒线
                tmp_df = np.where(data[key] > warning_value, data[key], np.nan)
                self.ap.append(mpf.make_addplot(tmp_df, color='red', ax=ax))
                # 绘制文字
                font = ind_text_font(color)
                left, top = rect.left + 0.05 * (index + 1), rect.top - 0.008
                self.fig.text(left, top, key, **font)
                index += 1

    def draw_trade(self, sell_list, buy_list):
        """
        绘制买卖信号
        df 需要以列名为 'Date' 为索引
        :param sell_list: eg: ['2022-01-10', '2022-01-11']
        :param buy_list: eg: ['2022-09-26', '2022-09-27']
        :return:
        """

        # 卖信号
        sell_color = 'blue'
        sell_signal = np.where(self.df.index.isin(sell_list), self.df['High'] * 1.008, np.nan)
        self.ap.append(mpf.make_addplot(sell_signal, type='scatter',
                                        markersize=30.0, marker=r'$\Downarrow$',
                                        color=sell_color, ax=self.ax_main))
        # 买信号
        buy_color = 'purple'
        buy_signal = np.where(self.df.index.isin(buy_list), self.df['Low'] * 0.992, np.nan)
        self.ap.append(mpf.make_addplot(buy_signal, type='scatter',
                                        markersize=30.0, marker=r'$\Uparrow$',
                                        color=buy_color, ax=self.ax_main))
        # 绘制文字
        right, top = self.rect_main.right - 0.01, self.rect_main.top - 0.02
        font = ind_text_font(sell_color)
        self.fig.text(right, top, 'sell', **font)
        top = top - 0.02
        font = ind_text_font(buy_color)
        self.fig.text(right, top, 'buy', **font)

    def plot(self, is_show):
        """
        显示并返回 figure
        :param is_show: 是否显示
        :return:
        """
        # 注意添加均线参数，macd, addplot=ap
        ax_volume = self.ax_sub_dict.get(f'{self.volume_key}', None)
        if ax_volume is None:
            mpf.plot(self.df,
                     ax=self.ax_main,
                     addplot=self.ap,
                     style=self.m_style,
                     type='candle')
        else:
            mpf.plot(self.df,
                     ax=self.ax_main,
                     volume=ax_volume,
                     addplot=self.ap,
                     style=self.m_style,
                     type='candle')
        if is_show:
            self.fig.show()

    def __add_sub(self, y_label=''):
        """
        用于添加指标的subplot
        :param y_label: 指标名称
        :return: ax
        """
        if len(y_label.strip()) == 0:
            raise Exception('label can not empty!!')
        sub_size = len(self.ax_sub_dict)
        bottom = self.bottom_margin + (self.ax_sub_size - 1 - sub_size) * self.sub_h
        rect = [self.left_margin, bottom, self.ax_w, self.sub_h]
        ax = self.fig.add_axes(rect, sharex=self.ax_main)
        self.ax_sub_dict[y_label] = ax
        ax.set_ylabel(y_label)
        return ax, Rect(rect[0], rect[1] + rect[3], rect[0] + rect[2], rect[1])

    def __del__(self):
        plt.close("all")


class Rect:
    """
    相对于figure的rect
    """

    def __init__(self, left, top, right, bottom):
        self.left = left
        self.top = top
        self.right = right
        self.bottom = bottom


class RpsPlot:

    def __init__(self):
        self.fig = plt.figure(figsize=(10, 2), dpi=80,
                              facecolor=(0.82, 0.83, 0.85),
                              edgecolor=(1, 1, 1),
                              frameon=True)
        self.df = None

        # 设置图片边缘距离
        plt.subplots_adjust(left=0.08, right=0.92, top=0.92, bottom=0.08)
        # 解决负号显示方块问题
        plt.rcParams['axes.unicode_minus'] = False

    def set_data(self, dataframe=None):
        if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
            raise Exception('DataFrame can not empty!!!')

        self.df = dataframe

    def before_draw(self):
        plt.clf()  # 清除之前画的图
        # plt.cla()  # 清除axes
        self.fig.clear(True)  # 重用之前需要先清除子图

    def plot(self, columns, warning_value=87, is_show=False):
        ax = self.fig.add_axes([0.05, 0.2, 0.9, 0.7])
        # 设置y轴范围
        ax.set_ylim(-4, 104)
        index = 0
        for key in columns:
            if not self.df[key].empty:
                color = color_list[index]
                ax.plot(self.df.index, self.df[key], color=color, lw=2)
                # 添加红色警戒线
                tmp_df = np.where(self.df[key] > warning_value, self.df[key], np.nan)
                ax.plot(self.df.index, tmp_df, color='red', lw=2)
                # 绘制文字
                font = ind_text_font(color)
                left, top = 0.05 + 0.05 * (index + 1), 0.9 - 0.008
                self.fig.text(left, top, key, **font)
                index += 1

        if is_show:
            self.fig.show()

    def __del__(self):
        plt.close("all")


class ContrastRpsPlot:
    """
    板块rps和个股rps一起画
    """

    def __init__(self):

        self.fig = plt.figure(figsize=(10, 4),
                              dpi=80,
                              facecolor=(0.82, 0.83, 0.85),
                              edgecolor=(1, 1, 1),
                              frameon=True)
        self.df = None
        self.block_data = None

        # 设置图片边缘距离
        plt.subplots_adjust(left=0.08, right=0.92, top=0.92, bottom=0.08)
        # 解决负号显示方块问题
        plt.rcParams['axes.unicode_minus'] = False

    def set_data(self, block_data, dataframe=None):
        if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
            raise Exception('DataFrame can not empty!!!')

        self.df = dataframe
        self.block_data = block_data

    def before_draw(self):
        plt.clf()  # 清除之前画的图
        # plt.cla()  # 清除axes
        self.fig.clear(True)  # 重用之前需要先清除子图

    def clear(self):
        plt.clf()  # 清除之前画的图
        # plt.cla()  # 清除axes
        self.fig.clear(True)  # 重用之前需要先清除子图

    def plot(self, columns, warning_value=87, is_show=False):

        # 板块rps
        ax = self.fig.add_axes([0.05, 0.5, 0.9, 0.4])
        # 设置y轴范围
        ax.set_ylim(-4, 104)
        index = 0
        for key in columns:
            if not self.block_data[key].empty:
                color = color_list[index]
                ax.plot(self.block_data.index, self.block_data[key], color=color, lw=2)
                # 添加红色警戒线
                tmp_df = np.where(self.block_data[key] > warning_value, self.block_data[key], np.nan)
                ax.plot(self.block_data.index, tmp_df, color='red', lw=2)
                # 绘制文字
                font = ind_text_font(color)
                left, top = 0.05 + 0.05 * (index + 1), 0.9 - 0.008
                self.fig.text(left, top, key, **font)
                index += 1

        # 个股rps
        ax = self.fig.add_axes([0.05, 0.1, 0.9, 0.4])
        # 设置y轴范围
        ax.set_ylim(-4, 104)
        index = 0
        for key in columns:
            if not self.df[key].empty:
                color = color_list[index]
                ax.plot(self.df.index, self.df[key], color=color, lw=2)
                # 添加红色警戒线
                tmp_df = np.where(self.df[key] > warning_value, self.df[key], np.nan)
                ax.plot(self.df.index, tmp_df, color='red', lw=2)
                # 绘制文字
                font = ind_text_font(color)
                left, top = 0.05 + 0.05 * (index + 1), 0.9 - 0.008
                self.fig.text(left, top, key, **font)
                index += 1

        if is_show:
            self.fig.show()

    def __del__(self):
        plt.close("all")


if __name__ == '__main__':
    # 数据库读取数据
    # df = sdb.stock_daily('000001', '2021-01-01', '2022-09-30')
    # df['rps10'] = np.random.randint(101, size=len(df))
    # df['rps20'] = np.random.randint(101, size=len(df))
    # df['rps50'] = np.random.randint(101, size=len(df))
    # sp = StockPlot(4, df)
    # sp.ema_plot([5, 10, 20, 50, 120, 250])
    # sp.volume_addplot()
    # sp.macd_addplot()
    # sp.rps_addplot(['rps10', 'rps20', 'rps50'], warning_value=50)
    # sp.macd_addplot('jklfasdk')
    # sp.draw_trade(df.index[0:5], df.index[9:13])
    # # sp.macd_addplot('djklfasdk5')
    # sp.plot(True)

    # rps_plot = RpsPlot(df)
    # rps_plot.plot(['rps10', 'rps20', 'rps50'], is_show=True)
    print(1)