import numpy as np
import pandas as pd
import plotly.graph_objects as go
import talib as ta
from dash import dcc, html, dash
from plotly.subplots import make_subplots

from config import stockconstant as sc
from database import stockdatabase as sdb


class PlotlyStock:

    def __init__(self, data, dt_breaks, rows):
        """
        :param data: 数据集合
         pd.DataFrame(
         columns = ['close','open','low','high','volume','ma05', 'ma10', 'ma20', 'ma50', 'ma120', 'ma250']
        index = datetime('%Y-%m-%d'))
        :param dt_breaks: 非交易日 ['2020-01-01','2020-01-01']
        :param rows: 子图个数，包括k线主图
        """

        # 处理为两位小数
        tmp_data = data.select_dtypes(include=['float'])
        tmp_data = tmp_data.applymap(lambda x: round(x, 2))
        data[list(tmp_data.columns)] = tmp_data[list(tmp_data.columns)]
        self.df = data
        self.rows = rows

        # trace
        self._k_trace = None
        self._volume_trace = None
        self._ma_trace_dict = None
        self._macd_trace = None
        self._other_line_trace_dict = None  # 其他线的指标
        self._ma_keys = ['ma05', 'ma10', 'ma20', 'ma50', 'ma120', 'ma250']

        # 样式
        self.upColor = '#fe4761'  # 涨的颜色
        self.downColor = '#3fcfb4'  # 跌的颜色
        self.gridcolor = '#46587b'  # 网格线颜色
        self.linecolor = '#46587b'  # 边界线颜色
        self.background_color = '#20334d'  # 背景色
        self.font_name = 'Times New Roman'  # 文字字体
        self.font_color = '#8f98ad'  # 文字颜色
        self.colors_list = ['#008080', '#ffd000', '#ef39b2', '#10cc55',
                            '#19a0ff', '#ff9a75', '#6e79ef', 'black']  # 线的颜色
        self.line_width = 0.5  # 线宽
        self.date_format = '%Y-%m-%d'

        # 确定子图个数
        self.fig = make_subplots(rows=self.rows, cols=1, shared_xaxes=True, vertical_spacing=0, )
        # 设置全局配置
        self._set_global_setting()
        # 解决k线交易日期不连续问题
        self.fig.update_xaxes(rangebreaks=[dict(values=dt_breaks)])

    def _set_global_setting(self):
        """
        全局的一些配置信息
        :param fig:
        :return:
        """
        # 滑块是否显示
        self.fig.update(layout_xaxis_rangeslider_visible=False)

        # 修改layout样式
        self.fig.update_layout(
            paper_bgcolor=self.background_color,  # 设置背景色
            plot_bgcolor=self.background_color,  # 设置作图区域背景色
            font=dict(family=self.font_name, size=12, color=self.font_color),  # 刻度字体
            # 修改悬停信息,x unified 汇总x轴所有
            dragmode='pan', legend_title_text=None, hovermode='x unified',
            hoverlabel=dict(bgcolor='rgba(32,51,77,0.6)'),  # 悬停信息设置半透明背景

            # 图例放顶部
            legend=dict(orientation="h", x=1, y=1.02, xanchor="right", yanchor="bottom"),
            height=700,
        )
        # 修改x轴样式
        self.fig.update_xaxes(
            showgrid=False,
            gridcolor=self.gridcolor,
            linecolor=self.linecolor,
            linewidth=1,
            gridwidth=0.5,
            griddash='dash',  # 网格线类型 ---
            layer='below traces',
            spikesnap='cursor',
            spikemode='across',
            spikecolor=self.gridcolor,
            spikethickness=1,
            title_font=dict(family=self.font_name, size=10, color=self.font_color),
            tickformat=self.date_format,  # 修改x轴刻度时间格式
            hoverformat=self.date_format  # 修改悬停x时间格式
        )
        # 修改y轴样式
        self.fig.update_yaxes(
            showgrid=True,
            gridcolor=self.gridcolor,
            linecolor=self.linecolor,
            linewidth=1,
            gridwidth=0.5,
            griddash='dash',  # 网格线类型 ---
            layer='below traces',  # 网格线在 traces 后面
            side='right',
            spikesnap='cursor',  # 设置基准线光标
            spikemode='across',  # 基准线 +
            spikecolor=self.gridcolor,  # 基准线颜色
            spikethickness=1,  # 基准线宽
            zeroline=False,  # 水平 0 线不显示
            title_font=dict(family=self.font_name, size=10, color=self.font_color),  # trace 字体
        )

    def k_line_trace(self):
        """
        k线trace
        :return:
        """
        self._k_trace = go.Candlestick(
            x=self.df.index,
            open=self.df['open'],
            high=self.df['high'],
            low=self.df['low'],
            close=self.df['close'],
            name='',  # trace 名字
            increasing_line_color=self.upColor,
            increasing_fillcolor=self.upColor,  # 填满颜色
            decreasing_line_color=self.downColor,
            decreasing_fillcolor=self.downColor,
            line_width=0.5,  # 上下影线宽度
            # showlegend=False,
        )

    def volume_trace(self):
        """
        成交量trace
        :return:
        """
        df_r = df[df['close'] - df['open'] >= 0.0]
        # 红柱
        volume_trace_r = go.Bar(
            x=df_r.index, y=df_r['volume'], name='',
            showlegend=False,  # 是否显示图例
            marker_line_width=0,  # 边框宽度
            marker_color=self.upColor, )
        # 绿柱
        df_g = df[df['close'] - df['open'] < 0.0]
        volume_trace_g = go.Bar(
            x=df_g.index, y=df_g['volume'],
            name='', showlegend=False,
            marker_line_width=0,
            marker_color=self.downColor, )

        self._volume_trace = (volume_trace_r, volume_trace_g)

    def ma_trace(self):
        """
        均线trace
        :return:
        """
        # 如果没有主图，不允许添加均线
        if self._k_trace is None:
            raise Exception('_k_trace can not null!!')

        self._ma_trace_dict = dict()
        for key, color in zip(self._ma_keys, self.colors_list):
            # 均线需要替换0值
            self.df.replace(
                {'ma05': 0, 'ma10': 0, 'ma20': 0,
                 'ma50': 0, 'ma120': 0, 'ma250': 0}, np.nan, inplace=True)

            self._ma_trace_dict[key] = go.Scatter(
                x=self.df.index, y=self.df[key],
                mode='lines', name=key, hoverinfo='none',
                line=dict(color=color, width=self.line_width))

    def trade_marker(self, buy_points, sell_points):
        """
        标注交易点，hoverinfo='none' 不显示悬停信息
        showlegend=False 不显示图例
        :param buy_points: 买点list['2020-01-01','2020-01-01']
        :param sell_points: 卖点list
        :return:
        """

        # 如果没有主图，不允许添加标注交易点
        if self._k_trace is None:
            raise Exception('_k_trace can not null!!')

        size = 6
        buy_standard = 0.980
        sell_standard = 1.020
        y_step = 0.015

        # 买点
        y_buy = df.loc[buy_points, 'low']
        self.fig.add_trace(go.Scatter(
            x=buy_points, y=y_buy * buy_standard,
            mode='markers', name='buy', hoverinfo='none',
            marker=dict(symbol='star-triangle-up', size=size, color='red')
        ))
        self.fig.add_trace(go.Scatter(
            x=buy_points, y=y_buy * (buy_standard - 1 * y_step),
            mode='markers', name='buy', showlegend=False, hoverinfo='none',
            marker=dict(symbol='star-triangle-up', size=size, color='red')
        ))
        self.fig.add_trace(go.Scatter(
            x=buy_points, y=y_buy * (buy_standard - 2 * y_step),
            mode='markers', name='buy', showlegend=False, hoverinfo='none',
            marker=dict(symbol='star-triangle-up', size=size, color='red')
        ))

        # 卖点
        y_sell = df.loc[sell_points, 'high']
        self.fig.add_trace(go.Scatter(
            x=sell_points, y=y_sell * sell_standard,
            mode='markers', name='sell', hoverinfo='none',
            marker=dict(symbol='star-triangle-down', size=size, color='green')
        ))
        self.fig.add_trace(go.Scatter(
            x=sell_points, y=y_sell * (sell_standard + y_step),
            mode='markers', name='sell', showlegend=False, hoverinfo='none',
            marker=dict(symbol='star-triangle-down', size=size, color='green')
        ))
        self.fig.add_trace(go.Scatter(
            x=sell_points, y=y_sell * (sell_standard + 2 * y_step),
            mode='markers', name='sell', showlegend=False, hoverinfo='none',
            marker=dict(symbol='star-triangle-down', size=size, color='green')
        ))

    def macd_trace(self):
        """
        macd 指标
        :return:
        """
        self._macd_trace = list()

        # 通过金融库 talib 生成MACD指标，
        short_win = 12  # 短期EMA平滑天数
        long_win = 26  # 长期EMA平滑天数
        macd_win = 9  # DEA线平滑天数
        macd_tmp = ta.MACD(self.df['close'], fastperiod=short_win,
                           slowperiod=long_win, signalperiod=macd_win)
        dif = list(map(lambda x: round(x, 3), macd_tmp[0]))
        dea = list(map(lambda x: round(x, 3), macd_tmp[1]))
        macd = macd_tmp[2]

        # 使用柱状图绘制快线和慢线的差值，根据差值的数值大小，分别用红色和绿色填充
        # 红色和绿色部分需要分别填充，因此先生成两组数据，分别包含大于零和小于等于零的数据
        # 快慢线
        self._macd_trace.append(go.Scatter(
            x=self.df.index, y=dif, mode='lines', name='DIF',
            line=dict(color=self.colors_list[0], width=self.line_width)))
        self._macd_trace.append(go.Scatter(
            x=self.df.index, y=dea, mode='lines', name='DEA',
            line=dict(color=self.colors_list[1], width=self.line_width)))

        # 使用柱状图填充（type='bar')，设置颜色分别为红色和绿色
        bar_r = np.where(macd >= 0, macd, 0)
        bar_r = list(map(lambda x: round(x, 3), bar_r))
        bar_g = np.where(macd < 0, macd, 0)
        bar_g = list(map(lambda x: round(x, 3), bar_g))
        s_bar_r = pd.Series(bar_r, index=self.df.index)
        s_bar_r = s_bar_r[s_bar_r >= 0]
        s_bar_g = pd.Series(bar_g, index=self.df.index)
        s_bar_g = s_bar_g[s_bar_g < 0]

        # 红柱
        self._macd_trace.append(go.Bar(
            x=s_bar_r.index, y=s_bar_r, name='',
            showlegend=False,  # 是否显示图例
            marker_line_width=0,  # 边框宽度
            marker_color=self.upColor, ))
        # 绿柱
        self._macd_trace.append(go.Bar(
            x=s_bar_g.index, y=s_bar_g, name='',
            showlegend=False, marker_line_width=0,
            marker_color=self.downColor, ))

    def other_line_trace(self, keys, label,
                         data=None, threshold=None):
        if self._other_line_trace_dict is None:
            self._other_line_trace_dict = dict()

        # 如果data为null,使用全局的self.df否则使用data的数据
        if data is None:
            df = self.df
        else:
            # 对齐self.df.index
            df, data = data.align(self.df, join='right', axis=0)
        line_width = 1
        line_trace_dict = dict()
        for key, color in zip(keys, self.colors_list):
            if threshold is None:
                line_trace_dict[key] = go.Scatter(
                    x=self.df.index, y=df[key],
                    mode='lines', name=key,
                    line=dict(color=color, width=line_width))
            else:
                line_trace_dict[key] = go.Scatter(
                    x=self.df.index, y=df[key],
                    mode='lines+markers', name=key,
                    line=dict(color=color, width=line_width),
                    marker=dict(color=['#ff0000' if y_i > threshold else color for y_i in df[key]],
                                size=[line_width + 2.5 if y_i > threshold else 0 for y_i in df[key]],
                                line=dict(color='#ff0000', width=0)  # 设置边框
                                ),
                )

                # 新增一条画线的有问题
                # line_trace_dict[f'{key}_normal'] = go.Scatter(
                #     x=df.index, y=df[key],
                #     mode='lines', name=f'{key}_normal',
                #     line=dict(color='#ff0000', width=line_width))
                # line_trace_dict[key] = go.Scatter(
                #     x=df.index, y=[y_i if y_i <= threshold else None for y_i in df[key]],
                #     mode='lines', name=key,
                #     line=dict(color=color, width=line_width))

        self._other_line_trace_dict[label] = line_trace_dict

    def plot(self):
        """
        动态控制子图domain位置
        :return:
        """

        row_count = 1

        # 添加k线
        if not self._k_trace is None:
            self.fig.add_trace(self._k_trace, row=row_count, col=1)
            y_domain = self._y_domain(row_count)
            print(f'主图： {y_domain}')
            self.fig.update_yaxes(title_text='股价', domain=self._y_domain(row_count), row=row_count, col=1, )
            row_count = row_count + 1

        # 添加均线
        if not self._ma_trace_dict is None:
            for key in self._ma_keys:
                self.fig.add_trace(self._ma_trace_dict[key], row=1, col=1)

        # 添加成交量
        if not self._volume_trace is None:
            y_domain = self._y_domain(row_count)
            print(f'成交量： {y_domain}')
            self.fig.add_trace(self._volume_trace[0], row=row_count, col=1)
            self.fig.add_trace(self._volume_trace[1], row=row_count, col=1)
            self.fig.update_yaxes(title_text='成交量', domain=self._y_domain(row_count), row=row_count, col=1)
            row_count = row_count + 1

        # 添加macd
        if not self._macd_trace is None:
            y_domain = self._y_domain(row_count)
            print(f'添加macd： {y_domain}')
            self.fig.add_trace(self._macd_trace[0], row=row_count, col=1)
            self.fig.add_trace(self._macd_trace[1], row=row_count, col=1)
            self.fig.add_trace(self._macd_trace[2], row=row_count, col=1)
            self.fig.add_trace(self._macd_trace[3], row=row_count, col=1)
            self.fig.update_yaxes(title_text='macd', domain=self._y_domain(row_count), row=row_count, col=1)
            row_count = row_count + 1

        # 其他线指标
        if not self._other_line_trace_dict is None:
            for label, trace_dict in self._other_line_trace_dict.items():
                for key, trace in trace_dict.items():
                    self.fig.add_trace(trace, row=row_count, col=1)

                self.fig.update_yaxes(title_text=label, domain=self._y_domain(row_count),
                                      range=[-4, 104],
                                      row=row_count, col=1)
                # 下一个其他线子图
                row_count = row_count + 1

        return self.fig

    def _y_domain(self, row):
        """
        fig 范围 [left,top,right,bottom] -> [0.0,1.0,1.0,0.0]
        最大添加 6 个图
        1. k线开始区域范围[0.6,0.0] 对应子图数[6,0]
        2. 其他子图开始区域范围是[0.0,0.6]
        3. 如果没有k线图则其他子图根据个数平分y_domain
        :param row: 子图所处的位置
        :return: 子图y_domain区域
        """

        if not self._k_trace is None:
            sub_count = self.rows - 1
            # 主图开始位置，如果子图小于等于3，直接分配子图区域0.2，其他
            if sub_count <= 3:
                k_y_start_domain = 0.2 * sub_count
            elif sub_count == 4:
                k_y_start_domain = 0.5
            else:
                k_y_start_domain = 0.6

            k_y_end_domain = 1.0

            if row == 1:  # 主图
                return [k_y_start_domain, k_y_end_domain]
            # 子图位置
            else:
                sub_y_domain = k_y_start_domain
                sub_y_domain_step = sub_y_domain / sub_count
                sub_y_start_domain = k_y_start_domain - (row - 1) * sub_y_domain_step  # 往下推子图开始位置
                sub_y_end_domain = sub_y_start_domain + sub_y_domain_step  # 子图结束区域加步长就是子图结束区域
                return [sub_y_start_domain, sub_y_end_domain]
        else:
            sub_count = self.rows
            sub_y_domain_step = 1.0 / self.rows
            sub_y_start_domain = (sub_count - row) * sub_y_domain_step
            sub_y_end_domain = sub_y_start_domain + sub_y_domain_step
            return [sub_y_start_domain, sub_y_end_domain]


if __name__ == '__main__':
    df = sdb.stock_daily_complete('002642', '2022-01-01', '2023-01-18')
    trade_date_df = pd.read_csv(sc.trade_date_path).set_index('trade_date')
    trade_date_df = trade_date_df[df.index[0].strftime('%Y-%m-%d'):df.index[-1].strftime('%Y-%m-%d')]
    trade_date = [str(d) for d in trade_date_df.index]
    # 剔除不交易日期
    dt_all = pd.date_range(start=df.index[0], end=df.index[-1])
    dt_all = [d.strftime("%Y-%m-%d") for d in dt_all]
    print(trade_date)
    # 剔除不连续x
    dt_breaks = list(set(dt_all) - set(trade_date))

    # 设置子图数
    stock_plot = PlotlyStock(df, dt_breaks, rows=4)
    # k线trace
    stock_plot.k_line_trace()
    # 均线
    stock_plot.ma_trace()
    # 买卖点
    buy_points = ['2022-01-07', '2022-04-01', '2022-04-15', '2022-04-25']
    sell_points = ['2022-04-28', '2022-05-09', '2022-06-30', '2022-06-15']
    # 标注交易点
    stock_plot.trade_marker(buy_points, sell_points)

    # # 成交量
    stock_plot.volume_trace()
    # macd
    stock_plot.macd_trace()

    # rps
    keys = ['rps20', 'rps50', 'rps120']
    stock_plot.other_line_trace(keys, 'RPS', threshold=87)
    # stock_plot.other_line_trace(keys, '板块RPS', threshold=87)

    fig = stock_plot.plot()
    app = dash.Dash(__name__)
    app.layout = html.Div(
        [
            html.H1('嵌入plotly图表'),
            dcc.Graph(figure=fig)
        ]
    )
    app.run(host='0.0.0.0', port='8888')
