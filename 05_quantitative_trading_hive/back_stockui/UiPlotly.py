import os
import sys
# 在linux会识别不了包 所以要加临时搜索目
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import numpy as np
from datetime import date
import datetime
import pandas as pd
import dash
import dash_html_components as html
from dash import dcc
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
from stockui.UiDataUtils import *
from itertools import product

class UiPlotly:
    """Plotly 画图"""
    def __init__(self):
        self.appName = os.path.basename(__file__)
        self.colors = ['#008080', '#ffd000', '#ef39b2', '#10cc55', '#19a0ff', '#ff9a75', '#6e79ef', 'black']
        # 确定子图个数
        # self.fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0, row_width=[0.2, 0.1, 0.7])



    def zx_fig(self, df,melt_list,threshold=0):
        '''
        :param df: 数据
        :param melt_list:需要画线的字段
        :param threshold: 阈值虚线值
        :return: fig
        '''
        # 折线图
        # print('zx_fig',df.head())
        melt_df = pd.melt(df, id_vars=['trade_date'], value_vars=melt_list)
        # 指定一个分类的数据类型 自定义排序
        category_size = pd.CategoricalDtype(melt_list, ordered=True)
        melt_df['variable'] = melt_df['variable'].astype(category_size)
        melt_df.sort_values(by=['variable', 'trade_date'], inplace=True)
        fig = px.line(melt_df, x='trade_date', y='value', color='variable',
                      color_discrete_sequence=self.colors,
                      hover_data={'variable': False, 'trade_date': "|%Y-%m-%d"})
        if threshold > 0:
            # 阈值虚线
            melt_df['threshold'] = threshold
            fig.add_trace(go.Scatter(
                x=melt_df['trade_date'],
                y=melt_df['threshold'],
                mode='lines',  # 模式：lines 线，markers 点。可用“+”相连
                name='阈值' + str(threshold),  # 折线名，显示于图例
                line=dict(color='rgb(205, 12, 24)', width=2, dash='dot')  # 虚线： dash 一一，dot ···，dashdot 一·一
            ))

        fig.update_xaxes(
            hoverformat='%Y-%m-%d',
            ticklabelmode='instant',  # ticklabelmode模式：居中 'instant', 'period'
            tickformatstops=[
                dict(dtickrange=[3600000, "M1"], value='%Y-%m-%d'),
                dict(dtickrange=["M1", "M12"], value='%Y-%m'),
                dict(dtickrange=["M12", None], value='%Y')
            ],
            rangeselector=dict(# 增加固定范围选择
                buttons=list([
                    dict(count=1, label='1M', step='month', stepmode='backward'),
                    dict(count=6, label='6M', step='month', stepmode='backward'),
                    dict(count=1, label='1Y', step='year', stepmode='backward'),
                    dict(count=1, label='YTD', step='year', stepmode='todate'),
                    dict(step='all')])),
            spikesnap='cursor',
            spikemode='across')
        fig.update_yaxes(side='right',spikesnap='cursor',spikemode='across')
        fig.update_layout(dragmode='pan',xaxis_title=None,yaxis_title=None,legend_title_text=None,
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                          font=dict(family="Times New Roman", size=16),
                          title={'text': df['code'].iloc[0] + df['name'].iloc[0], 'font_size': 18,'x': 0.128, 'y': 0.9,'xanchor': 'left', 'yanchor': 'bottom'},
                          # paper_bgcolor='#1c1d21',
                          # plot_bgcolor='#1c1d21'
                          # 上下左右的边距大小
                          margin=dict(l=0, r=0, t=0, b=0)
        )
        return fig

    def k_rps_fig(self, df,melt_list,threshold=0):
        '''
        k线+均线+rps
        :param df: 数据
        :param melt_list:需要画rps的字段
        :param threshold: 阈值虚线值
        :return: fig
        '''
        df.sort_values(by='trade_date', inplace=True)
        # 剔除不交易日期
        min_trade_date = min(df['trade_date'])
        max_trade_date = max(df['trade_date'])
        trade_date = UiDataUtils().get_trading_date(min_trade_date, max_trade_date)['trade_date']
        dt_all = pd.date_range(start=min_trade_date, end=max_trade_date)
        dt_all = [pd.to_datetime(d).date() for d in dt_all]
        dt_breaks = list(set(dt_all) - set(trade_date))

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,vertical_spacing=0, subplot_titles=(None, None, None),row_width=[0.2, 0.1, 0.7])
        # 绘制k数据
        fig.add_trace(go.Candlestick(x=df['trade_date'], open=df['open_price'], high=df['high_price'],low=df['low_price'], close=df['close_price'], name='',
                                     increasing_line_color="#ff3d3d",
                                     increasing_fillcolor="#ff3d3d",# 填满颜色
                                     decreasing_line_color='#00a9b2',
                                     decreasing_fillcolor='#00a9b2',
                                     line_width=0.5,  # 上下影线宽度
                                     ),row=1, col=1)
        # 绘制均线数据
        mas = ['ma_5d', 'ma_10d', 'ma_20d', 'ma_50d', 'ma_120d', 'ma_200d', 'ma_250d']
        for ma, color in zip(mas, self.colors):
            # name 删除最后一个字符d
            fig.add_trace(go.Line(x=df['trade_date'], y=df[ma], name=ma[:-1], line_color=color,hoverinfo='none'), row=1, col=1)
        # 绘制成交量数据
        df_r = df[df['close_price'] - df['open_price'] >= 0.0]
        fig.add_trace(go.Bar(x=df_r['trade_date'], y=df_r['volume'], showlegend=False, marker_color='#ff3d3d',marker_line_width=0), row=2, col=1)
        df_g = df[df['close_price'] - df['open_price'] < 0.0]
        fig.add_trace(go.Bar(x=df_g['trade_date'], y=df_g['volume'], showlegend=False, marker_color='#00a9b2',marker_line_width=0), row=2,col=1)

        # 绘制rps
        for melt, color in zip(melt_list, self.colors):
            fig.add_trace(go.Scatter(x=df['trade_date'], y=df[melt], name=melt, line_color=color), row=3, col=1)
        if threshold > 0:
            # 阈值虚线
            df['threshold'] = threshold
            fig.add_trace(go.Scatter(
                x=df['trade_date'],
                y=df['threshold'],
                mode='lines',  # 模式：lines 线，markers 点。可用“+”相连
                name='阈值' + str(threshold),  # 折线名，显示于图例
                line=dict(color='rgb(205, 12, 24)', width=2, dash='dot') # 虚线： dash 一一，dot ···，dashdot 一·一
            ), row=3, col=1)

        # 绘制标记点一 买卖点
        df_b_m = df[df['rps_5d'] >= 90]
        fig.add_trace(go.Scatter(x=df_r['trade_date'], y=df_r['low_price'],mode='markers', name='buy', hoverinfo='none',showlegend=False, marker=dict(symbol='star-triangle-up', size=6, color='red')), row=1, col=1)

        df_b_m = df[df['rps_50d'] >= 90]
        fig.add_trace(go.Scatter(x=df_r['trade_date'], y=df_r['high_price'],mode='markers', name='sell', hoverinfo='none',showlegend=False, marker=dict(symbol='star-triangle-down', size=6, color='green')), row=1, col=1)

        # 全局
        fig.update_xaxes(
            hoverformat='%Y-%m-%d',
            ticklabelmode='instant',  # ticklabelmode模式：居中 'instant', 'period'
            tickformatstops=[
                dict(dtickrange=[3600000, "M1"], value='%Y-%m-%d'),
                dict(dtickrange=["M1", "M12"], value='%Y-%m'),
                dict(dtickrange=["M12", None], value='%Y')
            ],
            # 去除休市的日期，保持连续
            rangebreaks=[dict(values=dt_breaks)],
            spikesnap='cursor',
            spikemode='across'
        )
        fig.update_yaxes(side='right',spikesnap='cursor',spikemode='across')
        # 单个子图设置 k线图
        fig.update_xaxes(
            rangeselector=dict(# 增加固定范围选择
                buttons=list([
                    dict(count=1, label='1M', step='month', stepmode='backward'),
                    dict(count=6, label='6M', step='month', stepmode='backward'),
                    dict(count=1, label='1Y', step='year', stepmode='backward'),
                    dict(count=1, label='YTD', step='year', stepmode='todate'),
                    dict(step='all')])),
            row=1, col=1
        )
        fig.update_yaxes(title_text='成交量', row=2, col=1)
        fig.update_yaxes(title_text='rps', row=3, col=1)

        fig.update(layout_xaxis_rangeslider_visible=False)
        # hovermode 修改悬停模式 'x unified' hovermode='x',
        fig.update_layout(dragmode='pan',height=1000, legend_title_text=None,hovermode='x unified',
                          legend=dict(orientation="h", x=1, y=1.02, xanchor="right", yanchor="bottom"),
                          hoverlabel=dict(bgcolor='rgba(0,0,0,0)'),  # 悬停信息设置半透明背景 rgba(32,51,77,0.6)
                          font=dict(family="Times New Roman", size=16),
                          title={'text': df['code'].iloc[0] + df['name'].iloc[0], 'x': 0.128, 'y': 0.92, 'xanchor': 'left','yanchor': 'bottom'},
                          # 上下左右的边距大小
                          margin=dict(l=0, r=0, t=20, b=0))
        return fig

    def test(self,df,melt_list,threshold=0):
        trace1 = {
            "name": "K线图", "type": "candlestick",
            "x": df['trade_date'], "yaxis": "y2",
            "low": df['low_price'], "high": df['high_price'],
            "open": df['open_price'], "close": df['close_price'],
            "decreasing": {"line": {"color": "red"}},
            "increasing": {"line": {"color": "green"}}
        }
        trace2 = {
            "name": "成交量", "type": "bar",
            "x": df['trade_date'], "y": df['volume'], "yaxis": "y",
            "marker": {"color": "#6495ED"}
        }
        data = [trace1, trace2]
        layout1 = {
            "yaxis": {"domain": [0, 0.2], "showticklabels": False},
            "yaxis2": {"domain": [0.2, 0.8]},
            "legend": {
                "x": 1,
                "y": 0.8
            },
        }
        fig = go.Figure(data=data)
        fig.layout.update(layout1)
        return fig

    def __exit__(self):
        print('{}：执行完毕！！！'.format(self.appName))


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/UiPlotly.py
if __name__ == '__main__':
    df = UiDataUtils().query_plate('BK0740', '2022-10-01', '2023-01-30')
    melt_list = ['rps_5d', 'rps_10d', 'rps_15d', 'rps_20d']
    threshold = 96
    fig = UiPlotly().zx_fig(df, melt_list,threshold)
    # UiPlotly().test()
    # fig = UiPlotly().k_rps_fig(df, melt_list,threshold)
    # fig = UiPlotly().test(df, melt_list,threshold)

    app = dash.Dash(__name__)
    app.layout = html.Div(
        [
            html.H1('嵌入plotly图表'),
            dcc.Graph(figure=fig)
        ]
    )
    app.run(host='0.0.0.0',port='7777')
