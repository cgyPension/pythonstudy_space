import os
import sys
# 在linux会识别不了包 所以要加临时搜索目
import numpy as np

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from datetime import date
import datetime
import pandas as pd
import dash
import dash_html_components as html
import pandas as pd
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

    def zx_fig(self, df,melt_list,threshold=0):
        '''
        :param df: 数据
        :param melt_list:需要画线的字段
        :param threshold: 阈值虚线值
        :return: fig
        '''
        # 折线图
        df = pd.melt(df, id_vars=['trade_date'], value_vars=melt_list)
        # 指定一个分类的数据类型 自定义排序
        category_size = pd.CategoricalDtype(melt_list, ordered=True)
        df['variable'] = df['variable'].astype(category_size)
        df.sort_values(by=['variable', 'trade_date'], inplace=True)

        fig = px.line(df, x='trade_date', y='value', color='variable',hover_data={'variable': False, 'trade_date': "|%Y-%m-%d"})
        if threshold > 0:
            # 阈值虚线
            df['threshold'] = threshold
            fig.add_trace(go.Scatter(
                x=df['trade_date'],
                y=df['threshold'],
                mode='lines',  # 模式：lines 线，markers 点。可用“+”相连
                name='阈值' + str(threshold),  # 折线名，显示于图例
                line=dict(color=('rgb(205, 12, 24)'), width=2, dash='dot')  # 虚线： dash 一一，dot ···，dashdot 一·一
            ))

        fig.update_xaxes(
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
                    dict(step='all')])))
        # fig.update_yaxes(side='right', ticklabelposition='inside')
        fig.update_yaxes(side='right')
        fig.update_layout(xaxis_title='xx板块',
                          yaxis_title=None,legend_title_text=None,
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                          font=dict(family="Times New Roman", size=16),
                          # paper_bgcolor='#1c1d21',
                          # plot_bgcolor='#1c1d21'
                          # margin=dict(b=40, l=40, r=40, t=40)
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

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,vertical_spacing=0.03, subplot_titles=('', '成交量', 'rps'),row_width=[0.2, 0.1, 0.7])
        # 绘制k数据
        fig.add_trace(go.Candlestick(x=df['trade_date'], open=df['open_price'], high=df['high_price'],low=df['low_price'], close=df['close_price'], name='',
                                     increasing_line_color="#ff3d3d", decreasing_line_color='#00a9b2',),row=1, col=1)
        # 绘制均线数据
        fig.add_trace(go.Line(x=df['trade_date'], y=df['ma_5d'], name='ma_5',line_color='#008080'), row=1, col=1)
        fig.add_trace(go.Line(x=df['trade_date'], y=df['ma_10d'], name='ma_10',line_color='#ffd000'), row=1, col=1)
        fig.add_trace(go.Line(x=df['trade_date'], y=df['ma_20d'], name='ma_20',line_color='#ef39b2'), row=1, col=1)
        fig.add_trace(go.Line(x=df['trade_date'], y=df['ma_50d'], name='ma_50',line_color='#10cc55'), row=1, col=1)
        fig.add_trace(go.Line(x=df['trade_date'], y=df['ma_120d'], name='ma_120',line_color='#19a0ff'), row=1, col=1)
        fig.add_trace(go.Line(x=df['trade_date'], y=df['ma_200d'], name='ma_200',line_color='#ff9a75'), row=1, col=1)
        fig.add_trace(go.Line(x=df['trade_date'], y=df['ma_250d'], name='ma_250',line_color='#6e79ef'), row=1, col=1)
        # 绘制成交量数据
        fig.add_trace(go.Bar(x=df['trade_date'], y=df['volume'], showlegend=False), row=2, col=1)

        # 绘制rps
        for melt in melt_list:
            fig.add_trace(go.Scatter(x=df['trade_date'], y=df[melt], name=melt), row=3, col=1)
        if threshold > 0:
            # 阈值虚线
            df['threshold'] = threshold
            fig.add_trace(go.Scatter(
                x=df['trade_date'],
                y=df['threshold'],
                mode='lines',  # 模式：lines 线，markers 点。可用“+”相连
                name='阈值' + str(threshold),  # 折线名，显示于图例
                line=dict(color=('rgb(205, 12, 24)'), width=2, dash='dot')  # 虚线： dash 一一，dot ···，dashdot 一·一
            ), row=3, col=1)
        fig.update_xaxes(
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
            # 去除休市的日期，保持连续
            rangebreaks=[dict(values=dt_breaks)],
            spikesnap='cursor'
        )
        fig.update_yaxes(side='right',spikesnap='cursor')
        fig.update(layout_xaxis_rangeslider_visible=False)
        # hovermode 修改悬停模式 'x unified' hovermode='x',
        fig.update_layout(dragmode='pan',height=1000,xaxis_title=None, yaxis_title=None, legend_title_text=None,
                          legend=dict(orientation="h", yanchor="bottom",y=1.02,xanchor="right",x=1),
                          font=dict(family="Times New Roman", size=16))
        return fig

    def test(self):
        pass

    def __exit__(self):
        print('{}：执行完毕！！！'.format(self.appName))


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/UiPlotly.py
if __name__ == '__main__':
    df = UiDataUtils().query_plate('教育', '2022-10-01', '2023-01-30')
    melt_list = ['rps_5d', 'rps_10d', 'rps_15d', 'rps_20d']
    threshold = 96
    # fig = UiPlotly().zx_fig(df, melt_list,threshold)
    # UiPlotly().test()
    # fig = UiPlotly().k_fig(df)
    fig = UiPlotly().k_rps_fig(df, melt_list,threshold)

    app = dash.Dash(__name__)
    app.layout = html.Div(
        [
            html.H1('嵌入plotly图表'),
            dcc.Graph(figure=fig)
        ]
    )
    app.run(host='0.0.0.0',port='7777')
