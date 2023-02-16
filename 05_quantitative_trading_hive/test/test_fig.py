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
import plotly.graph_objects as go
import pandas as pd
import numpy as np

class Ui:
    """Plotly 画图"""
    def __init__(self):
        self.appName = os.path.basename(__file__)
        self.colors = ['#008080', '#ffd000', '#ef39b2', '#10cc55', '#19a0ff', '#ff9a75', '#6e79ef', 'black']

    def get_fig(self,df):
        df.sort_values(by='trade_date', inplace=True)

        fig = go.Figure(data=[go.Candlestick(x=df['trade_date'], open=df['open_price'], high=df['high_price'],low=df['low_price'], close=df['close_price'])])

        fig.add_trace(go.Bar(
            x=df['trade_date'], y=df['volume'],
            marker_color='rgb(0,0,0,0.5)',
            name='Volume'
        ))

        fig.update_layout(
            title='The Great Recession',
            yaxis_title='AAPL Stock',
            shapes=[dict(
                x0='2022-10-01', x1='2022-10-01', y0=0, y1=1, xref='x', yref='paper',
                line_width=2)],
            annotations=[dict(
                x='2022-10-01', y=0.05, xref='x', yref='paper',
                showarrow=False, xanchor='left', text='Increase Period Begins')]
        )
        return fig


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/test/test_fig.py
if __name__ == '__main__':
    df = UiDataUtils().query_plate('BK0740', '2022-10-01', '2023-01-30')
    fig = Ui().get_fig(df)

    app = dash.Dash(__name__)
    app.layout = html.Div(
        [
            html.H1('嵌入plotly图表'),
            dcc.Graph(figure=fig)
        ]
    )
    app.run(host='0.0.0.0',port='7777')


