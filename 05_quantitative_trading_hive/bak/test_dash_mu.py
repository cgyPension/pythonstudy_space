import os
import sys
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import backtrader as bt
from backtrader.mathsupport import average
from datetime import date, datetime
import datetime
import time
# 衡量策略绩效指标的库
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt  # 由于 Backtrader 的问题，此处要求 pip install matplotlib==3.2.2
import akshare as ak
import dash
from dash import dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import dash_table
from flask_caching import Cache
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
# 在linux会识别不了包 所以要加临时搜索目录
import seaborn as sns
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

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/test_dash_mu.py
def run_cerebro_dash(list):
    '''回测结果可视化'''
    app = dash.Dash(__name__)
    # 缓存性能优化 可以用redis
    cache = Cache(app.server, config={
        'CACHE_TYPE': 'filesystem',
        'CACHE_DIR': 'cache-directory'
    })


    # app.layout = html.Div(
    #     dbc.Container(
    #         dash_table.DataTable(
    #             columns=[{'name': column, 'id': column} for column in df.columns],
    #             data=df.to_dict('records'),
    #             virtualization=True
    #         ),
    #         style={
    #             'margin-top': '100px'
    #         }
    #     )
    # )

    app.layout = html.Div(
        dbc.Spinner(
            dbc.Container(
                [
                    dcc.Location(id='url'),
                    html.Div(
                        id='page-content'
                    )
                ],
                style={
                    'paddingTop': '30px',
                    'paddingBottom': '50px',
                    'borderRadius': '10px',
                    'boxShadow': 'rgb(0 0 0 / 20%) 0px 13px 30px, rgb(255 255 255 / 80%) 0px -13px 30px'
                }
            ),
            fullscreen=True
        )
    )

    @app.callback(
        Output('strategy-links', 'children'),
        Input('url', 'pathname')
    )
    def render_strategy_links(pathname):
        return [
            html.Li(
                dcc.Link(href, href=f'/strategy-{i+1}_{href}', target='_blank')
            )
            for i,(href, df) in enumerate(list)
        ]

    @app.callback(
        Output('page-content', 'children'),
        Input('url', 'pathname')
    )
    def render_strategy_content(pathname):

        if pathname == '/':
            return [
                html.H2('策略列表：'),
                html.Div(
                    id='strategy-links',
                    style={
                        'width': '100%'
                    }
                )
            ]
        elif pathname.startswith('/strategy-'):
             index = pathname.split('-')[1].split('_')[0]
             df = list[int(index)-1]['基准']
             return (
                 html.Div(
                     dbc.Container(
                         dash_table.DataTable(
                             columns=[{'name': column, 'id': column} for column in df.columns],
                             data=df.to_dict('records'),
                             virtualization=True
                         ),
                         style={
                             'margin-top': '100px'
                         }
                     )
                 )
             )
        return dash.no_update


    # host设置为0000 为了主机能访问 虚拟机的web服务
    # http://hadoop102:8000/
    # app.run(host='0.0.0.0', port='8000', debug=True)
    app.run(host='0.0.0.0', port=8000)

if __name__ == '__main__':

    appName = os.path.basename(__file__)
    start_time = time.time()
    benchmark_df = ak.stock_zh_index_daily(symbol='sh000300')
    start_date1, end_date1 = pd.to_datetime('20200101').date(), pd.to_datetime('20201230').date()
    start_date2, end_date2 = pd.to_datetime('20220101').date(), pd.to_datetime('20221230').date()
    benchmark_df1 = benchmark_df[(benchmark_df['date'] >= start_date1) & (benchmark_df['date'] <= end_date1)]
    benchmark_df2 = benchmark_df[(benchmark_df['date'] >= start_date2) & (benchmark_df['date'] <= end_date2)]

    # strategy_name,
    rt_list = []
    # rt_list = [['策略':'策略1','基准':benchmark_df1],['策略2',benchmark_df2]]
    rt_list.append({'策略': '策略1', '基准': benchmark_df1})
    rt_list.append({'策略': '策略2', '基准': benchmark_df2})

    print('{} 获取数据 运行完毕!!!'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    run_cerebro_dash(rt_list)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))