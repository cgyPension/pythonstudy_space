import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import numpy as np
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots


def draw_icir_analysis_fig(df, data_dict_line, data_dict_bar, date_col=None, right_axis=None,
                           title=None):
    draw_df = df.copy()

    if date_col:
        time_data = draw_df[date_col]
    else:
        time_data = draw_df.index
    # 绘制左轴数据
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    for key in data_dict_bar:
        # fig.add_trace(go.Scatter(x=time_data, y=draw_df[data_dict[key]], name=key, ))
        fig.add_trace(go.Bar(name=key, x=time_data, y=draw_df[data_dict_bar[key]]))

    for key in data_dict_line:
        # fig.add_trace(go.Scatter(x=time_data, y=draw_df[data_dict[key]], name=key, ))
        fig.add_trace(go.Scatter(name=key, x=time_data, y=draw_df[data_dict_line[key]]))

    # fig.add_trace(go.Scatter(name='净值55日均值', x=draw_df['交易日期'], y=df['净值55日均值']))
    # 绘制右轴数据
    if right_axis:
        # for key in list(right_axis.keys()):

        # 标明设置一个不同于trace1的一个坐标轴
        keys = list(right_axis.keys())
        for r_key in keys:
            fig.add_trace(go.Scatter(x=time_data, y=draw_df[right_axis[r_key]],
                                     name=r_key + '(右轴)', yaxis='y2'))

    fig.update_layout(template="none", title_text=title, hovermode='x')

    fig.update_layout(
        font_size=10,
        # paper_bgcolor='#E0E0E0',
        # plot_bgcolor='#C0A090',
        # width=1000,
        # margin=dict(t=100, pad=10)
        margin=dict(t=30, b=70, l=30, r=0, pad=5)
    )

    fig.update(layout_xaxis_rangeslider_visible=False)

    return fig


def draw_step_fig(data_dic, name=None, title=None):
    step_list = list(data_dic.keys())
    revenue_list = list(data_dic.values())

    # 绘制左轴数据
    # fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig = make_subplots(
        rows=1, cols=2, shared_xaxes=False,
        column_widths=[0.6, 0.4],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               ])

    fig.add_trace(go.Bar(name=name, x=step_list, y=revenue_list))

    fig.update_layout(template="none", title_text=title, hovermode='x')

    fig.update_layout(
        font_size=10,
        # paper_bgcolor='#E0E0E0',
        # plot_bgcolor='#C0A090',
        # width=1000,
        # margin=dict(t=100, pad=10)
        margin=dict(t=0, b=80, l=30, r=0, pad=5)
    )

    fig.update(layout_xaxis_rangeslider_visible=False)

    return fig


def draw_recession_fig(fig, key=None, x_rec=[], y_rec=[]):
    fig.add_trace(go.Bar(name=key, x=x_rec, y=y_rec)
                  , row=1, col=2)

    return fig
