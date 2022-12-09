import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import numpy as np
import pandas as pd
from dash import Dash, dash_table, dcc, html, Input, Output, State

from factor_figure import *
from factor_format import *
from factor_score import *
from dash.long_callback import DiskcacheLongCallbackManager
from datetime import datetime

## Diskcache
import diskcache

cache = diskcache.Cache("./cache")
long_callback_manager = DiskcacheLongCallbackManager(cache)

# app = Dash(__name__)  # 数据开始时间
app = Dash(__name__, long_callback_manager=long_callback_manager)

####################### global config ###################################

# candle_list = all_data_list

####################### dash layout ###################################
styles = {
    'pre': {
        'border': 'thin lightgrey solid',
        # 'overflowX': 'scroll'
    }
}


def make_progress_graph(progress, total):
    progress_graph = (
        go.Figure(data=[go.Bar(x=[progress])])
        .update_xaxes(range=[0, total])
        .update_yaxes(
            showticklabels=False,
        )
        .update_layout(height=100, margin=dict(t=20, b=40))
    )
    return progress_graph


app.layout = html.Div([

    html.Div([
        html.Div([
            dcc.Graph(id='factor-graphic'),
        ], style={'width': '80%'}),
        html.Div([
            html.Label('数据选择'),
            dcc.Dropdown(
                all_data_list,
                id='stock-data-list',
                # value=random.choice(equity_list)
                value=all_data_list[0]
            ),
            html.Br(),

            html.Label('因子选择'),
            dcc.Dropdown(
                factor_list,
                id='factor-list',
                value=factor_list[0]
            ),

            # 第一个参数是标签列表
            # 第二个参数是默认选中列表
            dcc.Checklist(option_list,
                          option_list,
                          id="option-checklist",
                          ),
            html.Button(id="refresh_btn", children="更新图表"),
            html.Div([
                html.P(id="paragraph_id", children=["更新完毕"]),
                html.Progress(id="progress_bar"),
            ]),
            html.Br(),

            # dcc.Loading(id="ls-loading-1", children=[html.Div(id="ls-loading-output")], type="default"),
            # html.Br(),
        ], style={'width': '20%', 'float': 'right'})
    ], style={'display': 'flex', 'flex-direction': 'row', 'backgroundColor': '#CCDAEE'}),

    html.Div([
        html.Div([
            dcc.Graph(id='factor-graphic2'),
        ], style={'width': '80%'}),

        html.Div([
            html.Label('因子评估表'),
            dash_table.DataTable(
                id='computed-table',
                columns=[
                    {'name': '指标类型', 'id': 'index-name'},
                    {'name': '评分', 'id': 'index-value'}
                ],
                data=[{'index-name': fac_scr} for fac_scr in factor_score_list],
                editable=False,
                # style_cell={
                #     'overflow': 'hidden',
                #     'textOverflow': 'ellipsis',
                #     'maxWidth': 0,
                # },
                style_cell_conditional=[
                    {'if': {'column_id': 'index-name'},
                     'width': '40%'},
                    {'if': {'column_id': 'index-value'},
                     'width': '60%'},
                ]
            ),
            # html.Br(),
            html.Div([
                dcc.Markdown("""
                    ```
                    'T_ABS均值' : >2则 因子显著有效
                    'T_ABS>2(%)' : 因子显著性是否稳定
                    'IC_均值' : 因子整体趋势(spearman 同涨同跌不看幅度)
                    'IC_IR' : 因子是否稳健，越大越稳定
                    'IC>0.00(%)' : 是否为正向因子
                    'IC>0.02(%)' : 是否为可用因子(均值为负，则<-0.02)
                    'IC>0.05(%)' : 是否为强势因子(均值为负，则<-0.05)
                    ```
                """),
                html.Pre(id='relayout-data', style=styles['pre']),
            ], style={'display': 'block'})
            # ], style={'padding': 2, 'flex': 1})
        ], style={'width': '20%', 'float': 'right'})
    ], style={'display': 'flex', 'flex-direction': 'row',
              'backgroundColor': '#C0CAB7'}),

])


####################### figure callback ###################################

# 主图表： 策略查询
# @app.callback(
@app.long_callback(
    Output('factor-graphic', 'figure'),
    Output('factor-graphic2', 'figure'),
    # Output("ls-loading-output", "children"),
    Output('computed-table', 'data'),
    Input('refresh_btn', 'n_clicks'),
    Input('stock-data-list', 'value'),
    State('factor-list', 'value'),
    State("option-checklist", "value"),
    State('computed-table', 'data'),
    running=[
        (Output("refresh_btn", "disabled"), True, False),
        (
                Output("paragraph_id", "style"),
                {"visibility": "hidden"},
                {"visibility": "visible"},
        ),
        (
                Output("progress_bar", "style"),
                {"visibility": "visible"},
                {"visibility": "hidden"},
        ),
    ],
    progress=[Output("progress_bar", "value"), Output("progress_bar", "max")]
)
def update_graph_main(set_progress, n_clicks, stock_file, factor_key, options_format, score_rows):
    # 标记开始时间
    reset_time()

    debug_mode = False
    percent_max = 10

    print(f'@update factor:{stock_file}_{factor_key}')

    print(all_data_list)

    cur_factor_info = {factor_key: factor_info[factor_key]}

    print_diff_time('start_fmt')
    set_progress((str(0), str(percent_max)))

    all_stock_df = pd.read_pickle(stock_data_path + stock_file)

    set_progress((str(1), str(percent_max)))

    print_diff_time('dtart_drop_na')
    df = drop_na_factor(all_stock_df, factor_key)

    set_progress((str(2), str(percent_max)))

    # 1.去极值
    # 2.市场行业中性化
    # 3.标准化，虽然rank ic也算标准化，但是ic衰减还是需要标准化数据就一起做了
    df = factor_format(df,
                       options_format, factors=list(cur_factor_info.keys()))

    # if debug_mode:
    #     tmp_fig = make_subplots(specs=[[{"secondary_y": True}]])
    #     return tmp_fig, tmp_fig, score_rows

    print(f'@update factor options:{options_format}')

    set_progress((str(4), str(percent_max)))

    print_diff_time('start_icir')
    df = cal_rank_ic_ir(df, factor_info=cur_factor_info, recall=recall_cycle)

    set_progress((str(5), str(percent_max)))

    ############################## 左上角 ICIR和累计ICIR的关系 ##########################
    print_diff_time('start_fig_1')
    factor_ic = f'{factor_key}_RankIC'
    factor_ic_mean = f'{factor_key}_RankIC_mean'
    factor_ir = f'{factor_key}_RankICIR'
    factor_cumsum = f'{factor_key}_RankIC_cumsum'

    # key = show name | data = col name
    data_dic_bar_left = {factor_ic + f'_{recall_cycle}T': factor_ic_mean}
    data_dic_line_left = {factor_ir + f'_{recall_cycle}T': factor_ir}

    group_df = pd.DataFrame()
    temp_df = pd.DataFrame()
    group_df[factor_ic] = df.groupby('交易日期')[factor_ic].mean()
    group_df[factor_ic_mean] = df.groupby('交易日期')[factor_ic_mean].mean()
    group_df[factor_ir] = df.groupby('交易日期')[factor_ir].mean()

    # 日期从索引还原回回数据列
    group_df.reset_index(drop=False, inplace=True)
    group_df[factor_cumsum] = group_df[factor_ic].cumsum()

    # right dict process
    data_dic_right = {}
    data_dic_right[f'{factor_key}_ic累加'] = factor_cumsum
    for base_val in [0.02, 0.05]:
        factor_base_val = base_val
        factor_base = f'{factor_base_val}_IC_base'
        factor_base_cumsum = f'{factor_base_val}_IC_base_cumsum'
        group_df[factor_base] = factor_base_val
        group_df[factor_base_cumsum] = group_df[factor_base].cumsum()

        data_dic_right[f'{factor_base_val}_参考ic累加'] = factor_base_cumsum

    set_progress((str(6), str(percent_max)))
    fig_factor = draw_icir_analysis_fig(group_df, date_col='交易日期',
                                        data_dict_line=data_dic_line_left,
                                        data_dict_bar=data_dic_bar_left,
                                        right_axis=data_dic_right, title=None)

    ######################## 左下角 因子排名分位数和长期涨跌幅的排布 ########################

    print_diff_time('start_cross')
    df = cal_rank_by_cross(df, factor=factor_key, pct_enable=True)

    print_diff_time('start_step')
    # ret dic {分位数:涨跌幅均值}，
    # per 百分比
    data_dic = cal_step_data(df, factor=factor_key, per=1)

    set_progress((str(7), str(percent_max)))

    print(f'draw step figure', datetime.now() - start_time)
    fig_bottom = draw_step_fig(data_dic, name=f'{factor_key}_升序收益分布', title=None)

    ######################## 右下角 IC衰退 ########################
    print_diff_time('start_recess')
    x, y = get_ic_recession(df, factor_key)

    set_progress((str(8), str(percent_max)))

    print_diff_time('start_draw_fig2')
    fig_bottom = draw_recession_fig(fig_bottom, key=f'{factor_key}_ic衰退',
                                    x_rec=x, y_rec=y)
    ######################## 右下角 评分表 ########################
    print_diff_time('start_fac_score')
    result = cal_factor_score(df, factor_key)

    set_progress((str(9), str(percent_max)))
    print_diff_time('score_done')

    for index in range(len(score_rows)):
        # for row in score_rows:
        row = score_rows[index]
        # print(row)
        row['index-value'] = result[index]

    set_progress((str(10), str(percent_max)))
    print_diff_time('process_done')

    return fig_factor, fig_bottom, score_rows


if __name__ == '__main__':
     # app.run_server(debug=True)
    app.run_server(debug=False)
