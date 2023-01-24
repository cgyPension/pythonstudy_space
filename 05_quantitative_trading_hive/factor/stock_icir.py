import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import time
import warnings
from datetime import date,datetime
import akshare as ak
import numpy as np
import pandas as pd
import statsmodels.api as sm
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
from util.factorFormatUtils import neutralization, factor_ic, ic_ir, factor_clean
from util.CommonUtils import get_spark

# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


def get_table_factors_icir(factors,start_date, end_date,hold_day=2,port='7777'):
    '''
    factors：表里f_前缀是已经进行去极值、标准化、行业市值中性化的因子 没有的是不需要做中性化处理
    缺失值处理：由于缺失值处理在方法上都使用了截面均值，或直接删除缺失数据，这样只能单个因子检测 除非忽略一部分数据 全部因子一起检测
    '''
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
    hold_day = hold_day

    # 剔除ST、涨停、停牌、新股
    spark_df = spark.sql("""
            with tmp_01 as (
                    select *,
                           lead(close_price,%s)over(partition by stock_code order by trade_date)/close_price-1 as holding_yield_n
                    from stock.dwd_stock_quotes_stand_di
                    where td between '%s' and '%s'
                          and stock_name not rlike 'ST'
                          and nvl(stock_label_names,'保留null') not rlike '当天涨停'
            )
            select a.trade_date,
                   a.stock_code,
                   a.stock_name,
                   a.industry_plate,
                   a.rps_5d,
                   a.rps_10d,
                   a.rps_20d,
                   a.rps_50d,
                   a.rs,
                   a.rsi_6d,
                   a.rsi_12d,
                   a.ma_5d,
                   a.ma_10d,
                   a.ma_20d,
                   a.ma_50d,
                   a.ma_120d,
                   a.ma_200d,
                   a.ma_250d,
                   a.sub_factor_score,
                   a.hot_rank,
                   a.f_volume,
                   a.f_volume_ratio_1d,
                   a.f_volume_ratio_5d,
                   a.f_turnover,
                   a.f_turnover_rate,
                   a.f_turnover_rate_5d,
                   a.f_turnover_rate_10d,
                   a.f_total_market_value,
                   a.f_pe,
                   a.f_pe_ttm,
                   a.holding_yield_n
            from tmp_01 a
            left join (select trade_date,lead(trade_date,1)over(order by trade_date) as next_trade_date from stock.ods_trade_date_hist_sina_df) b
                 on a.trade_date = b.trade_date
            where a.suspension_time is null
                  or a.estimated_resumption_time < b.next_trade_date
        """ % (hold_day, start_date, end_date))
    pd_df = spark_df.toPandas()
    pd_df['industry_plate'] = pd_df['industry_plate'].fillna('其他行业')
    pd_df['holding_yield_n'] = pd_df['holding_yield_n'].astype(float)

    result_df = pd.DataFrame()
    rlt_df = pd.DataFrame(columns=['trade_date'])  # 存储热力图
    for factor in factors:
        # pd_df.rename(columns={'factor': factor}, inplace=True)
        pd_df[factor] = pd_df[factor].astype(float)
        c_df = pd_df[['trade_date', 'stock_code', 'stock_name', factor, 'holding_yield_n']].dropna()
        ic_df = factor_ic(factor, c_df)
        ic_ir_df = ic_ir(factor, ic_df)
        result_df = result_df.append(ic_ir_df)
        # 财务字段 股票热度空的情况比较多 测试的时候财务可以单独自己测试
        rlt_df = pd.merge(rlt_df, ic_df, how='outer', on='trade_date')

    result_df['abs_ic_mean'] = abs(result_df['ic_mean'])
    result_df = result_df.sort_values(by=['abs_ic_mean', 'ic_ir'], ascending=[False, False]).drop(['abs_ic_mean'], axis=1)
    # print(result_df)
    rlt_df = rlt_df.dropna()[factors].corr().apply(lambda x: round(x, 2))

    spark.stop()
    print('{} 开始可视化!!!'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    run_cerebro_dash(result_df, rlt_df, start_date, end_date, hold_day, port=port)
    print('{}：执行完毕！！！'.format(appName))

def get_new_factors_icir(clean_factors,factors,start_date, end_date,hold_day=2,port='7777'):
    '''
    clean_factors：需要进行极值标准化行业市值中性化的因子
    factors：不需要进行极值标准化行业市值中性化的因子 ：表里f_前缀是已经进行去极值、标准化、行业市值中性化的因子 没有的是不需要做中性化处理
    缺失值处理：由于缺失值处理在方法上都使用了截面均值，或直接删除缺失数据，这样只能单个因子检测 除非忽略一部分数据 全部因子一起检测
    '''
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
    hold_day = hold_day

    # 剔除ST、涨停、停牌、新股
    spark_df = spark.sql("""
            with tmp_01 as (
                    select *,
                           -- 需要的因子逻辑字段
                           lead(close_price,%s)over(partition by stock_code order by trade_date)/close_price-1 as holding_yield_n,
                           log(total_market_value) as market_value,
                    from stock.dwd_stock_quotes_stand_di
                    where td between '%s' and '%s'
                          and stock_name not rlike 'ST'
                          and nvl(stock_label_names,'保留null') not rlike '当天涨停'
            )
            select a.trade_date,
                   a.stock_code,
                   a.stock_name,
                   -- 需要的因子逻辑字段
                   a.holding_yield_n,
                   a.market_value,
                   a.industry_plate
            from tmp_01 a
            left join (select trade_date,lead(trade_date,1)over(order by trade_date) as next_trade_date from stock.ods_trade_date_hist_sina_df) b
                 on a.trade_date = b.trade_date
            where a.suspension_time is null
                  or a.estimated_resumption_time < b.next_trade_date
        """ % (hold_day, start_date, end_date))

    pd_df = spark_df.toPandas()
    pd_df['holding_yield_n'] = pd_df['holding_yield_n'].astype(float)

    result_df = pd.DataFrame()
    rlt_df = pd.DataFrame(columns=['trade_date'])  # 存储热力图
    # 需要进行极值标准化行业市值中性化的因子
    for factor in clean_factors:
        clean_df = factor_clean(factor,pd_df)
        clean_df[factor] = clean_df[factor].astype(float)
        fhy_df = pd.merge(pd_df, clean_df, on=['trade_date','stock_code'])[['trade_date', 'stock_code', 'stock_name', factor, 'holding_yield_n']].dropna()
        ic_df = factor_ic(factor, fhy_df)
        ic_ir_df = ic_ir(factor, ic_df)
        result_df = result_df.append(ic_ir_df)
        # 财务字段 股票热度空的情况比较多 测试的时候财务可以单独自己测试
        rlt_df = pd.merge(rlt_df, ic_df, how='outer', on='trade_date')

    # 不需要进行极值标准化行业市值中性化的因子
    for factor in factors:
        pd_df[factor] = pd_df[factor].astype(float)
        pd_df = pd_df[['trade_date', 'stock_code', 'stock_name', factor, 'holding_yield_n']].dropna()
        ic_df = factor_ic(factor, pd_df)
        ic_ir_df = ic_ir(factor, ic_df)
        result_df = result_df.append(ic_ir_df)
        rlt_df = pd.merge(rlt_df, ic_df, how='outer', on='trade_date')

    result_df['abs_ic_mean'] = abs(result_df['ic_mean'])
    result_df = result_df.sort_values(by=['abs_ic_mean', 'ic_ir'], ascending=[False, False]).drop(['abs_ic_mean'], axis=1)
    # print(result_df)
    rlt_df = rlt_df.dropna()[factors].corr().apply(lambda x: round(x, 2))

    spark.stop()
    print('{} 开始可视化!!!'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    run_cerebro_dash(result_df, rlt_df, start_date, end_date, hold_day, port=port)
    print('{}：执行完毕！！！'.format(appName))

def run_cerebro_dash(icir_df,rlt_df,start_date,end_date,hold_day,port = '7777'):
    '''回测结果可视化'''
    app = dash.Dash(__name__)
    # 缓存性能优化 可以用redis
    cache = Cache(app.server, config={
        'CACHE_TYPE': 'filesystem',
        'CACHE_DIR': 'cache-directory'
    })

    rlt_data = rlt_df.values
    rlt_fig = ff.create_annotated_heatmap(
        rlt_data,
        x=rlt_df.index.tolist(),
        y=rlt_df.columns.tolist(),
        annotation_text=rlt_data,  # 标注文本内容
        # colorscale='RdBu',
        colorscale=[[0.0, '#2B3467'], [0.5, '#EFF5F5'], [1, '#EB455F']],
        showscale=True
    )
    # 字体大小设置
    for i in range(len(rlt_fig.layout.annotations)):
        rlt_fig.layout.annotations[i].font.size = 12
    # width = 1447, height = 80
    rlt_fig.update_layout(height=800)

    app.layout = html.Div(
        [
            html.H3(
                children='因子检测评估',
                style=dict(textAlign='center', color='black')),
            html.Div(
                children='检测日期：{} ~ {}  持股周期：{}天     因子数量：{}'.format(start_date,end_date,hold_day,len(icir_df)),
                style=dict(textAlign='center', color='#7FDBFF')),
            html.H4(children='因子icir', style={'margin-top': '-10px'}),
            dcc.Markdown(""" 
            ```
            ic_mean(ic均值的绝对值)：>0.05好;>0.1很好;>0.15非常好;>0.2可能错误(未来函数);当ic均值>0是正向因子
            ic_ir(ir=ic均值/ic标准差)：>=0.5认为因子稳定获取超额收益能力较强;越大越好
            ic>0：ic>0的概率 没什么作用
            abs_ic>0.02：ic绝对值>0,02的比例
            t_abs：样本T检验，X对比0，如果t只在1，-1之间，说明X均值为0，假设成立;在这绝对值应该越大越好，t_abs<1：因子有效性差
            p：当p值小于0.05时，认为与0差异显著;在这越小越好
            skew：偏度 为正则是右偏，为负则是左偏，指正态左右偏峰谷在另一则
            kurtosis：峰度 峰度描述的是分布集中趋势高峰的形态，通常与标准正态分布相比较。
                     在归一化到同一方差时，若分布的形状比标准正态分布更瘦高，则称为尖峰分布，若分布的形状比标准正态分布更矮胖，则称为平峰分布
                     当峰度系数为 0 则为标准正态分布，大于 0 为尖峰分布，小于 0 为平峰分布
            ```
            """, style={'margin-top': '-10px'}),
            dash_table.DataTable(
                id='icir',
                data=icir_df.to_dict('records'),
                columns=[{'name': column, 'id': column} for column in icir_df.columns],
                style_data_conditional=(
                        [
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(220, 220, 220)',
                            }
                        ] +
                        [
                            {
                                'if': {
                                    'filter_query': '{{{}}} > 0'.format(col),
                                    'column_id': col
                                },
                                'color': '#ff0000'
                            } for col in ['ic_mean']
                        ] +
                        [
                            {
                                'if': {
                                    'filter_query': '{{{}}} < 0'.format(col),
                                    'column_id': col
                                },
                                'color': '#008000'
                            } for col in ['ic_mean']
                        ]
                ),
                style_header={
                    'font-family': 'Times New Romer',
                    'font-weight': 'bold',
                    'font-size': 11,
                    'text-align': 'center',
                    'backgroundColor': 'rgb(210, 210, 210)',
                    'color': 'black',
                    'margin-top': '-30px'
                },
                style_data={
                    'whiteSpace': 'normal',
                    'font-family': 'Times New Romer',
                    'font-size': 11,
                    'text-align': 'center',
                    'color': 'black',
                    'backgroundColor': 'white'
                }
            ),
            html.H4('因子热力图'),
            dcc.Graph(figure=rlt_fig,style={'margin-top': '-20px'})
        ]
    )

    # host设置为0000 为了主机能访问 虚拟机的web服务 http://hadoop102:8000/
    # app.run(host='0.0.0.0', port='8000', debug=True)
    app.run(host='0.0.0.0', port=port)

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/stock_icir.py 20210101 20230118 2 7777
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/stock_icir.py 20210101 20230118 5 7777
if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("请携带3个参数 start_date, end_date, hold_day")
    else:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        hold_day = sys.argv[3]
        port = sys.argv[4]
    # factors = ['rps_5d','rps_10d','rps_20d','rps_50d','rs','rsi_6d','rsi_12d','sub_factor_score','hot_rank','f_volume','f_volume_ratio_1d','f_volume_ratio_5d','f_turnover','f_turnover_rate','f_turnover_rate_5d','f_turnover_rate_10d','f_total_market_value','f_pe','f_pe_ttm']
    factors = ['rps_5d','rps_10d','rps_20d','rps_50d','rs','rsi_6d','rsi_12d','sub_factor_score','f_volume','f_volume_ratio_1d','f_volume_ratio_5d','f_turnover','f_turnover_rate','f_turnover_rate_5d','f_turnover_rate_10d','f_total_market_value','f_pe','f_pe_ttm']
    start_time = time.time()
    get_table_factors_icir(factors,start_date,end_date,hold_day,port)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))