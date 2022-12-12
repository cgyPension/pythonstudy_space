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
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
# 在linux会识别不了包 所以要加临时搜索目录
from util import algorithmUtils

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

class StockCommission(bt.CommInfoBase):
    '''自定义股票交易费用'''
    params = (
        ('stocklike', True),  # 指定为股票模式
        ('commtype', bt.CommInfoBase.COMM_PERC),  # 使用百分比费用模式
        ('percabs', True),  # commission 不以 % 为单位
        ('stamp_duty', 0.001),)  # 印花税默认为 0.1%
    # 自定义费用计算公式
    def _getcommission(self, size, price, pseudoexec):
        if size > 0:  # 买入时，只考虑佣金
            return abs(size) * price * self.p.commission
        elif size < 0:  # 卖出时，同时考虑佣金和印花税
            return abs(size) * price * (self.p.commission + self.p.stamp_duty)
        else:
            return 0

# 添加分析器
def add_ananlsis_indictor(cerebro):
    '''A股一年有250日交易日
      backtrader默认是美股一年252个交易日
      用官方的分析器成本更高
    '''
    # 添加自定义的分析指标
    # cerebro.addanalyzer(Kelly, _name='_Kelly')
    cerebro.addanalyzer(trade_list, _name='_tradelist')
    cerebro.addanalyzer(trade_assets, _name='_trade_assets')

def get_analysis_indictor(start,benchmark_df):
    tl_df = pd.DataFrame(start.analyzers._tradelist.get_analysis())
    # Kelly_df = pd.DataFrame(start.analyzers._Kelly.get_analysis())
    cl_df, cl_an_df = start.analyzers._trade_assets.get_analysis()
    benchmark_df,benchmark_an_df = benchmark_analysis(benchmark_df)
    analyzer_df = pd.concat([cl_an_df, benchmark_an_df], axis=0).reset_index()
    # 相对收益率
    xd_df = pd.DataFrame()
    xd_df['交易日期'] = cl_df['交易日期']
    xd_df['标签'] = '相对收益率'
    xd_df['累计收益率'] = np.array(cl_df['累计收益率'])-np.array(benchmark_df['累计收益率'])

    cl_df['标签'] = '策略收益率'
    benchmark_df['标签'] = '沪深300'

    zx_df = pd.concat([cl_df[['交易日期','标签','累计收益率']],benchmark_df[['交易日期','标签','累计收益率']],xd_df], axis=0).reset_index(drop=True)
    zx_df['交易日期'] = zx_df['交易日期'].apply(lambda x: pd.to_datetime(x).date())

    # print('zx_df:',zx_df)
    return zx_df,cl_df[['交易日期','持仓比']],analyzer_df,tl_df

def benchmark_analysis(benchmark_df):
    benchmark_df['交易日期'] = np.vectorize(lambda s: pd.to_datetime(s).date())(benchmark_df.index.to_pydatetime())
    benchmark_df['今日收益率']=benchmark_df['close']/benchmark_df['close'].shift(1)
    benchmark_df['今日收益率']=benchmark_df['今日收益率'].fillna(0)
    benchmark_df['近7天收益率']=benchmark_df['今日收益率'].rolling(window=7,min_periods=1).sum()
    # 这里取 22天交易日
    benchmark_df['近1月收益率']=benchmark_df['今日收益率'].rolling(window=22,min_periods=1).sum()
    benchmark_df['近3月收益率']=benchmark_df['今日收益率'].rolling(window=66,min_periods=1).sum()
    benchmark_df['累计收益率']=(benchmark_df['close']/benchmark_df['close'].iloc[0])-1
    # benchmark_df['累计收益率']=benchmark_df['今日收益率'].cumprod()
    # benchmark_df['累计收益率']=(benchmark_df['今日收益率']+1).cumprod()-1

    # 累计收益走势图
    # 收益统计表
    total_ret=round(benchmark_df['累计收益率'].iloc[-1],2)
    rate_1d=round(benchmark_df['今日收益率'].iloc[-1],2)
    rate_7d=round(benchmark_df['近7天收益率'].iloc[-1],2)
    rate_22d=round(benchmark_df['近1月收益率'].iloc[-1],2)
    rate_66d=round(benchmark_df['近3月收益率'].iloc[-1],2)
    # 年化收益率
    annual_ret = round(pow(1 + benchmark_df['累计收益率'].iloc[-1], 250/len(benchmark_df)) - 1,2)
    max_drawdown_rate = algorithmUtils.max_drawdown(benchmark_df['close'])
    # 夏普比率 表示每承受一单位总风险，会产生多少的超额报酬，可以同时对策略的收益与风险进行综合考虑。可以理解为经过风险调整后的收益率。计算公式如下，该值越大越好
    # 超额收益率以无风险收益率为基准
    # 公认默认无风险收益率为年化3%
    exReturn = benchmark_df['今日收益率'] - 0.03 / 250
    sharperatio = round(np.sqrt(len(exReturn)) * exReturn.mean() / exReturn.std(),2)

    benchmark_an_df = pd.DataFrame([['沪深300', total_ret, annual_ret,None,max_drawdown_rate,sharperatio,rate_1d,rate_7d,rate_22d,rate_66d]],
                                      columns=['策略','累计收益率', '年化收益率', '胜率','最大回撤%', '夏普比率','今日收益率','近7天收益率','近1月收益率','近3月收益率'])

    return benchmark_df.reset_index(drop=True),benchmark_an_df


class trade_assets(bt.Analyzer):
    '''todo 自定义分析器 获取每日总资产 收益率等'''

    def get_analysis(self):
        return self.cl_df, self.cl_an_df

    def start(self):
        super(trade_assets, self).start()
        self.rets = []
        self.cl_df = pd.DataFrame()
        self.cl_an_df = pd.DataFrame()
        self.winProb = None
        self.pnlWins = []
        self.pnlLosses = []

    def next(self):
        super(trade_assets, self).next()
        # 这里总资产会自动3位小数四舍五入 与策略的总资产next获取有小小区别 这个持仓占比好像有点问题
        self.rets.append({'交易日期': pd.to_datetime(self.datas[0].datetime.datetime()).date(), '当前总资产': self.strategy.broker.getvalue(),'持仓比': (self.strategy.broker.getvalue()-self.strategy.broker.getcash())/self.strategy.broker.getvalue()})

    def notify_trade(self, trade):
        if trade.status == trade.Closed:
            if trade.pnlcomm > 0:
                # 盈利加入盈利列表
                self.pnlWins.append(trade.pnlcomm)
            else:
                # 亏损加入亏损列表 利润0算亏损
                self.pnlLosses.append(trade.pnlcomm)

    def stop(self):
        # 计算胜率
        if len(self.pnlWins) > 0 and len(self.pnlLosses) > 0:
            avgWins = average(self.pnlWins)  # 计算平均盈利
            avgLosses = abs(average(self.pnlLosses))  # 计算平均亏损（绝对值）
            winLossRatio = avgWins / avgLosses  # 盈亏比
            if winLossRatio == 0:
                self.winProb = None
            else:
                numberOfWins = len(self.pnlWins)  # 获胜次数
                numberOfLosses = len(self.pnlLosses)  # 亏损次数
                numberOfTrades = numberOfWins + numberOfLosses  # 总交易次数
                self.winProb = numberOfWins / numberOfTrades  # 计算胜率
        else:
            self.winProb = None

        self.cl_df = pd.DataFrame(self.rets)
        self.cl_df['今日收益率'] = self.cl_df['当前总资产']/self.cl_df['当前总资产'].shift(1)
        self.cl_df['今日收益率'] = self.cl_df['今日收益率'].fillna(0)
        self.cl_df['近7天收益率'] = self.cl_df['今日收益率'].rolling(window=7, min_periods=1).sum()
        # 这里取 22天交易日
        self.cl_df['近1月收益率'] = self.cl_df['今日收益率'].rolling(window=22, min_periods=1).sum()
        self.cl_df['近3月收益率'] = self.cl_df['今日收益率'].rolling(window=66, min_periods=1).sum()
        self.cl_df['累计收益率'] = (self.cl_df['当前总资产'] / self.cl_df['当前总资产'].iloc[0]) - 1
        # self.cl_df['累计收益率']=self.cl_df['今日收益率'].cumprod()
        # self.cl_df['累计收益率']=(self.cl_df['今日收益率']+1).cumprod()-1

        # 累计收益走势图
        # 收益统计表
        total_ret = round(self.cl_df['累计收益率'].iloc[-1], 2)
        rate_1d = round(self.cl_df['今日收益率'].iloc[-1], 2)
        rate_7d = round(self.cl_df['近7天收益率'].iloc[-1], 2)
        rate_22d = round(self.cl_df['近1月收益率'].iloc[-1], 2)
        rate_66d = round(self.cl_df['近3月收益率'].iloc[-1], 2)
        # 年化收益率
        annual_ret = round(pow(1 + self.cl_df['累计收益率'].iloc[-1], 250 / len(self.cl_df)) - 1, 2)
        max_drawdown_rate = algorithmUtils.max_drawdown(self.cl_df['当前总资产'])
        # 夏普比率 表示每承受一单位总风险，会产生多少的超额报酬，可以同时对策略的收益与风险进行综合考虑。可以理解为经过风险调整后的收益率。计算公式如下，该值越大越好
        # 超额收益率以无风险收益率为基准
        # 公认默认无风险收益率为年化3%
        exReturn = self.cl_df['今日收益率'] - 0.03 / 250
        sharperatio = round(np.sqrt(len(exReturn)) * exReturn.mean() / exReturn.std(), 2)

        self.cl_an_df = pd.DataFrame([['本策略', total_ret, annual_ret, round(self.winProb * 100, 2), max_drawdown_rate,
                                       sharperatio, rate_1d, rate_7d, rate_22d, rate_66d]],
                                     columns=['策略', '累计收益率', '年化收益率', '胜率', '最大回撤%', '夏普比率', '今日收益率', '近7天收益率',
                                              '近1月收益率',
                                              '近3月收益率'])

def run_cerebro_dash(zx_df,cc_df,analyzer_df,tl_df,strategy_name,start_date,end_date,start_cash,end_cash):
    '''回测结果可视化'''
    app = dash.Dash(__name__)
    # 收益率走势

    # '交易日期', '标签', '累计收益率'
    # 剔除不交易日期
    dt_all = pd.date_range(start_date,end_date)
    dt_all = [pd.to_datetime(d).date() for d in dt_all]
    dt_breaks = list(set(dt_all) - set(zx_df['交易日期']))

    hovertext = []  # 添加悬停信息
    # for date in zx_df['交易日期'].unique(): # <br>表示
    #     hovertext.append('日期: ' + str(date) +
    #                      '<br>策略收益率: ' +str(zx_df.query('标签=="策略收益率" & 交易日期 == @date')['累计收益率'])+
    #                      '<br>沪深300: ' +str(zx_df.query('标签=="沪深300" & 交易日期 == @date')['累计收益率'])+
    #                      '<br>相对收益率: ' +str(zx_df.query('标签=="相对收益率" & 交易日期 == @date')['累计收益率'])
    #                      )
    # for date in zx_df['交易日期'].unique():
    #     hovertext.append({'交易日期': date,
    #                       '策略收益率': zx_df.query('标签=="策略收益率" & 交易日期 == @date')['累计收益率'],
    #                        '沪深300': zx_df.query('标签=="沪深300" & 交易日期 == @date')['累计收益率'],
    #                        '相对收益率': zx_df.query('标签=="相对收益率" & 交易日期 == @date')['累计收益率']
    #                       })

    zx_fig = px.line(
        zx_df,  # 绘图数据
        x=zx_df['交易日期'],  # x轴标签
        y=zx_df['累计收益率'],
        color='标签',
        color_discrete_sequence=['#FF0000', '#2776B6', '#8F4E4F'],
        hover_data={'标签': False}
    )

    # zx_fig.add_trace(go.Scatter(x=zx_df.query('标签=="策略收益率"')['交易日期'], y=zx_df.query('标签=="策略收益率"')['累计收益率'],mode='lines',marker_color='#FF0000', name='策略收益率'))
    # zx_fig.add_trace(go.Scatter(x=zx_df.query('标签=="沪深300"')['交易日期'], y=zx_df.query('标签=="沪深300"')['累计收益率'],mode='lines',marker_color='#7ABDFF', name='沪深300'))
    # zx_fig.add_trace(go.Scatter(x=zx_df.query('标签=="相对收益率"')['交易日期'], y=zx_df.query('标签=="相对收益率"')['累计收益率'],mode='lines',marker_color='#333333', name='相对收益率'))
    zx_fig.update_xaxes(
        dtick="D1",
        tickformat='%Y-%m-%d',  # 日期显示模式
        ticklabelmode='instant',  # ticklabelmode模式：居中 'instant', 'period'
        # rangeslider_visible=True, #开启范围滑块
        rangebreaks=[dict(values=dt_breaks)],# 去除休市的日期，保持连续
        rangeselector=dict(
            # 增加固定范围选择
            buttons=list([
                dict(count=1, label='1M', step='month', stepmode='backward'),
                dict(count=6, label='6M', step='month', stepmode='backward'),
                dict(count=1, label='1Y', step='year', stepmode='backward'),
                dict(count=1, label='YTD', step='year', stepmode='todate'),
                dict(step='all')]))
    )
    # Do not show OHLC's rangeslider plot
    # zx_fig.update(layout_xaxis_rangeslider_visible=False)
    zx_fig.update_layout(xaxis_title=None, yaxis_title=None, legend_title_text=None,legend=dict(orientation="h", yanchor="bottom",y=1.02,xanchor="right",x=1))

    # 绘制持仓比
    cc_fig = px.line(
        cc_df,  # 绘图数据
        x=cc_df['交易日期'],  # x轴标签
        y=cc_df['持仓比'],
        hover_data={'交易日期': "|%Y-%m-%d"},  # 悬停信息设置
        color_discrete_sequence=['#66FF99']
    )
    cc_fig.update_xaxes(tickformat='%Y-%m-%d',rangebreaks=[dict(values=dt_breaks)],visible=False, fixedrange=True)
    cc_fig.update_yaxes(fixedrange=True)
    cc_fig.update_layout(xaxis_title=None,margin=dict(t=10,l=10,b=10,r=10), width=1447,height=80)

    # cc_fig = make_subplots(rows=2, cols=1, shared_xaxes=True,vertical_spacing=0.02, subplot_titles=('', '持仓比'),row_width=[0.2, 0.7])
    # cc_fig.add_trace(go.Line(x=cc_df['交易日期'], y=cc_df['持仓比'], showlegend=True), row=2, col=1)
    # cc_fig.update_xaxes(dtick="D1",tickformat='%Y-%m-%d',rangebreaks=[dict(values=dt_breaks)])
    # cc_fig.update_layout(xaxis_title=None, yaxis_title=None, legend_title_text=None,legend=dict(orientation="h", yanchor="bottom",y=1.02,xanchor="right",x=1))

    # todo 交易详情 分页
    tl_df.sort_index(ascending=False,inplace=True)
    app.layout = html.Div(
        [
            html.H3(
                children='{}策略评估'.format(strategy_name),
                style=dict(textAlign='center', color='black')),
            html.Div(
                children='回测日期：{} ~ {}  期初资金：¥{}    期末资金：¥{}    持股周期：2天     买入规则：排名<=3'.format(start_date,end_date,start_cash,end_cash),
                style=dict(textAlign='center', color='black')),
            html.H4('累计收益率走势'),
            dcc.Graph(figure=zx_fig, style={'margin-top': '-22px'}),
            dcc.Graph(figure=cc_fig, style={'margin-top': '-62px'}),
            html.H4(children='收益统计', style={'margin-top': '-2px'}),
            dash_table.DataTable(
                id='收益统计table',
                data=analyzer_df.to_dict('records'),
                columns=[
                    {'name': column, 'id': column}
                    for column in analyzer_df.drop(['index'], axis=1).columns
                ],
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
                        } for col in ['累计收益率', '年化收益率', '夏普比率','今日收益率','近7天收益率','近1月收益率','近3月收益率']
                    ] +
                    [
                        {
                            'if': {
                                'filter_query': '{{{}}} < 0'.format(col),
                                'column_id': col
                            },
                            'color': '#008000'
                        } for col in ['累计收益率', '年化收益率', '夏普比率','今日收益率','近7天收益率','近1月收益率','近3月收益率']
                    ]
                ),
                style_header={
                    'font-family': 'Times New Romer',
                    'font-weight': 'bold',
                    'font-size': 11,
                    'text-align': 'center',
                    'backgroundColor': 'rgb(210, 210, 210)',
                    'color': 'black',
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
            html.H4('交易详情'),
            # dcc.Graph(figure=tl_fig),
            dbc.Container(
                [
                    dbc.Spinner(
                        dash_table.DataTable(
                            id='dash-table',
                            columns=[
                                {'name': column, 'id': column}
                                for column in tl_df.columns
                            ],
                            page_size=15,  # 设置单页显示15行记录行数
                            page_action='custom',
                            page_current=0,
                            sort_action='custom',
                            sort_mode='multi',
                            style_data_conditional=[
                                {
                                    'if': {'row_index': 'odd'},
                                    'backgroundColor': 'rgb(220, 220, 220)',
                                }
                            ],
                            export_format='xlsx',
                            style_header={
                                'font-family': 'Times New Romer',
                                'font-weight': 'bold',
                                'font-size': 11,
                                'text-align': 'center',
                                'backgroundColor': 'rgb(210, 210, 210)',
                                'color': 'black',
                            },
                            style_data={
                                'whiteSpace': 'normal',
                                'font-family': 'Times New Romer',
                                'font-size': 11,
                                'text-align': 'center',
                                'color': 'black',
                                'backgroundColor': 'white'
                            }
                        )
                    )
                ],
                style={
                    'margin-top': '-15px'
                }
            )
        ]
    )

    @app.callback(
        [Output('dash-table', 'data'),
         Output('dash-table', 'page_count')],
        [Input('dash-table', 'page_current'),
         Input('dash-table', 'page_size'),
         Input('dash-table', 'sort_by')]
    )
    def refresh_page_data(page_current, page_size, sort_by):
        if sort_by:
            return (
                tl_df.sort_values(
                    [col['column_id'] for col in sort_by],
                    ascending=[
                        col['direction'] == 'asc'
                        for col in sort_by
                    ]
                ).iloc[page_current * page_size:(page_current + 1) * page_size].to_dict('records'),
                1 + tl_df.shape[0] // page_size
            )
        return tl_df.iloc[page_current * page_size:(page_current + 1) * page_size].to_dict('records'), 1 + tl_df.shape[0] // page_size
    # host设置为0000 为了主机能访问 虚拟机的web服务
    # http://hadoop102:8000/
    app.run(host='0.0.0.0', port='8000', debug=True)


class trade_list(bt.Analyzer):
    '''todo 自定义分析器 用于查看每笔交易盈亏情况'''
    # https://blog.csdn.net/qq_26742269/article/details/123051695
    def get_analysis(self):
        return self.trades

    def __init__(self):
        self.trades = []
        self.cumprofit = 0.0

    def notify_trade(self, trade):
        if trade.isclosed:
            brokervalue = self.strategy.broker.getvalue()
            dir = 'short'
            if trade.history[0].event.size > 0: dir = 'long'
            pricein = trade.history[len(trade.history) - 1].status.price
            priceout = trade.history[len(trade.history) - 1].event.price
            datein = bt.num2date(trade.history[0].status.dt)
            dateout = bt.num2date(trade.history[len(trade.history) - 1].status.dt)
            if trade.data._timeframe >= bt.TimeFrame.Days:
                datein = datein.date()
                dateout = dateout.date()

            pcntchange = 100 * priceout / pricein - 100
            pnl = trade.history[len(trade.history) - 1].status.pnlcomm
            pnlpcnt = 100 * pnl / brokervalue
            # barlen = trade.history[len(trade.history) - 1].status.barlen
            barlen = trade.history[len(trade.history) - 1].status.barlen+1
            pbar = pnl / barlen
            self.cumprofit += pnl

            size = value = 0.0
            for record in trade.history:
                if abs(size) < abs(record.status.size):
                    size = record.status.size
                    value = record.status.value

            highest_in_trade = max(trade.data.high.get(ago=0, size=barlen + 1))
            lowest_in_trade = min(trade.data.low.get(ago=0, size=barlen + 1))
            hp = 100 * (highest_in_trade - pricein) / pricein
            lp = 100 * (lowest_in_trade - pricein) / pricein
            if dir == 'long':
                mfe = hp
                mae = lp
            if dir == 'short':
                mfe = -lp
                mae = -hp

            # self.trades.append({'ref': trade.ref, 'ticker': trade.data._name, 'dir': dir,
            #                     'datein': datein, 'pricein': pricein, 'dateout': dateout, 'priceout': priceout,
            #                     'chng%': round(pcntchange, 2), 'pnl': pnl, 'pnl%': round(pnlpcnt, 2),
            #                     'size': size, 'value': value, 'cumpnl': self.cumprofit,
            #                     'nbars': barlen, 'pnl/bar': round(pbar, 2),
            #                     'mfe%': round(mfe, 2), 'mae%': round(mae, 2)})

            self.trades.append({'订单': trade.ref, '股票': trade.data._name,
                                '买入日期': datein, '买价': round(pricein, 2), '卖出日期': dateout, '卖价': round(priceout, 2),
                                '收益率%': round(pcntchange, 2), '利润': round(pnl, 2), '利润总资产比%': round(pnlpcnt, 2),
                                '股数': size, '股本': round(value, 2), '仓位比%': round(value/brokervalue*100, 2),'累计收益': round(self.cumprofit, 2),
                                '最大利润%': round(mfe, 2), '最大亏损%': round(mae, 2)})

class Kelly(bt.Analyzer):
    '''关于凯利公式
kelly公式得到的凯利比率，实际上并不是对策略的绩效的评价，那么它起什么作用呢：
如果凯利比率为负，则说明策略是亏损的，不能采用。如果kelly比率（kellyRatio）为正数，比如kellyRatio=0.215
，那么说明，理论上每次下单时，购买金额应该为当时总现金值的 kellyPercent 即 21.5%。
K = W - (1 - W) / R
K: 凯利最优比率
W: 胜率
R: 盈亏比，即平均盈利除以平均损失
    '''
    def get_analysis(self):
        return self.rets

    def __init__(self):
        self.rets = []
        self.pnlWins = []
        self.pnlLosses = []

    def notify_trade(self, trade):
        if trade.status == trade.Closed:
            if trade.pnlcomm > 0:
                # 盈利加入盈利列表
                self.pnlWins.append(trade.pnlcomm)
            else:
                # 亏损加入亏损列表 利润0算亏损
                self.pnlLosses.append(trade.pnlcomm)

    def stop(self):
        if len(self.pnlWins) > 0 and len(self.pnlLosses) > 0:
            avgWins = average(self.pnlWins)  # 计算平均盈利
            avgLosses = abs(average(self.pnlLosses))  # 计算平均亏损（绝对值）
            winLossRatio = avgWins / avgLosses  # 盈亏比
            if winLossRatio == 0:
                # kellyPercent = None
                print('凯利比率为空！！！')
            else:
                numberOfWins = len(self.pnlWins)  # 获胜次数
                numberOfLosses = len(self.pnlLosses)  # 亏损次数
                numberOfTrades = numberOfWins + numberOfLosses  # 总交易次数
                winProb = numberOfWins / numberOfTrades  # 计算胜率
                inverse_winProb = 1 - winProb
                # 计算凯利比率，即每次交易投入资金占总资金 占 总资金 的最优比率
                kellyPercent = winProb - (inverse_winProb / winLossRatio)
                self.rets.append({'凯利比率': kellyPercent,'胜率': winProb,'盈利次数': numberOfWins, '亏损次数': numberOfLosses,'总交易次数': numberOfTrades})
        else:
            # kellyPercent = None  # 信息不足
            print('凯利比率为空！！！')