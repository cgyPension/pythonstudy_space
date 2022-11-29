import pandas as pd
import dash
import trader
import trader_func
import stock_data
from dash import dash_table
from dash import Input,Output
from dash import html,dcc
from datetime import datetime
from PIL import Image
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as fg
import analysis
import akshare as ak
now_date_str=str(datetime.now())[:10]
now_date=''.join(str(datetime.now())[:10].split('-'))
all_code=stock_data.get_all_stock_code()
code_dict=dict(zip(all_code['代码'].tolist(),all_code['名称'].tolist()))
app=dash.Dash(__name__)
app.layout=html.Div([
    html.H2('MACD交易策略'),
    html.Tbody(
        html.Tr([
            html.Td('交易策略',style={'border':'1px solid','width':'200px','font-size':20}),
            html.Td('股票代码',style={'border':'1px solid','width':'200px','font-size':20}),
            html.Td('数据周期',style={'border':'1px solid','width':'200px','font-size':20}),
            html.Td('交易手数(股)',style={'border':'1px solid','width':'200px','font-size':20}),
            html.Td('开始资金',style={'border':'1px solid','width':'200px','font-size':20}),
            html.Td('开始时间',style={'border':'1px solid','width':'200px','font-size':20}),
            html.Td('结束时间',style={'border':'1px solid','width':'200px','font-size':20}),
            html.Td('回测按钮',style={'border':'1px solid','width':'200px','font-size':20}),
            html.Td('显示结果按钮',style={'border':'1px solid','width':'200px','font-size':20})
        ])
    ),
    html.Tbody(
        html.Tr([
            html.Td(dcc.Dropdown(options={'MACD':'MACD'},value='MACD',id='trader_type'),
                    style={'border':'1px solid','width':'200px'}),
            html.Td(dcc.Dropdown(options=code_dict,value='600031',id='stock'),
                    style={'border':'1px solid','width':'200px'}),
            html.Td(dcc.Dropdown(options={'日线':'日线','1分钟':'1分钟','5分钟':'5分钟','15分钟':'15分钟','30分钟':'30分钟','60分钟':'60分钟'},
                                 value='日线',id='data_period'),style={'border':'1px solid','width':'200px'}),
            html.Td(dcc.Input(value=1000,id='amount'),style={'border':'1px solid','width':'200px'}),
            html.Td(dcc.Input(value=1000000,id='start_cash'),style={'border':'1px solid','width':'200px'}),
            html.Td(dcc.DatePickerSingle(date='20210101',id='start_date'),style={'border':'1px solid','width':'200px'}),
            html.Td(dcc.DatePickerSingle(date=now_date,id='end_date',),style={'border':'1px solid','width':'200px'}),
            html.Td(dcc.RadioItems(options={'回测':'回测','不回测':'不回测'},value='不回测',id='run_hc'),
                    style={'border':'1px solid','width':'200px'}),
            html.Td(dcc.RadioItems(options={'显示结果':'显示结果','不显示结果':'不显示结果'},value='不显示结果',id='show_result'),
                    style={'border':'1px solid','width':'200px'})
        ])
    ),
    html.Img(id='figure_result'),
    dash_table.DataTable(
        id='show_table',
        page_size=10,
        style_table={'font-size': 18},
        sort_action='native'
    ),
    #Image.open('结果.jpg')
    dcc.Graph(id='figure_1'),
    #收益对比
    dcc.Graph(id='figure_2'),
    dcc.Graph(id='figure_3'),
    dcc.Graph(id='figure_4'),
])
#回调回测
@app.callback(
    Output(component_id='figure_result',component_property='src'),
    Input(component_id='trader_type',component_property='value'),
    Input(component_id='stock',component_property='value'),
    Input(component_id='data_period',component_property='value'),
    Input(component_id='amount',component_property='value'),
    Input(component_id='start_cash',component_property='value'),
    Input(component_id='start_date',component_property='date'),
    Input(component_id='end_date',component_property='date'),
    Input(component_id='run_hc',component_property='value'),
)
def update_tun_hc_data(trader_type,stock,data_period,amount,start_cash,start_date,end_date,run_hc):
    if run_hc=='回测':
        if stock[0]=='6':
            stock='sh'+stock
        else:
            stock='sz'+stock
        start_date=''.join(str(start_date)[:10].split('-'))
        end_date = ''.join(str(end_date)[:10].split('-'))
        #选择回测类型MACD
        trader.trader_func_name=trader_type
        #运行
        trader.run_data(code=stock,start_date=start_date,end_date=end_date,start_cash=float(start_cash),mount=float(amount),comm=0.0)
        src=Image.open(r'结果.jpg')
        return src
#显示表格
@app.callback(
    Output(component_id='show_table',component_property='data'),
    Input(component_id='show_result',component_property='value')
)
def update_show_table(show_result):
    if show_result=='显示结果':
        df=analysis.analysis_base_indictors()
        src = Image.open(r'结果.jpg')
        return df.to_dict('records')
#显示image
@app.callback(
    Output(component_id='figure_result',component_property='width'),
    Input(component_id='show_result',component_property='value'),
    Input(component_id='run_hc',component_property='value'),
)
def update_show_image_windth(show_result,run_hc):
    if show_result=='显示结果':
        return 0
    elif run_hc=='回测':
        return 1400
#显示image
@app.callback(
    Output(component_id='figure_result',component_property='height'),
    Input(component_id='show_result',component_property='value'),
    Input(component_id='run_hc', component_property='value'),
)
def update_show_image_height(show_result,run_hc):
    if show_result=='显示结果':
        return 0
    elif run_hc=='回测':
        return 700
#显示股票图
@app.callback(
    Output(component_id='figure_1',component_property='figure'),
    Input(component_id='stock',component_property='value'),
    Input(component_id='start_date',component_property='date'),
    Input(component_id='end_date',component_property='date'),
    Input(component_id='show_result',component_property='value')
)
def update_figure_1(stock,start_date,end_date,show_result):
    if show_result == '显示结果':
        if stock[0]=='6':
            stock='sh'+stock
        else:
            stock='sz'+stock
        start_date=''.join(str(start_date)[:10].split('-'))
        end_date = ''.join(str(end_date)[:10].split('-'))
        df=ak.stock_zh_a_daily(symbol=stock,start_date=start_date,end_date=end_date)
        df['date']=pd.to_datetime(df['date'])
        df.index=df['date']
        fig=go.Figure(data=go.Candlestick(x=df['date'],open=df['open'],close=df['close'],low=df['open'],high=df['high']))
        return fig
    else:
        fig=px.line(x=[1,1],y=[2,2])
        return fig
@app.callback(
    Output(component_id='figure_2',component_property='figure'),
    Input(component_id='show_result', component_property='value')
)
def update_figure_2(show_result):
    if show_result == '显示结果':
        df=pd.read_excel(r'分析数据\交易数据.xlsx')
        data=pd.DataFrame()
        data['date']=df['datetime']
        data['策略收益']=df['value'].pct_change().cumsum().tolist()
        data['基准收益']=df['index_close'].pct_change().cumsum().tolist()
        data['策略收益']=data['策略收益']*100
        data['基准收益']=data['基准收益']*100
        fig=px.line(data_frame=data,x='date',y=['策略收益','基准收益'],title='收益率对比%')
        return fig
    else:
        fig=px.line(x=[1,1],y=[2,2])
        return fig
#毛收益
@app.callback(
    Output(component_id='figure_3',component_property='figure'),
    Input(component_id='show_result', component_property='value')
)
def update_figure_2(show_result):
    if show_result == '显示结果':
        df=pd.read_excel(r'分析数据\交易数据.xlsx')
        data=pd.DataFrame()
        data['date'] = df['datetime']
        data['毛收益']=df['pnlplus'].cumsum().fillna(method='bfill').tolist()
        data['净收益']=df['pnlminus'].cumsum().fillna(method='bfill').tolist()
        fig=px.line(data_frame=data,x='date',y=['毛收益','净收益'],title='累计收益')
        return fig
    else:
        fig = px.line(x=[1, 1], y=[2, 2])
        return fig
#账户变化
@app.callback(
    Output(component_id='figure_4',component_property='figure'),
    Input(component_id='show_result', component_property='value')
)
def update_figure_2(show_result):
    if show_result == '显示结果':
        df=pd.read_excel(r'分析数据\交易数据.xlsx')
        data=pd.DataFrame()
        data['date'] = df['datetime']
        data['账户现金']=df['cash'].tolist()
        data['账户价值']=df['value'].tolist()
        fig = px.line(data_frame=data, x='date', y=['账户现金', '账户价值'], title='账户变化')
        return fig
    else:
        fig = px.line(x=[1, 1], y=[2, 2])
        return fig
if __name__=='__main__':
    app.run_server(debug=True)