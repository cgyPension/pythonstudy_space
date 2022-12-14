import os
import sys
from datetime import date
import akshare as ak
import dash
import dash_html_components as html
import pandas as pd
from dash import dcc
import plotly.express as px
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.offline as py
from plotly.subplots import make_subplots
#使用Spark中的函数，例如 round、sum 等
from pyspark.sql.functions import *
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import get_spark

app = dash.Dash(__name__)
appName = os.path.basename(__file__)
def get_stock_price(stock_code, start_date, end_date):
    # 获取个股行情数据
    # 本地模式
    spark = get_spark(appName)
    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()
    sql = """
            select *
            from stock.dwd_stock_quotes_di
            where td between '%s' and '%s'
                and stock_code = '%s'
    """ % (start_date,end_date,stock_code)

    spark_df = spark.sql(sql)
    pd_df = spark_df.toPandas()
    return pd_df


def get_trading_date():
    # 获取市场的交易时间
    trade_date = ak.tool_trade_date_hist_sina()['trade_date']
    trade_date = [d.strftime("%Y-%m-%d") for d in trade_date]
    return trade_date


def plot_cand_volume(data, dt_breaks):
    # Create subplots and mention plot grid size
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.03, subplot_titles=('', '成交量'),
                        row_width=[0.2, 0.7])

    # 绘制k数据
    fig.add_trace(go.Candlestick(x=data["date"], open=data["open"], high=data["high"],
                                 low=data["low"], close=data["close"], name=""),
                  row=1, col=1
                  )

    # 绘制成交量数据
    fig.add_trace(go.Bar(x=data['date'], y=data['volume'], showlegend=False), row=2, col=1)


    fig.update_xaxes(
        title_text='股票K线图', # 标题
        rangeslider_visible=True,  # 下方滑动条缩放
        rangeselector=dict(
            # 增加固定范围选择
            buttons=list([
                dict(count=1, label='1M', step='month', stepmode='backward'),
                dict(count=6, label='6M', step='month', stepmode='backward'),
                dict(count=1, label='1Y', step='year', stepmode='backward'),
                dict(count=1, label='YTD', step='year', stepmode='todate'),
                dict(step='all')])))

    # Do not show OHLC's rangeslider plot
    fig.update(layout_xaxis_rangeslider_visible=False)
    # 去除休市的日期，保持连续
    fig.update_xaxes(rangebreaks=[dict(values=dt_breaks)])
    return fig


def get_n_stock_price(stock_code, start_date, end_date):
    # 获取个股行情数据
    # 本地模式
    spark = get_spark(appName)
    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()
    sql = """
    select trade_date,
           stock_code,
           close_price
    from stock.dwd_stock_quotes_di
    where td between '2021-01-01' and '2022-11-30'
        and stock_code in ('sh600006','sz300506','sh600018')
    """
    # % (start_date, end_date, stock_code)
    # sparksql in 里面不能写子查询 要算所有都转只能用pivot的算子 行转列
    # spark_df = spark.sql(sql).groupBy('trade_date').pivot('stock_code').agg(sum('close_price'))
    spark_df = spark.sql(sql)
    # 行列转换
    # pd_df = spark_df.toPandas().pivot(index='trade_date', columns='stock_code',values='close_price')
    pd_df = spark_df.toPandas()
    return pd_df


def work(stock_code, start_date, end_date):
    data = get_stock_price(stock_code, start_date, end_date)
    data = data.rename(columns={'trade_date': 'date', 'open_price': 'open', 'close_price': 'close', 'high_price': 'high', 'low_price': 'low'})
    trade_date = get_trading_date()
    # 剔除不交易日期
    dt_all = pd.date_range(start=data['date'].iloc[0], end=data['date'].iloc[-1])
    dt_all = [d.strftime("%Y-%m-%d") for d in dt_all]
    dt_breaks = list(set(dt_all) - set(trade_date))
    # 绘制 复杂k线图
    fig = plot_cand_volume(data, dt_breaks)
    # 走势图
    # fig = plot_trend(data, dt_breaks)
    app.layout = html.Div(
        [
            html.H1('k线图'),
            dcc.Graph(figure=fig)
        ]
    )

    # fig.show()
    # py.plot(fig, filename='filename.html')
    print('{}：执行完毕！！！'.format(appName))

def work2(stock_code, start_date, end_date):
    data = get_n_stock_price(stock_code, start_date, end_date)
    # fig1 = go.Figure([go.Scatter(
    #     x=data.index,
    #     y=data['sh600006'])])
    #
    # fig1.update_layout(title={
    #     'text': '股票走势折线图',
    #     'x': 0.52,
    #     'y': 0.96,
    #     'xanchor': 'center',
    #     'yanchor': 'top'
    # })


    # fig2 = px.line(
    #     data,  # 绘图数据
    #     x=data.index,  # x轴标签
    #     y=data.columns,
    #     # hover_data={'date': "|%B %d, %Y"},  # 悬停信息设置
    #     title='标签个性化设置-居中'  # 图标题
    # )
    #
    # fig2.update_xaxes(
    #     dtick="M1",  # 表示one month：每个月显示一次
    #     tickformat="%b\n%Y",  # 日期显示模式
    #     ticklabelmode='instant'  # ticklabelmode模式：居中 'instant', 'period'
    # )
    #
    fig3 = px.line(
        data,  # 绘图数据
        x=data.trade_date,  # x轴标签
        y=data.close_price,
        color='stock_code',
        # hover_data={'date': "|%B %d, %Y"},  # 悬停信息设置
        title='标签个性化设置-居中'  # 图标题
    )

    fig3.update_xaxes(
        # dtick="M1",  # 表示one month：每个月显示一次 "%b\n%Y"
        dtick="D1",  # 表示one month：每个月显示一次
        tickformat='%Y-%m-%d',  # 日期显示模式
        ticklabelmode='instant',  # ticklabelmode模式：居中 'instant', 'period'
        rangeslider_visible=True #开启范围滑块
    )

    app.layout = html.Div(
        [
            html.H1('股票走势图'),
            # dcc.Graph(figure=fig1),
            dcc.Graph(figure=fig3)
        ]
    )

    print('{}：执行完毕！！！'.format(appName))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/stock_plotly_k.py
if __name__ == '__main__':
    work2('sz000681','20220101', '20221130')
    # host设置为0000 为了主机能访问 虚拟机的web服务
    # http://hadoop102:8000/
    # app.run(host='0.0.0.0',port='7777',debug=True)
    app.run(host='0.0.0.0',port='7777')
    # df = get_n_stock_price('sz000681','20220101', '20221130')
