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
def get_stock_price():
    # 获取个股行情数据
    # 本地模式
    spark = get_spark(appName)
    sql = """
    select trade_date,
           stock_code,
           close_price
    from stock.dwd_stock_quotes_di
    where td between '2022-01-01' and '2022-11-30'
        and stock_code in ('sh600006','sz300506','sh600018')
    """
    spark_df = spark.sql(sql)
    pd_df = spark_df.toPandas().sort_values(by=['trade_date'], ascending=True)
    return pd_df


def get_trading_date():
    # 获取市场的交易时间
    # trade_date = ak.tool_trade_date_hist_sina()['trade_date']
    df = ak.tool_trade_date_hist_sina()
    trade_date = df['trade_date'].apply(lambda x: pd.to_datetime(x).date())
    return trade_date

def work():
    data = get_stock_price()
    trade_date = get_trading_date()
    # 剔除不交易日期
    # print(trade_date)
    dt_all = pd.date_range(start=data['trade_date'].iloc[0], end=data['trade_date'].iloc[-1])
    dt_all = [pd.to_datetime(d).date() for d in dt_all]
    dt_breaks = list(set(dt_all) - set(trade_date))

    fig = px.line(
        data,  # 绘图数据
        x=data.trade_date,  # x轴标签
        y=data.close_price,
        color='stock_code',
        color_discrete_sequence=['#FF0000', '#2776B6', '#8F4E4F'],
        hover_data={'trade_date': "|%Y-%m-%d"}
    )

    fig.update_xaxes(
            ticklabelmode='instant',  # ticklabelmode模式：居中 'instant', 'period'
            rangeslider_visible=True, #开启范围滑块
            tickformatstops=[
                # dict(dtickrange=[None, 1000], value="%H:%M:%S.%L ms"),
                # dict(dtickrange=[1000, 60000], value="%H:%M:%S s"),
                # dict(dtickrange=[60000, 3600000], value="%H:%M m"),
                # dict(dtickrange=[3600000, 86400000], value="%H:%M h"),
                # dict(dtickrange=[86400000, 604800000], value="%e. %b d"),
                # dict(dtickrange=[604800000, "M1"], value="%e. %b w"),
                dict(dtickrange=[3600000, "M1"], value='%Y-%m-%d'),
                dict(dtickrange=["M1", "M12"], value='%Y-%m'),
                dict(dtickrange=["M12", None], value='%Y')
            ],
        rangeselector=dict(
            # 增加固定范围选择
            buttons=list([
                dict(count=1, label='1M', step='month', stepmode='backward'),
                dict(count=6, label='6M', step='month', stepmode='backward'),
                dict(count=1, label='1Y', step='year', stepmode='backward'),
                dict(count=1, label='YTD', step='year', stepmode='todate'),
                dict(step='all')])),
        rangebreaks=[dict(values=dt_breaks)] # 去除休市的日期
    )
    fig.update_yaxes(ticksuffix='%',side='right',ticklabelposition='inside')
    fig.update_layout(xaxis_title=None, yaxis_title=None, legend_title_text=None,legend=dict(orientation="h", yanchor="bottom",y=1.02,xanchor="right",x=1))


    app.layout = html.Div(
        [
            html.H1('股票走势图'),
            dcc.Graph(figure=fig)
        ]
    )
    print('{}：执行完毕！！！'.format(appName))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/test_k.py
if __name__ == '__main__':
    work()
    # host设置为0000 为了主机能访问 虚拟机的web服务
    # http://hadoop102:8000/
    app.run(host='0.0.0.0',port='7777',debug=True)

