import os
import sys
from datetime import date, datetime
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

# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import get_spark

app = dash.Dash(__name__)
appName = os.path.basename(__file__)

def work():
    open_data = [133.0, 133.3, 133.5, 133.0, 134.1]
    high_data = [133.1, 133.3, 133.6, 133.2, 134.8]
    low_data = [132.7, 132.7, 132.8, 132.6, 132.8]
    close_data = [133.0, 132.9, 133.3, 133.1, 133.1]

    # 绘图的5个日期：指定年、月、日
    dates = [datetime(year=2019, month=10, day=10),
             datetime(year=2019, month=11, day=10),
             datetime(year=2019, month=12, day=10),
             datetime(year=2020, month=1, day=10),
             datetime(year=2020, month=2, day=10)]

    # 绘图：传入时间和数据
    fig = go.Figure(data=[go.Ohlc(
        x=dates,
        open=open_data,
        high=high_data,
        low=low_data,
        close=close_data)])

    app.layout = html.Div(
        [
            html.H1('嵌入plotly图表'),
            dcc.Graph(figure=fig)
        ]
    )

    # fig.show()
    # py.plot(fig, filename='filename.html')
    print('{}：执行完毕！！！'.format(appName))



if __name__ == '__main__':
    work()
    # http://hadoop102:8000/
    app.run(host='0.0.0.0',port='8000',debug=True)
