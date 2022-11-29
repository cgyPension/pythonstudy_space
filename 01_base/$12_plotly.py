import os
import sys
import dash
import dash_html_components as html
from dash import dcc
import plotly.express as px
import dash_core_components as dcc
from dash.dependencies import Input, Output
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

app = dash.Dash(__name__)
appName = os.path.basename(__file__)


fig = px.scatter(x=range(10), y=range(10))
app.layout = html.Div(
    [
        html.H1('嵌入plotly图表'),
        dcc.Graph(figure=fig)
    ]
)
print('{}：执行完毕！！！'.format(appName))



if __name__ == '__main__':
    # host设置为0000 为了主机能访问 虚拟机的web服务
    app.run(host='0.0.0.0',port='8000',debug=True)
