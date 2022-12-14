import os
import sys
import dash
import pandas as pd
import dash
from dash import dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import dash_table
from dash.dash_table.Format import Format, Symbol
from flask_caching import Cache
import plotly.express as px
from dash.dependencies import Input, Output
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

app = dash.Dash(__name__)
appName = os.path.basename(__file__)


fig = px.scatter(x=range(10), y=range(10))
df = pd.DataFrame({"A": [-5, 3, 3, 0],
                   "B": [11, -2, 0, -3],
                   "C": [-31, 0, -4, 1]})


columns = [
    dict(id='A', name='A'),
    dict(id='B', name='B'),
    dict(id='C', name='C', type='numeric', format=dict(locale=dict(symbol=['', '%']), specifier='$'))
]

app.layout = html.Div(
    [
        html.H1('嵌入plotly图表'),
        dcc.Graph(figure=fig),
        html.H4(children='收益统计', style={'margin-top': '-2px'}),
        dash_table.DataTable(
            id='收益统计table',
            data=df.to_dict('records'),
            # columns=[
            #     {'name': column, 'id': column}
            #     for column in df.columns
            # ],
            columns=columns,
            style_data_conditional=(
                    [
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': 'rgb(220, 220, 220)',
                        }
                    ]+
                    [
                        {
                            'if': {
                                'filter_query': '{{{}}} > 0'.format(col),
                                'column_id': col
                            },
                            'color': '#ff0000'
                        } for col in ['B','C']
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
        )
    ]
)

print('{}：执行完毕！！！'.format(appName))


#  python /opt/code/pythonstudy_space/01_base/12_plotly.py
if __name__ == '__main__':
    # host设置为0000 为了主机能访问 虚拟机的web服务
    # http://hadoop102:8000/
    app.run(host='0.0.0.0',port='8000',debug=True)
