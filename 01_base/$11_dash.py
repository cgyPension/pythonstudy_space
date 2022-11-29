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

# app.layout = html.H1('第一个Dash应用！')

# app.layout = html.Div(
#     [
#         html.H1('标题1'),
#         html.H1('标题2'),
#         html.P(['测试', html.Br(), '测试']),
#         html.Table(
#             html.Tr(
#                 [
#                     html.Td('第一列'),
#                     html.Td('第二列')
#                 ]
#             )
#         )
#     ]
# )


# app.layout = html.Div(
#     [
#         html.H1('下拉选择'),
#         html.Br(),
#         dcc.Dropdown(
#             options=[
#                 {'label': '选项一', 'value': 1},
#                 {'label': '选项二', 'value': 2},
#                 {'label': '选项三', 'value': 3}
#             ]
#         )
#     ]
# )


# fig = px.scatter(x=range(10), y=range(10))
# app.layout = html.Div(
#     [
#         html.H1('嵌入plotly图表'),
#         dcc.Graph(figure=fig)
#     ]
# )


# app.layout = html.Div(
#     [
#         html.H1('根据省名查询省会城市：'),
#         html.Br(),
#         dcc.Dropdown(
#             id='province',
#             options=[
#                 {'label': '四川省', 'value': '四川省'},
#                 {'label': '陕西省', 'value': '陕西省'},
#                 {'label': '广东省', 'value': '广东省'}
#             ],
#             value='四川省'
#         ),
#         html.P(id='city')
#     ]
# )
#
# province2city_dict = {
#     '四川省': '成都市',
#     '陕西省': '西安市',
#     '广东省': '广州市'
# }
#
# @app.callback(Output('city', 'children'),
#               Input('province', 'value'))
# def province2city(province):
#     return province2city_dict[province]


# 监听图表交互式选择行为
fig = px.scatter(x=range(10), y=range(10), height=400)
fig.update_layout(clickmode='event+select')  # 设置点击模式

app.layout = html.Div(
    [
        dcc.Graph(figure=fig, id='scatter'),
        html.Hr(),
        html.Div([
            '悬浮事件：',
            html.P(id='hover')
        ]),
        html.Hr(),
        html.Div([
            '点击事件：',
            html.P(id='click')
        ]),
        html.Hr(),
        html.Div([
            '选择事件：',
            html.P(id='select')
        ]),
        html.Hr(),
        html.Div([
            '框选事件：',
            html.P(id='zoom')
        ])
    ]
)


# 多对多的回调函数
@app.callback([Output('hover', 'children'),
               Output('click', 'children'),
               Output('select', 'children'),
               Output('zoom', 'children'),],
              [Input('scatter', 'hoverData'),
               Input('scatter', 'clickData'),
               Input('scatter', 'selectedData'),
               Input('scatter', 'relayoutData')])
def listen_to_hover(hoverData, clickData, selectedData, relayoutData):
    return str(hoverData), str(clickData), str(selectedData), str(relayoutData)

if __name__ == '__main__':
    # host设置为0000 为了主机能访问 虚拟机的web服务
    app.run(host='0.0.0.0',port='8000',debug=True)