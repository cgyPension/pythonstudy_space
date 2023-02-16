import os
import sys
# 在linux会识别不了包 所以要加临时搜索目
import numpy as np

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from datetime import date
import datetime
import pandas as pd
import dash
import dash_html_components as html
import pandas as pd
from dash import dcc
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
from back_stockui.UiDataUtils import *
from itertools import product

class UiPlotly:
    """Plotly 画图"""
    def __init__(self):
        self.appName = os.path.basename(__file__)



    def test(self):
        df = pd.DataFrame({'POD': {0: 'IAD',
                                   1: 'IAD',
                                   2: 'IAD',
                                   3: 'IAD',
                                   4: 'IAD',
                                   5: 'IAD',
                                   6: 'IAD',
                                   7: 'IAD',
                                   8: 'IAD',
                                   9: 'IAD',
                                   10: 'IAD',
                                   11: 'IAD',
                                   12: 'IAD',
                                   13: 'IAD',
                                   14: 'IAD',
                                   15: 'IAD',
                                   16: 'IAD',
                                   17: 'IAD',
                                   18: 'IAD',
                                   19: 'SJCtest',
                                   20: 'SJCtest',
                                   21: 'SJCtest',
                                   22: 'SJCtest',
                                   23: 'SJCtest',
                                   24: 'SJCtest'},
                           'Start': {0: '4/1/2019',
                                     1: '5/1/2019',
                                     2: '6/1/2019',
                                     3: '7/1/2019',
                                     4: '8/1/2019',
                                     5: '9/1/2019',
                                     6: '10/1/2019',
                                     7: '11/1/2019',
                                     8: '12/1/2019',
                                     9: '1/1/2020',
                                     10: '2/1/2020',
                                     11: '3/1/2020',
                                     12: '4/1/2020',
                                     13: '5/1/2020',
                                     14: '6/1/2020',
                                     15: '7/1/2020',
                                     16: '8/1/2020',
                                     17: '9/1/2020',
                                     18: '10/1/2020',
                                     19: '4/1/2019',
                                     20: '5/1/2019',
                                     21: '6/1/2019',
                                     22: '7/1/2019',
                                     23: '8/1/2019',
                                     24: '9/1/2019'},
                           'End': {0: '5/1/2019',
                                   1: '6/1/2019',
                                   2: '7/1/2019',
                                   3: '8/1/2019',
                                   4: '9/1/2019',
                                   5: '10/1/2019',
                                   6: '11/1/2019',
                                   7: '12/1/2019',
                                   8: '1/1/2020',
                                   9: '2/1/2020',
                                   10: '3/1/2020',
                                   11: '4/1/2020',
                                   12: '5/1/2020',
                                   13: '6/1/2020',
                                   14: '7/1/2020',
                                   15: '8/1/2020',
                                   16: '9/1/2020',
                                   17: '10/1/2020',
                                   18: '11/1/2020',
                                   19: '5/1/2019',
                                   20: '6/1/2019',
                                   21: '7/1/2019',
                                   22: '8/1/2019',
                                   23: '9/1/2019',
                                   24: '10/1/2019'},
                           'Diff': {0: 160.4279,
                                    1: 136.0248,
                                    2: 174.0513,
                                    3: 112.0424,
                                    4: 141.8488,
                                    5: 103.5522,
                                    6: 125.6087,
                                    7: 145.2591,
                                    8: 115.5121,
                                    9: 185.7191,
                                    10: 126.7386,
                                    11: 231.3461,
                                    12: 97.02587,
                                    13: 42.85235,
                                    14: 124.666,
                                    15: 357.9974,
                                    16: 490.9587,
                                    17: 204.5478,
                                    18: 287.6025,
                                    19: 12.38486,
                                    20: -2.61735,
                                    21: -5.6187,
                                    22: 3.204252,
                                    23: -25.3782,
                                    24: -10.9717},
                           'Percent': {0: 11.108089999999999,
                                       1: 8.476797999999999,
                                       2: 9.998946,
                                       3: 5.851551000000001,
                                       4: 6.998691,
                                       5: 4.774984,
                                       6: 5.528085,
                                       7: 6.058016,
                                       8: 4.542251,
                                       9: 6.985672999999999,
                                       10: 4.455896,
                                       11: 7.786734,
                                       12: 3.02981,
                                       13: 1.298792,
                                       14: 3.729997,
                                       15: 10.326089999999999,
                                       16: 12.8358,
                                       17: 4.739428,
                                       18: 6.362292,
                                       19: 5.780551,
                                       20: -1.15487,
                                       21: -2.50814,
                                       22: 1.4671530000000002,
                                       23: -11.4521,
                                       24: -5.5913699999999995},
                           'Date': {0: '04-01-2019 to 05-01-2019',
                                    1: '05-01-2019 to 06-01-2019',
                                    2: '06-01-2019 to 07-01-2019',
                                    3: '07-01-2019 to 08-01-2019',
                                    4: '08-01-2019 to 09-01-2019',
                                    5: '09-01-2019 to 10-01-2019',
                                    6: '10-01-2019 to 11-01-2019',
                                    7: '11-01-2019 to 12-01-2019',
                                    8: '12-01-2019 to 01-01-2020',
                                    9: '01-01-2020 to 02-01-2020',
                                    10: '02-01-2020 to 03-01-2020',
                                    11: '03-01-2020 to 04-01-2020',
                                    12: '04-01-2020 to 05-01-2020',
                                    13: '05-01-2020 to 06-01-2020',
                                    14: '06-01-2020 to 07-01-2020',
                                    15: '07-01-2020 to 08-01-2020',
                                    16: '08-01-2020 to 09-01-2020',
                                    17: '09-01-2020 to 10-01-2020',
                                    18: '10-01-2020 to 11-01-2020',
                                    19: '04-01-2019 to 05-01-2019',
                                    20: '05-01-2019 to 06-01-2019',
                                    21: '06-01-2019 to 07-01-2019',
                                    22: '07-01-2019 to 08-01-2019',
                                    23: '08-01-2019 to 09-01-2019',
                                    24: '09-01-2019 to 10-01-2019'}})

        print(df)
        fig = px.line(df, x="Date", y="Diff", color='POD')


        # lim = {'lower': 0, 'upper': 90, 'color': 'red'}
        # lim = {'rps_5d': {'lower': 0, 'upper': 90, 'color': 'red'}, 'rps_10d': {'lower': 0, 'upper': 90, 'color': 'red'},'rps_15d': {'lower': 0, 'upper': 90, 'color': 'red'}, 'rps_20d': {'lower': 0, 'upper': 90, 'color': 'red'}}
        # lim = dict(product(melt_list, lim))

        included = 0

        lim = {'IAD': {'lower': 90, 'upper': 350, 'color': 'yellow'},
               'SJCtest': {'lower': 10, 'upper': 12, 'color': 'green'}}

        for i, d in enumerate(fig.data):
            # print('i,d：',i,d)
            print('d：',d)
            for j, y in enumerate(d.y):
                print('j,y：', j, y,lim[d.name]['lower'])
                if y < lim[d.name]['lower'] or y > lim[d.name]['upper']:

                    if j == 0:
                        fig.add_traces(go.Scatter(x=[fig.data[i]['x'][j]],
                                                  y=[fig.data[i]['y'][j]],
                                                  mode='markers',
                                                  marker=dict(color=lim[d.name]['color']),
                                                  name=d.name + ' threshold',
                                                  legendgroup=d.name + ' threshold'))
                        included = included + 1
                    else:
                        fig.add_traces(go.Scatter(x=[fig.data[i]['x'][j - 1], fig.data[i]['x'][j]],
                                                  y=[fig.data[i]['y'][j - 1], fig.data[i]['y'][j]],
                                                  mode='lines',
                                                  # marker = dict(color='yellow'),
                                                  line=dict(width=6, color=lim[d.name]['color']),
                                                  name=d.name + ' threshold',
                                                  legendgroup=d.name + ' threshold',
                                                  showlegend=False if included > 0 else True,
                                                  ))
                        included = included + 1
        return fig

    def __exit__(self):
        print('{}：执行完毕！！！'.format(self.appName))


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/test_Plotly.py
if __name__ == '__main__':
    fig = UiPlotly().test()
    app = dash.Dash(__name__)
    app.layout = html.Div(
        [
            html.H1('嵌入plotly图表'),
            dcc.Graph(figure=fig)
        ]
    )
    app.run(host='0.0.0.0',port='7777')
