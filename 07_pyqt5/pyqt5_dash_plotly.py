import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import threading
from PyQt5 import QtWidgets
import dash
import dash_core_components as dcc
import dash_html_components as html


def run_dash(data, layout):
    app = dash.Dash()

    app.layout = html.Div(children=[
        html.H1(children='Hello Dash'),

        html.Div(children='''
            Dash: A web application framework for Python.
        '''),

        dcc.Graph(
            id='example-graph',
            figure={
                'data': data,
                'layout': layout
            })
        ])
    app.run_server(debug=False)


class MainWindow(QtWidgets.QMainWindow):
    pass


# python /opt/code/pythonstudy_space/07_pyqt5/pyqt5_dash_plotly.py
if __name__ == '__main__':
    data = [
        {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
        {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
    ]

    layout = {
        'title': 'Dash Data Visualization'
    }

    threading.Thread(target=run_dash, args=(data, layout), daemon=True).start()
    app = QtWidgets.QApplication(sys.argv)
    mainWin = MainWindow()
    mainWin.show()
    sys.exit(app.exec_())