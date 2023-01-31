import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import tempfile
from plotly.io import to_html
import plotly.graph_objs as go
from PyQt5 import QtCore, QtGui, QtWidgets, QtWebEngineWidgets


class PlotlyViewer(QtWebEngineWidgets.QWebEngineView):
    def __init__(self, fig=None):
        super().__init__()
        self.set_figure(fig)


    def set_figure(self, fig=None):
        self.temp_file.seek(0)
        if fig is None:
            fig = go.Figure()
        html = to_html(fig, config={"responsive": True, 'scrollZoom': True})
        html += "\n<style>body{margin: 0;}" \
                "\n.plot-container,.main-svg,.svg-container{width:100% !important; height:100% !important;}</style>"
        self.temp_file.write(html)
        self.temp_file.truncate()
        self.temp_file.seek(0)
        self.load(QtCore.QUrl.fromLocalFile(self.temp_file.name))
