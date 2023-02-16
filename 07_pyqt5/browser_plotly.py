import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
from PyQt5.QtWidgets import QDockWidget, QApplication
from qtpy import uic

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from PyQt5 import QtCore, QtWidgets, QtWebEngineWidgets
import plotly.express as px
import qdarkstyle
from back_stockui.UiDataUtils import *
from back_stockui.UiPlotly import UiPlotly

class Widget(QtWidgets.QWidget,UiDataUtils,UiPlotly):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.button = QtWidgets.QPushButton('Plot', self)
        self.browser = QtWebEngineWidgets.QWebEngineView(self)

        vlayout = QtWidgets.QVBoxLayout(self)
        vlayout.addWidget(self.button, alignment=QtCore.Qt.AlignHCenter)
        vlayout.addWidget(self.browser)

        self.button.clicked.connect(self.show_graph)
        self.resize(1000,800)

    def show_graph(self):
        df = self.query_plate('BK0740', '2022-10-01', '2023-01-30')
        fig = self.zx_fig(df, melt_list=['rps_5d', 'rps_10d', 'rps_15d', 'rps_20d', 'rps_50d'], threshold=96)
        self.browser.setHtml(fig.to_html(include_plotlyjs='cdn'))

class DwBottomRps(QDockWidget,UiDataUtils):
    def __init__(self):
        super(DwBottomRps, self).__init__()
        self._init_view()
        self._init_listener()

    def _init_view(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/dw_main_bottom.ui'), self)
        # 添加plotly的承载体
        self.browser = QtWebEngineWidgets.QWebEngineView(self)
        # WebGL enabled: this line appears to be useless
        # self.browser.page().settings().setAttribute(QtWebEngineWidgets.QWebEngineSettings.WebGLEnabled, True)
        self.ui.PlotlyLayout.addWidget(self.browser)

    def _init_listener(self):
        self.ui.LoadData.clicked.connect(self._show_graph)

    def _show_graph(self):
        df = self.query_plate('BK0740', '2022-10-01', '2023-01-30')
        fig = UiPlotly().zx_fig(df, melt_list=['rps_5d', 'rps_10d', 'rps_15d', 'rps_20d', 'rps_50d'], threshold=96)
        self.browser.setHtml(fig.to_html(include_plotlyjs='cdn'))
        self.show()

class BlockIndWindow(QtWidgets.QMainWindow,UiDataUtils):
    def __init__(self):
        super(BlockIndWindow, self).__init__()
        self._init_view()
        self._init_listener()
        self.show()

    def _init_view(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/main_block_indicator.ui'), self)
        # 添加plotly的承载体
        self.browser = QtWebEngineWidgets.QWebEngineView(self)
        # WebGL enabled: this line appears to be useless
        # self.browser.page().settings().setAttribute(QtWebEngineWidgets.QWebEngineSettings.WebGLEnabled, True)
        self.ui.rpsLayout.addWidget(self.browser)

    def _init_listener(self):
        self.ui.pb_stock_refresh_rps.clicked.connect(self._show_graph)

    def _show_graph(self):
        df = self.query_plate('BK0740', '2022-10-01', '2023-01-30')
        fig = UiPlotly().zx_fig(df, melt_list=['rps_5d', 'rps_10d', 'rps_15d', 'rps_20d', 'rps_50d'], threshold=96)
        self.browser.setHtml(fig.to_html(include_plotlyjs='cdn'))
        self.show()

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/browser_plotly.py
if __name__ == "__main__":
    # app = QtWidgets.QApplication([])
    # widget = Widget()
    # widget.show()

    # app = QtWidgets.QApplication([])
    # BottomRps = DwBottomRps()
    # BottomRps.show()

    app = QApplication(sys.argv)        # 创建QApplication
    app.setStyleSheet(qdarkstyle.load_stylesheet())     # 设置样式表
    main_window = BlockIndWindow()

    sys.exit(app.exec_())
    print('{}：程序运行完毕！！！'.format(os.path.basename(__file__)))