import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from back_stockui.UiDataUtils import *
from qtpy import uic
from back_stockui.UiPlotly import UiPlotly
from PyQt5.QtWidgets import QDockWidget
from PyQt5 import QtWebEngineWidgets, QtWidgets




class DwBottomRps(QDockWidget,UiDataUtils,UiPlotly):
    """行业面板底部的rps"""

    def __init__(self):
        super(DwBottomRps, self).__init__()

        self._code = ''
        self._name = ''
        self._start_date = ''
        self._end_date = ''
        self._data = None
        self._init_view()
        self._init_listener()

    def _init_view(self):
        self.setFloating(False)
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/dw_main_bottom.ui'), self)

        # 添加plotly的承载体
        self.browser = QtWebEngineWidgets.QWebEngineView(self)
        # WebGL enabled: this line appears to be useless
        # self.browser.page().settings().setAttribute(QtWebEngineWidgets.QWebEngineSettings.WebGLEnabled, True)
        self.ui.PlotlyLayout.addWidget(self.browser)

        self.ui.dte_end.setDate(self.q_end_date)
        self.ui.dte_start.setDate(self.q_start_date)

        self._start_date = str(self.start_date)
        self._end_date = str(self.end_date)

    def _show_graph(self, again_load_data=True):
        # print('self._code:',self._code, self._start_date, self._end_date)
        # 是否需要重新加载数据
        if again_load_data:
            self._data = self.query_plate(self._code, self._start_date, self._end_date)
        fig = UiPlotly().zx_fig(self._data, melt_list=['rps_5d', 'rps_10d', 'rps_15d', 'rps_20d', 'rps_50d'], threshold=90)
        self.browser.setHtml(fig.to_html(include_plotlyjs='cdn'))
        self.show()

    # == == == == == == == == == == == == == == == == == == == == 事件或小部件显示 == == == == == == == == == == == == == == == == == == == ==

    def _init_listener(self):
        self.ui.dte_start.dateChanged.connect(self._start_date_change)
        self.ui.dte_end.dateChanged.connect(self._end_date_change)
        self.ui.LoadData.clicked.connect(self._show_graph)

    def _start_date_change(self, date):
        """
        改变开始时间
        :param date:
        :return:
        """
        date = trade_date(date)
        self.ui.dte_start.setDate(date)
        self._start_date = date.toString('yyyy-MM-dd')
        self._show_graph()

    def _end_date_change(self, date):
        """
        改变结束时间
        :param date:
        :return:
        """
        date = trade_date(date)
        self.ui.dte_end.setDate(date)
        self._end_date = date.toString('yyyy-MM-dd')
        self._show_graph()

    def show_with_data(self, s):
        code = s['code']
        name = s['name']
        if self._code == code:
            return
        self._code = code
        self._name = name
        self._show_graph()
        self.setWindowTitle(f'{name}rps')

    def current_change_tab(self, position):
        # DockWidget 的显示和隐藏
        self.dw_strategy_stock_pool.setVisible(True if position == self.tabPosition else False)
        self.dw_strategy_result.setVisible(True if position == self.tabPosition else False)

        if position != self.tabPosition:
            pass
        else:
            pass

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/view/DwBottomRps.py
if __name__ == '__main__':
    app = QtWidgets.QApplication([])

    BottomRps=DwBottomRps()
    BottomRps.show()
    sys.exit(app.exec_())
    print('{}：程序运行完毕！！！'.format(os.path.basename(__file__)))