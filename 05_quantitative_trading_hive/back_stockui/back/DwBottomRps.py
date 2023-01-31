import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QGraphicsScene
from qtpy import uic

from back_stockui.UiDataUtils import *
from back_stockui.view.figurecanvas import RpsSingleFigureCanvas
from back_stockui.ComboCheckBox import ComboCheckBox


class DwBottomRps(QtWidgets.QDockWidget):
    """
    行业面板底部的rps
    """

    def __init__(self):
        super(DwBottomRps, self).__init__()

        self._name = ''
        self._start_date = ''
        self._end_date = ''
        self._data = None
        # rps红值提醒
        self.rps_list = ['rps_10d', 'rps_20d', 'rps_50d']
        self.warning_value = 90
        self.canvas = None

        self._init_view()
        self._init_listener()
        self._init_data()

    def _init_view(self):
        self.setFloating(False)
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/dw_stock_main_bottom.ui'), self)

        # 添加rps勾选布局
        self.combo = ComboCheckBox(['rps_5d', 'rps_10d', 'rps_20d', 'rps_50d'])
        self.ui.layout_rps.addWidget(self.combo)
        self.combo.set_select(self.rps_list)

        self.ui.dte_end.setDate(trade_date(QDate.currentDate()))
        end_date = self.ui.dte_end.date()
        # 开始时间为当前时间前推一年
        start_date = end_date.addDays(-365)
        self.ui.dte_start.setDate(start_date)
        self._start_date = start_date.toString('yyyy-MM-dd')
        self._end_date = end_date.toString('yyyy-MM-dd')
        # k线画布
        self.graphic_scene = QGraphicsScene()

    def _init_listener(self):
        self.ui.dte_start.dateChanged.connect(self._start_date_change)
        self.ui.dte_end.dateChanged.connect(self._end_date_change)
        self.ui.pb_refresh_rps.clicked.connect(self._refresh_rps)

    def _init_data(self):
        pass

    def _refresh_rps(self):
        """
        刷新绘制底部rps
        :return:
        """
        self.rps_list = self.combo.get_selected()
        print(self.rps_list)
        self._refresh_canvas(False)

    def _start_date_change(self, date):
        """
        改变开始时间
        :param date:
        :return:
        """
        date = trade_date(date, self.config())
        self.ui.dte_start.setDate(date)
        self._start_date = date.toString('yyyy-MM-dd')
        self._refresh_canvas()

    def _end_date_change(self, date):
        """
        改变结束时间
        :param date:
        :return:
        """
        date = trade_date(date, self.config())
        self.ui.dte_end.setDate(date)
        self._end_date = date.toString('yyyy-MM-dd')
        self._refresh_canvas()

    def _refresh_canvas(self, again_load_data=True):
        """
        整个k线 canvas 重新刷新
        :param again_load_data:
        :return:
        """
        if self.canvas is None:
            self.canvas = RpsSingleFigureCanvas()

        # 是否需要重新加载数据
        if again_load_data:
            self._data = self.get_plate_k(self._name, self._start_date, self._end_date)

        self.canvas.set_data(self._data)
        self.canvas.kline(self.rps_list, self.warning_value)

        # 创建一个QGraphicsScene
        self.graphic_scene.addWidget(self.canvas)
        # 把QGraphicsScene放入QGraphicsView
        self.ui.graphicsView.setScene(self.graphic_scene)
        self.canvas.draw()
        self.ui.graphicsView.show()
        self.show()

    def show_with_data(self, s, warning_value=90):
        # code = s['code']
        name = s['plate_name']
        if self._name == name:
            return
        self.warning_value = warning_value
        self._name = name
        self._init_data()
        self._refresh_canvas()
        self.setWindowTitle(f'{name}rps 摘要')
