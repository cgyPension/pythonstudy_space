import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from PyQt5.QtWidgets import QGraphicsScene,QDockWidget
from qtpy import uic
from stockui.UiDataUtils import *
from stockui.view.figurecanvas import RpsSingleFigureCanvas
from stockui.widgets.ComboCheckBox import ComboCheckBox


class DwBottomRps(QDockWidget,UiDataUtils):
    """行业面板底部的rps"""

    def __init__(self):
        super(DwBottomRps, self).__init__()

        self._name = ''
        self._start_date = ''
        self._end_date = ''
        self._data = None
        # rps红值提醒
        self.rps_list = ['rps_5d','rps_10d','rps_15d','rps_20d']
        self.warning_value = 90
        self.canvas = None

        self._init_view()
        self._init_listener()
        self._init_date()

    def _init_view(self):
        self.setFloating(False)
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/dw_stock_main_bottom.ui'), self)

        # 添加rps勾选布局
        self.combo = ComboCheckBox(['rps_5d','rps_10d','rps_15d','rps_20d','rps_50d'])
        self.ui.layout_rps.addWidget(self.combo)
        self.combo.set_select(self.rps_list)

        self.ui.dte_end.setDate(self.q_end_date)
        self.ui.dte_start.setDate(self.q_start_date)

        self._start_date = str(self.start_date)
        self._end_date = str(self.end_date)
        # k线画布
        self.graphic_scene = QGraphicsScene()

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
            self._data = self.query_plate_rps(self._name, self._start_date, self._end_date)

        self.canvas.set_data(self._data)
        self.canvas.kline(self.rps_list, self.warning_value)

        # 创建一个QGraphicsScene
        self.graphic_scene.addWidget(self.canvas)
        # 把QGraphicsScene放入QGraphicsView
        self.ui.graphicsView.setScene(self.graphic_scene)
        self.canvas.draw()
        self.ui.graphicsView.show()
        self.show()

    # == == == == == == == == == == == == == == == == == == == == 事件或小部件显示 == == == == == == == == == == == == == == == == == == == ==

    def _init_listener(self):
        self.ui.dte_start.dateChanged.connect(self._start_date_change)
        self.ui.dte_end.dateChanged.connect(self._end_date_change)
        self.ui.pb_refresh_rps.clicked.connect(self._refresh_rps)

    def _init_date(self):
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
        date = trade_date(date)
        self.ui.dte_start.setDate(date)
        self._start_date = date.toString('yyyy-MM-dd')
        self._refresh_canvas()

    def _end_date_change(self, date):
        """
        改变结束时间
        :param date:
        :return:
        """
        date = trade_date(date)
        self.ui.dte_end.setDate(date)
        self._end_date = date.toString('yyyy-MM-dd')
        self._refresh_canvas()

    def show_with_data(self, s, warning_value=90):
        # code = s['code']
        name = s['name']
        if self._name == name:
            return
        self.warning_value = warning_value
        self._name = name
        self._init_date()
        self._refresh_canvas()
        self.setWindowTitle(f'{name}rps 摘要')
