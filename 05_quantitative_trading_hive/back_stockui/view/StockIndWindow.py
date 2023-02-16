import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from PyQt5.QtWidgets import QGraphicsScene
from qtpy import uic
from back_stockui.UiDataUtils import *
from PyQt5 import QtWidgets
from back_stockui.view.figurecanvas import StockFigureCanvas
from back_stockui.widgets.ComboCheckBox import ComboCheckBox


class StockIndWindow(QtWidgets.QMainWindow,UiDataUtils):
    """
    个股详情
    """

    def __init__(self):
        super(StockIndWindow, self).__init__()

        self.canvas = None
        # 板块的属性
        self._stock_code = ''
        self._stock_start_date = ''
        self._stock_end_date = ''
        self._stock_data = None
        # rps红值提醒
        self.stock_rps_list = ['rps_5d', 'rps_10d', 'rps_20d']
        self._block_name = ''
        self._block_data = None

        self._init_view()
        self._init_listener()

    def _init_view(self):

        self.ui = uic.loadUi(os.path.join(curPath, 'designer/main_stock_indicator.ui'), self)
        # 添加rps勾选布局
        self.combo = ComboCheckBox(['rps_5d', 'rps_10d', 'rps_20d', 'rps_50d'])
        self.ui.layout_rps.addWidget(self.combo)
        self.combo.set_select(self.stock_rps_list)

        self.ui.dte_end.setDate(self.q_end_date)
        self.ui.dte_start.setDate(self.q_start_date)
        self._stock_start_date = str(self.start_date)
        self._stock_end_date = str(self.end_date)
        # k线画布
        self.graphic_scene = QGraphicsScene()

    # == == == == == == == == == == == == == == == == == == == == 事件或小部件显示 == == == == == == == == == == == == == == == == == == == ==
    def _init_listener(self):
        self.ui.dte_start.dateChanged.connect(self._start_date_change)
        self.ui.dte_end.dateChanged.connect(self._end_date_change)
        self.ui.pb_refresh_rps.clicked.connect(self._refresh_rps)

    def _refresh_rps(self):
        """
        刷新绘制底部rps
        :return:
        """
        self.stock_rps_list = self.combo.get_selected()
        print(self.stock_rps_list)
        self._refresh_kline_canvas(False)

    def _start_date_change(self, date):
        """
        改变开始时间
        :param date:
        :return:
        """
        date = trade_date(date)
        self.ui.dte_start.setDate(date)
        self._stock_start_date = date.toString('yyyy-MM-dd')
        self._refresh_kline_canvas()

    def _end_date_change(self, date):
        """
        改变结束时间
        :param date:
        :return:
        """
        date = trade_date(date)
        self.ui.dte_end.setDate(date)
        self._stock_end_date = date.toString('yyyy-MM-dd')
        self._refresh_kline_canvas()

    def _refresh_kline_canvas(self, again_load_data=True):
        """
        整个k线 canvas 重新刷新
        :param again_load_data:
        :return:
        """

        if self.canvas is None:
            self.canvas = StockFigureCanvas()

        # 是否需要重新加载数据
        if again_load_data:
            df = self.query_stock(self._stock_code, self._stock_start_date, self._stock_end_date)
            self._stock_data = df

            # 加载板块rps指标
            self._block_data = self.query_plate_range(self._block_code, self._stock_start_date, self._stock_end_date)

        self.canvas.set_data(self._stock_data, self._block_data)
        self.canvas.kline(self.stock_rps_list, 87)

        # 创建一个QGraphicsScene
        self.graphic_scene.addWidget(self.canvas)
        # 把QGraphicsScene放入QGraphicsView
        self.ui.graphicsView.setScene(self.graphic_scene)
        self.canvas.draw()
        self.ui.graphicsView.show()
        self.show()

    def show_with_data(self, s, block_name):
        """
        双击个股，刷新
        :param s:
        :param block_warning_value: 板块的提示值
        :return:
        """
        code = s['code']
        if self._stock_code == code:
            return

        self._block_name = block_name
        self._stock_code = code
        name = s['name']

        self._refresh_kline_canvas()
        self.setWindowTitle(name)
        self.show()
