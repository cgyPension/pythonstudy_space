from PyQt5.QtWidgets import QGraphicsScene
from qtpy import uic

from database import stockdatabase as sdb
from ui.base.basewidget import BaseDockWidget
from ui.utils.uiutils import *
from ui.view.figurecanvas import RpsSingleFigureCanvas
from ui.widget.ComboCheckBox import ComboCheckBox


class RpsDockWidget(BaseDockWidget):
    def __init__(self, application):
        super(RpsDockWidget, self).__init__(application)

        self._code = ''
        self._start_date = ''
        self._end_date = ''
        self._data = None
        # rps红值提醒
        self.rps_list = ['rps10', 'rps20', 'rps50']
        self.warning_value = 90
        self.canvas = None

        self._init_view()
        self._init_listener()
        self._init_data()

    def _init_view(self):
        self.setFloating(False)
        self.ui = uic.loadUi(os.path.join(ui_dir_path, 'designer/dw_stock_main_bottom.ui'), self)

        # 添加rps勾选布局
        self.combo = ComboCheckBox(['rps05', 'rps10', 'rps20', 'rps50', 'rps120', 'rps250'])
        self.ui.layout_rps.addWidget(self.combo)
        self.combo.set_select(self.rps_list)

        self.ui.dte_end.setDate(trade_date(QDate.currentDate(), self.config()))
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
            self._data = sdb.block_stock_daily(self._code, self._start_date, self._end_date)

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
        code = s['code']
        name = s['name']
        if self._code == code:
            return
        self.warning_value = warning_value
        self._code = code
        self._init_data()
        self._refresh_canvas()
        self.setWindowTitle(f'{name}rps 摘要')
