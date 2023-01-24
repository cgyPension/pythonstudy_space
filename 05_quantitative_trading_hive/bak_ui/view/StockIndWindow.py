from PyQt5.QtWidgets import QGraphicsScene
from qtpy import uic

from database import stockdatabase as sdb
from ui.base.basewidget import BaseMainWindow
from ui.utils.uiutils import *
from ui.view.figurecanvas import StockFigureCanvas
from ui.widget.ComboCheckBox import ComboCheckBox


class StockIndWindow(BaseMainWindow):
    """
    个股详情
    """

    def __init__(self, application):
        super(StockIndWindow, self).__init__(application)

        self.canvas = None
        # 板块的属性
        self._stock_code = ''
        self._stock_start_date = ''
        self._stock_end_date = ''
        self._stock_data = None
        # rps红值提醒
        self.stock_rps_list = ['rps10', 'rps20', 'rps50']
        self._block_code = ''
        self._block_data = None
        self.block_warning_value = 90

        self._init_view()
        self._init_listener()

    def _init_view(self):

        self.ui = uic.loadUi(os.path.join(ui_dir_path, 'designer/main_stock_indicator.ui'), self)

        # 添加rps勾选布局
        self.combo = ComboCheckBox(['rps05', 'rps10', 'rps20', 'rps50', 'rps120', 'rps250'])
        self.ui.layout_rps.addWidget(self.combo)
        self.combo.set_select(self.stock_rps_list)

        self.ui.dte_end.setDate(trade_date(QDate.currentDate(), self.config()))
        end_date = self.ui.dte_end.date()
        # 开始时间为当前时间前推一年
        start_date = end_date.addDays(-365)
        self.ui.dte_start.setDate(start_date)
        self._stock_start_date = start_date.toString('yyyy-MM-dd')
        self._stock_end_date = end_date.toString('yyyy-MM-dd')
        # k线画布
        self.graphic_scene = QGraphicsScene()

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
        date = trade_date(date, self.config())
        self.ui.dte_start.setDate(date)
        self._stock_start_date = date.toString('yyyy-MM-dd')
        self._refresh_kline_canvas()

    def _end_date_change(self, date):
        """
        改变结束时间
        :param date:
        :return:
        """
        date = trade_date(date, self.config())
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
            df = sdb.stock_daily(self._stock_code, self._stock_start_date, self._stock_end_date)
            df1 = sdb.stock_pool_indicator(self._stock_code, self._stock_start_date, self._stock_end_date)
            df2 = sdb.stock_indicator(self._stock_code, self._stock_start_date, self._stock_end_date)
            df[['rps05', 'rps10', 'rps20', 'rps50', 'rps120', 'rps250']] = df1[
                ['rps05', 'rps10', 'rps20', 'rps50', 'rps120', 'rps250']]
            df[['ma05', 'ma10', 'ma20', 'ma50', 'ma120', 'ma250']] = df2[
                ['ma05', 'ma10', 'ma20', 'ma50', 'ma120', 'ma250']]
            self._stock_data = df

            # 加载板块rps指标
            use_col = ['name', 'rps05', 'rps10', 'rps20', 'rps50', 'rps120', 'rps250']
            self._block_data = sdb.block_stock_daily(self._block_code, self._stock_start_date, self._stock_end_date,
                                                     use_col)
            # 对齐个股日期
            self._block_data, tmp = self._block_data.align(self._stock_data, join='right', axis=0)

        self.canvas.set_data(self._stock_data, self._block_data)
        self.canvas.kline(self.stock_rps_list, 87, self.block_warning_value)

        # 创建一个QGraphicsScene
        self.graphic_scene.addWidget(self.canvas)
        # 把QGraphicsScene放入QGraphicsView
        self.ui.graphicsView.setScene(self.graphic_scene)
        self.canvas.draw()
        self.ui.graphicsView.show()
        self.show()

    def show_with_data(self, s, block_code, block_warning_value=90):
        """
        双击个股，刷新
        :param s:
        :param block_warning_value: 板块的提示值
        :return:
        """
        code = s['code']
        if self._stock_code == code:
            return

        self.block_warning_value = block_warning_value
        self._block_code = block_code
        self._stock_code = code
        name = s['name']

        self._refresh_kline_canvas()
        self.setWindowTitle(name)
        self.show()
