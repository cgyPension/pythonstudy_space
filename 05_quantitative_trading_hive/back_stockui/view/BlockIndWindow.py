import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
from back_stockui.UiPlotly import UiPlotly

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import pandas as pd
from PyQt5.QtWidgets import QGraphicsScene
from qtpy import uic
import qdarkstyle
from PyQt5 import QtWidgets, QtWebEngineWidgets
from PyQt5.QtWidgets import QApplication
from back_stockui.UiDataUtils import *
from back_stockui.view.StockIndWindow import StockIndWindow
from back_stockui.view.figurecanvas import MyFigureCanvas, ContrastRpsFigureCanvas
from back_stockui.PdTable import PdTable
from back_stockui.widgets.ComboCheckBox import ComboCheckBox


class BlockIndWindow(QtWidgets.QMainWindow,UiDataUtils):
    """板块详情"""
    def __init__(self):
        super(BlockIndWindow, self).__init__()
        # 板块的属性
        self._block_code = ''
        self._block_name = ''
        self._block_start_date = ''
        self._block_end_date = ''
        self._block_data = None

        # 板块成分股
        self.all_model = PdTable()

        self.stock_df = pd.DataFrame()
        # 选中股的属性
        # 表格
        self.stock_model = PdTable()
        self._stock_code = ''
        self._stock_start_date = ''
        self._stock_end_date = ''
        self._stock_data = None

        # 添加plotly的承载体
        self.browser_k = QtWebEngineWidgets.QWebEngineView(self)
        self.browser_rps = QtWebEngineWidgets.QWebEngineView(self)
        self.ui.kLayout.addWidget(self.browser_k)
        self.ui.rpsLayout.addWidget(self.browser_rps)
        # 股票详情
        self.stock_details_window = StockIndWindow()

        self._init_view()
        # 初始化底部板块
        self._init_bottom_view()
        self._init_listener()


    def _init_view(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/main_block_indicator.ui'), self)
        self.ui.dte_end.setDate(self.q_end_date)
        self.ui.dte_start.setDate(self.q_start_date)
        self._block_start_date = str(self.start_date)
        self._block_end_date = str(self.end_date)


    def _init_bottom_view(self):
        """
        底部成分股view初始化
        :return:
        """
        self.ui.dte_block_date.setDate(self.q_start_date)
        self.ui.dte_stock_end.setDate(self.q_end_date)
        self._stock_start_date = str(self.start_date)
        self._stock_end_date = str(self.end_date)

    # == == == == == == == == == == == == == == == == == == == == 事件或小部件显示 == == == == == == == == == == == == == == == == == == == ==
    def _init_listener(self):
        self.ui.dte_start.dateChanged.connect(self._start_date_change)
        self.ui.dte_end.dateChanged.connect(self._end_date_change)
        self.ui.pb_refresh_k.clicked.connect(self._refresh_k)
        # 板块成分股点击监听
        self.ui.tv_all_stock.doubleClicked.connect(self.double_clicked_bottom_table_view)
        self.ui.tv_all_stock.clicked.connect(self.clicked_bottom_table_view_item)
        # 成分股rps点击监听
        self.ui.pb_stock_refresh_rps.clicked.connect(self._refresh_stock_rps)
        self.ui.cb_block_sort.stateChanged.connect(self.fix_block_sort)
        self.ui.cb_show_rps.stateChanged.connect(self.show_bottom_rps_view)
        self.ui.table_view_bottom.doubleClicked.connect(self.double_clicked_bottom_table_view)
        self.ui.table_view_bottom.clicked.connect(self.clicked_bottom_table_view_item)



    def on_block_date_change(self, date):
        self.load_block_stock(date)

    def fix_block_sort(self, state):
        self.stock_model.fix_sort = state == 2

    def show_bottom_rps_view(self, state):
        self.ui.widget_layout_rps.setVisible(state == 2)

    def double_clicked_bottom_table_view(self, item):
        """
        打印被选中的单元格
        :return:
        """
        data = item.data()
        column = item.column()
        row = item.row()
        model = item.model()

        s = model.get_data(row)
        name = s['name']
        # 右侧的model
        if self.all_model == model:
            df = self.stock_df.reset_index()
            s = df[df['name'] == name].iloc[0]

        self.stock_details_window.show_with_data(s, self._block_name)

    def clicked_bottom_table_view_item(self, item):
        """
        打印被选中的单元格
        :return:
        """
        # 未选中显示，直接返回
        if self.ui.cb_show_rps.checkState() != 2:
            return
        data = item.data()
        column = item.column()
        row = item.row()
        model = item.model()
        s = model.get_data(row)
        name = s['name']
        # 右侧的model
        if self.all_model == model:
            df = self.stock_df.reset_index()
            s = df[df['name'] == name].iloc[0]
        print(name)
        self._set_stock_rps_code(s)

    def _init_block_data(self):
        # 查询板块所有成分股
        df = self.query_plate_stock(self._block_name)
        if not df.empty:
            self.stock_df = df.copy(True)

            # 右侧dw的数据
            self.all_model.notify_data(df)
            self.ui.tv_all_stock.setModel(self.all_model)

            # 如果底部面板显示，直接加载数据
            if self.ui.cb_show_bottom.checkState() == 2:
                date = self.ui.dte_block_date.date()
                self.load_block_stock(date)

                # 清除底部画布
                if not self.stock_brief_canvas is None:
                    self.stock_brief_canvas.clear_figure()
                    self.stock_brief_canvas.draw()
                    self.ui.stock_graphics_view.show()

    def load_block_stock(self, date):
        date = trade_date(date)
        date_str = date.toString('yyyy-MM-dd')
        df = self.query_stock(str(self._stock_code_list())[2:-2], date_str,date_str)

        if df.empty:
            print(f'{date_str} rps 数据为空')
            return
        df['turnover'] = df['turnover'].map(str_amount)
        df['volume'] = df['volume'].map(str_value)
        self.stock_model.notify_data(df)
        self.ui.table_view_bottom.setModel(self.stock_model)



    def _refresh_k(self):
        """刷新绘制底部rps"""
        self._refresh_kline(False)


    def _refresh_kline(self, again_load_data=True):
        """
        整个k线 canvas 重新刷新
        :param again_load_data:
        :return:
        """

        if self.canvas is None:
            self.canvas = MyFigureCanvas()

        # 是否需要重新加载数据
        if again_load_data:
            self._block_data = self.query_plate_rps(self._block_code, self._block_start_date, self._block_end_date)

        # 创建一个QGraphicsScene
        self.graphic_scene.addWidget(self.canvas)
        # 把QGraphicsScene放入QGraphicsView
        self.ui.graphicsView.setScene(self.graphic_scene)
        self.canvas.draw()
        self.ui.graphicsView.show()
        self.show()

    def show_with_data(self, s):
        code = s['name']
        name = s['name']
        if self._block_name == name:
            return
        self._block_code = code
        self._block_name = name
        self._init_block_data()

        self._refresh_kline()
        self.setWindowTitle(name)
        self.show()

    def _stock_code_list(self):
        return list(self.stock_df['code'])

    def _set_stock_rps_code(self, s):
        """
        刷新个股rps
        :param s:
        :param warning_value:
        :return:
        """
        code = s['code']
        name = s['name']
        if self._stock_code == code:
            return

        self._stock_code = code
        self._refresh_stock_rps_graph()

    def _refresh_stock_rps(self):
        """刷新绘制底部rps"""
        self._refresh_stock_rps_graph(False)

    def _refresh_stock_rps_graph(self, again_load_data=True):
        """
        个股rps画布刷新
        :param again_load_data:
        :return:
        """
        # 是否需要重新加载数据
        if again_load_data:
            self._stock_data = self.query_stock(self._stock_code, self._stock_start_date, self._stock_end_date)
        fig = UiPlotly().zx_fig(self._data, melt_list=['rps_5d','rps_10d','rps_20d','rps_50d'], threshold=90)
        self.browser_rps.setHtml(fig.to_html(include_plotlyjs='cdn'))
        self.show()

    def btn_clk(self):
        date = self.ui.dte_block_date.date()
        self.load_block_stock(date)

    def btn_add(self):
        """日期递增"""
        date = self.ui.dateTimeEdit.date()
        date = date.addDays(1)
        self.ui.dateTimeEdit.setDate(trade_date(date))

    def btn_sub(self):
        """日期递减少"""
        date = self.ui.dateTimeEdit.date()
        date = date.addDays(-1)
        self.ui.dateTimeEdit.setDate(trade_date(date))


    def _start_date_change(self, date):
        """改变开始时间"""
        date = trade_date(date)
        self.ui.dte_start.setDate(date)
        self._block_start_date = date.toString('yyyy-MM-dd')
        self._refresh_kline_canvas()

    def _end_date_change(self, date):
        """改变结束时间"""
        date = trade_date(date)
        self.ui.dte_end.setDate(date)
        self._block_end_date = date.toString('yyyy-MM-dd')
        self._refresh_kline_canvas()

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/back_stockui/view/BlockIndWindow.py
if __name__ == '__main__':
    # 创建QApplication
    app = QApplication(sys.argv)
    # 设置样式表
    app.setStyleSheet(qdarkstyle.load_stylesheet())
    # 主窗口
    main_window = BlockIndWindow()
    main_window.show_main_window()

    sys.exit(app.exec_())