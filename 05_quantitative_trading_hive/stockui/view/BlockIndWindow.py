import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import pandas as pd
from PyQt5.QtWidgets import QGraphicsScene
from qtpy import uic
import qdarkstyle
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QApplication
from stockui.UiDataUtils import *
from stockui.view.StockIndWindow import StockIndWindow
from stockui.view.figurecanvas import MyFigureCanvas, ContrastRpsFigureCanvas
from stockui.PdTable import PdTable
from stockui.widgets.ComboCheckBox import ComboCheckBox


class BlockIndWindow(QtWidgets.QMainWindow,UiDataUtils):
    """
    板块详情
    """

    def __init__(self):
        super(BlockIndWindow, self).__init__()

        # 板块的属性
        self._block_name = ''
        self._block_start_date = ''
        self._block_end_date = ''
        self._block_data = None
        # rps红值提醒
        self.block_rps_list = ['rps_5d', 'rps_10d', 'rps_15d','rps_20d']
        self.block_warning_value = 90
        self.canvas = None  # 主视图画布
        self.stock_brief_canvas = None  # 底部股票摘要画布

        # 板块成分股
        self.all_model = PdTable()
        self.my_model = PdTable()

        self.stock_df = pd.DataFrame()
        # 选中股的属性
        # 表格
        self.stock_model = PdTable()
        self._stock_code = ''
        self._stock_start_date = ''
        self._stock_end_date = ''
        self._stock_data = None
        # rps红值提醒
        self.stock_rps_list = ['rps_5d','rps_10d', 'rps_20d', 'rps_50d']
        self.stock_warning_value = 87

        # 股票详情
        self.stock_details_window = StockIndWindow()

        self._init_view()
        # 初始化底部板块
        self._init_bottom_view()
        self._init_listener()
        self._init_bottom_listener()

    def _init_view(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/main_block_indicator.ui'), self)

        # 添加rps勾选布局
        self.combo = ComboCheckBox(['rps_5d', 'rps_10d','rps_15d', 'rps_20d', 'rps_50d'])
        self.ui.layout_rps.addWidget(self.combo)
        self.combo.set_select(self.block_rps_list)

        self.ui.dte_end.setDate(self.q_end_date)
        self.ui.dte_start.setDate(self.q_start_date)
        self._block_start_date = str(self.start_date)
        self._block_end_date = str(self.end_date)
        # k线画布
        self.graphic_scene = QGraphicsScene()

        # 设置主链接控件占比，即0号窗口大小占80 %，即1号窗口大小占20 %
        # 摸鱼时刻，主图搞小点
        is_to_catch_fish = True
        self.ui.splitter_main.setStretchFactor(0, 90 if not is_to_catch_fish else 10)
        self.ui.splitter_main.setStretchFactor(1, 10 if is_to_catch_fish else 90)


    def _init_bottom_view(self):
        """
        底部成分股view初始化
        :return:
        """
        # 底部先隐藏
        # self.ui.splitter_bottom.setVisible(False)

        # 添加rps勾选布局
        self.combo_bottom = ComboCheckBox(['rps_5d', 'rps_10d', 'rps_20d', 'rps_50d'])
        self.ui.layout_stock_rps.addWidget(self.combo_bottom)
        self.combo_bottom.set_select(self.stock_rps_list)

        # 先把时间初始化好
        self.ui.dte_block_date.setDate(self.q_start_date)
        self.ui.dte_stock_end.setDate(self.q_end_date)
        self._stock_start_date = str(self.start_date)
        self._stock_end_date = str(self.end_date)

        # 个股线画布
        self.stock_graphic_scene = QGraphicsScene()

    # == == == == == == == == == == == == == == == == == == == == 事件或小部件显示 == == == == == == == == == == == == == == == == == == == ==
    def _init_listener(self):
        self.ui.dte_start.dateChanged.connect(self._start_date_change)
        self.ui.dte_end.dateChanged.connect(self._end_date_change)
        self.ui.pb_refresh_rps.clicked.connect(self._refresh_rps)
        self.ui.cb_show_bottom.stateChanged.connect(self.show_bottom_brief)

        # 板块股票列表点击监听
        self.ui.tv_all_stock.doubleClicked.connect(self.double_clicked_bottom_table_view)
        self.ui.tv_my_stock.doubleClicked.connect(self._double_clicked_my_table)

        self.ui.tv_all_stock.clicked.connect(self.clicked_bottom_table_view_item)

    def _init_bottom_listener(self):
        """
        成分股view监听
        :return:
        """
        self.ui.dte_block_date.dateChanged.connect(self.on_block_date_change)
        self.ui.pb_load.clicked.connect(self.btn_clk)
        self.ui.pb_add.clicked.connect(self.btn_add)
        self.ui.pb_sub.clicked.connect(self.btn_sub)
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

        self.stock_details_window.show_with_data(s, self._block_name, self.block_warning_value)

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
            # self.ui.tv_my_stock.setModel(self.my_model)

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

    def show_bottom_brief(self, state):
        """显示底部 """
        self.ui.splitter_bottom.setVisible(state == 2)

    def _double_clicked_all_table(self):
        pass

    def _double_clicked_my_table(self):
        pass

    def _refresh_rps(self):
        """刷新绘制底部rps"""
        self.block_rps_list = self.combo.get_selected()
        print(self.block_rps_list)
        self._refresh_kline_canvas(False)

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

    def _refresh_kline_canvas(self, again_load_data=True):
        """
        整个k线 canvas 重新刷新
        :param again_load_data:
        :return:
        """

        if self.canvas is None:
            self.canvas = MyFigureCanvas()

        # 是否需要重新加载数据
        if again_load_data:
            self._block_data = self.query_plate_rps(self._block_name, self._block_start_date, self._block_end_date)

        self.canvas.set_data(self._block_data)
        self.canvas.kline(self.block_rps_list, self.block_warning_value)

        # 创建一个QGraphicsScene
        self.graphic_scene.addWidget(self.canvas)
        # 把QGraphicsScene放入QGraphicsView
        self.ui.graphicsView.setScene(self.graphic_scene)
        self.canvas.draw()
        self.ui.graphicsView.show()
        self.show()

    def show_with_data(self, s, warning_value=90):
        name = s['name']
        if self._block_name == name:
            return

        self.block_warning_value = warning_value
        self._block_name = name
        name = s['name']
        self._init_block_data()

        self._refresh_kline_canvas()
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
        self._refresh_stock_rps_canvas()
        # self.setWindowTitle(f'{name}rps 摘要')

    def _refresh_stock_rps(self):
        """
        刷新绘制底部rps
        :return:
        """
        self.stock_rps_list = self.combo_bottom.get_selected()
        print(self.stock_rps_list)
        self._refresh_stock_rps_canvas(False)

    def _refresh_stock_rps_canvas(self, again_load_data=True):
        """
        个股rps画布刷新
        :param again_load_data:
        :return:
        """

        if self.stock_brief_canvas is None:
            self.stock_brief_canvas = ContrastRpsFigureCanvas()
        # 是否需要重新加载数据
        if again_load_data:
            self._stock_data = self.query_stock_rps(self._stock_code, self._stock_start_date, self._stock_end_date)

        # 板块数据对齐个股
        block_data = self._block_data.reindex_like(self._stock_data)

        self.stock_brief_canvas.set_data(self._stock_data, block_data)
        self.stock_brief_canvas.kline(self.stock_rps_list, self.stock_warning_value)

        # 创建一个QGraphicsScene
        self.stock_graphic_scene.addWidget(self.stock_brief_canvas)
        # 把QGraphicsScene放入QGraphicsView
        self.ui.stock_graphics_view.setScene(self.stock_graphic_scene)
        self.stock_brief_canvas.draw()
        self.ui.stock_graphics_view.show()
        self.show()


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