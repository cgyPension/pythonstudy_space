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
from back_stockui.UiDataUtils import *
# from back_stockui.view.StockIndWindow import StockIndWindow
# from back_stockui.view.figurecanvas import MyFigureCanvas, ContrastRpsFigureCanvas
from back_stockui.viewmodel.pdviewmodel import PdTable
from back_stockui.ComboCheckBox import ComboCheckBox


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
        self.block_rps_list = ['rps_10d', 'rps_20d', 'rps_50d']
        self.block_warning_value = 90
        self.canvas = None  # 主视图画布
        self.stock_brief_canvas = None  # 底部股票摘要画布

        # 板块成分股，指定初始排序列
        self.all_model = PdTable(column=1)
        self.my_model = PdTable(column=1)

        self.stock_df = pd.DataFrame()
        # 选中股的属性
        # 表格
        self.stock_model = PdTable(column=2)
        self._stock_code = ''
        self._stock_start_date = ''
        self._stock_end_date = ''
        self._stock_data = None
        # rps红值提醒
        self.stock_rps_list = ['rps_10d', 'rps_20d', 'rps_50d']
        self.stock_warning_value = 87

        # 股票详情
        # self.stock_details_window = StockIndWindow()

        self._init_view()
        # 初始化底部板块
        self._init_bottom_view()
        self._init_listener()
        self._init_bottom_listener()

    def _init_view(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/main_block_indicator.ui'), self)

        # 添加rps勾选布局
        self.combo = ComboCheckBox(['rps_5d', 'rps_10d', 'rps_20d', 'rps_50d'])
        self.ui.layout_rps.addWidget(self.combo)
        self.combo.set_select(self.block_rps_list)

        self.ui.dte_end.setDate(trade_date(QDate.currentDate()))
        end_date = self.ui.dte_end.date()
        # 开始时间为当前时间前推一年
        start_date = end_date.addDays(-365)
        self.ui.dte_start.setDate(start_date)
        self._block_start_date = start_date.toString('yyyy-MM-dd')
        self._block_end_date = end_date.toString('yyyy-MM-dd')
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
        date_str = self.config().rps_stock_start_time()
        end_date = QDate.fromString(date_str, 'yyyy-MM-dd')
        self.ui.dte_block_date.setDate(trade_date(end_date))

        self.ui.dte_stock_end.setDate(trade_date(QDate.currentDate()))
        end_date = self.ui.dte_stock_end.date()
        # 开始时间为当前时间前推一年
        start_date = end_date.addDays(-365)
        self.ui.dte_stock_start.setDate(start_date)
        self._stock_start_date = start_date.toString('yyyy-MM-dd')
        self._stock_end_date = end_date.toString('yyyy-MM-dd')

        # 个股线画布
        self.stock_graphic_scene = QGraphicsScene()

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