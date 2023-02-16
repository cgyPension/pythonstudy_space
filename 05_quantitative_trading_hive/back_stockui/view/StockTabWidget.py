import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import qdarkstyle
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QApplication, QDesktopWidget
from qtpy import uic
from back_stockui.UiDataUtils import *
from back_stockui.PdTable import PdTable
from back_stockui.basewidget import BaseTabWidget


class StockTabWidget(BaseTabWidget,UiDataUtils):
    """个股tab布局面板"""
    def __init__(self, mainWindow, tabPosition):
        super(StockTabWidget, self).__init__(mainWindow, tabPosition)
        # 表格
        self.model = PdTable()
        self._init_view()
        self._init_listener()
        self._init_date()

    def _init_view(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/tab_stock.ui'), self)

    def _init_listener(self):
        # 设置监听
        self.ui.dateTimeEdit.dateChanged.connect(self.onDateChange)
        self.ui.pb_load.clicked.connect(self.btn_clk)
        self.ui.pb_add.clicked.connect(self.btn_add)
        self.ui.pb_sub.clicked.connect(self.btn_sub)
        self.ui.cb_block_sort.stateChanged.connect(self.fix_block_sort)
        # self.ui.cb_show_rps.stateChanged.connect(self.show_bottom_rps)
        # self.ui.tableView.doubleClicked.connect(self.double_clicked_table_view)
        # self.ui.tableView.clicked.connect(self.clicked_table_view_item)

    def _init_date(self):
        # 初始化日期 数据
        self.ui.dateTimeEdit.setDate(self.q_end_date)

    def _load_table_view(self, date):
        date = trade_date(date)
        date_str = date.toString('yyyy-MM-dd')
        df = self.get_all_stock(date_str, date_str)
        if df.empty:
            print(f'{date_str} rps 数据为空')
            return
        self.model.notify_data(df)
        self.ui.tableView.setModel(self.model)

    # == == == == == == == == == == == == == == == == == == == == 事件或小部件显示 == == == == == == == == == == == == == == == == == == == ==
    def btn_clk(self):
        date = self.ui.dateTimeEdit.date()
        self._load_table_view(date)

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

    def onDateChange(self, date):
        """日期改变"""
        self._load_table_view(date)

    def fix_block_sort(self, state):
        """板块是否固定排序"""
        print(f'check box {state}')
        self.model.fix_sort = state == 2

    # def current_change_tab(self, position):
    #     # DockWidget 的显示和隐藏
    #     self.dw_bottom.setVisible(True if position == self.tabPosition and self.ui.cb_show_rps.checkState() == 2
    #                               else False)
    #
    #     if position != self.tabPosition:
    #         pass
    #     else:
    #         pass
    #
    # def double_clicked_table_view(self, item):
    #     """打印被选中的单元格"""
    #     data = item.data()
    #     column = item.column()
    #     row = item.row()
    #     model = item.model()
    #     s = model.get_data(row)
    #     name = s['name']
    #     print(name)
    #     self.child_window.show_with_data(s, self._warning_value())
    #
    # def clicked_table_view_item(self, item):
    #     """打印被选中的单元格"""
    #     # 未选中显示，直接返回
    #     if self.ui.cb_show_rps.checkState() != 2:
    #         return
    #
    #     data = item.data()
    #     column = item.column()
    #
    #     row = item.row()
    #     model = item.model()
    #     s = model.get_data(row)
    #     name = s['name']
    #     self.dw_bottom.show_with_data(s, self._warning_value())
    #
    # def show_bottom_rps(self, state):
    #     """显示底部rps dw_bottom"""
    #     self.dw_bottom.setVisible(state == 2)





# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/view/StockTabWidget.py
if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = StockTabWidget()
    ex.show()
    sys.exit(app.exec_())