import sys

import qdarkstyle
from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication
from qtpy import uic

from app.stockapplication import StockApplication
from database import stockdatabase as sdb
from ui.base.basewidget import BaseMainWindow
from ui.utils.uiutils import *
from ui.view.BlockIndWindow import BlockIndWindow
from ui.view.RpsDockWidget import RpsDockWidget
from ui.viewmodel.pdviewmodel import PdTable


class StockMainWindow(BaseMainWindow):
    def __init__(self, application):
        super(StockMainWindow, self).__init__(application)

        # 表格
        self.model = PdTable(column=2)

        self._init_view()
        self._init_listener()
        self._init_data()

    def _init_view(self):
        # create main window
        self.setObjectName('main_window')
        self.ui = uic.loadUi(os.path.join(ui_dir_path, 'designer/app_main.ui'), self)
        self.setWindowTitle('量化系统')

        # 板块子窗口
        self.child_window = BlockIndWindow(self.application)

        # 底部的显示 QDockWidget
        self.dw_bottom = RpsDockWidget(self.application)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.dw_bottom)
        self.dw_bottom.setVisible(False)

        # 状态栏
        statusbar = QtWidgets.QStatusBar()
        statusbar.addWidget(QtWidgets.QLabel('版本号：version 1.0.0'))
        self.setStatusBar(statusbar)

    def _init_listener(self):
        # 设置监听
        self.ui.dateTimeEdit.dateChanged.connect(self.onDateChange)
        self.ui.pb_load.clicked.connect(self.btn_clk)
        self.ui.pb_add.clicked.connect(self.btn_add)
        self.ui.pb_sub.clicked.connect(self.btn_sub)
        self.ui.cb_block_sort.stateChanged.connect(self.fix_block_sort)
        self.ui.cb_show_rps.stateChanged.connect(self.show_bottom_rps)
        self.ui.tableView.doubleClicked.connect(self.double_clicked_table_view)
        self.ui.tableView.clicked.connect(self.clicked_table_view_item)

    def _init_data(self):
        # 初始化信息
        self.ui.dateTimeEdit.setDate(trade_date(QDate.currentDate(), self.config()))

    def load_block_rps(self, date):
        date = trade_date(date, self.config())
        date_str = date.toString('yyyy-MM-dd')
        use_col = ['name', 'rps05', 'rps10', 'rps20', 'rps50', 'rps120', 'rps250',
                   'volume', 'amount', 'open', 'close']
        df = sdb.block_daily_on_date(date_str, use_col)
        if df.empty:
            print(f'{date_str} rps 数据为空')
            return
        # 改变列位置
        df['amount'] = df['amount'].map(str_amount)
        df['volume'] = df['volume'].map(str_value)
        df['change'] = df[['open', 'close']].apply(str_change, axis=1)
        df = df[['code', 'name', 'change', 'amount', 'rps05', 'rps10', 'rps20', 'rps50', 'rps120', 'rps250', 'volume']]
        df.drop_duplicates('name', inplace=True)
        self.model.notify_data(df)
        self.ui.tableView.setModel(self.model)

    def btn_clk(self):
        date = self.ui.dateTimeEdit.date()
        self.load_block_rps(date)

    def btn_add(self):
        """
        日期递增
        :param ui:
        :return:
        """
        date = self.ui.dateTimeEdit.date()
        date = date.addDays(1)
        self.ui.dateTimeEdit.setDate(trade_date(date, self.config(), False))

    def btn_sub(self):
        """
        日期递减少
        :param ui:
        :return:
        """
        date = self.ui.dateTimeEdit.date()
        date = date.addDays(-1)
        self.ui.dateTimeEdit.setDate(trade_date(date, self.config()))

    def double_clicked_table_view(self, item):
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
        print(name)
        self.child_window.show_with_data(s, self._warning_value())

    def clicked_table_view_item(self, item):
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
        self.dw_bottom.show_with_data(s, self._warning_value())

    def onDateChange(self, date):
        """
        日期改变
        :param date:
        :return:
        """
        self.load_block_rps(date)

    def fix_block_sort(self, state):
        """
        板块是否固定排序
        :return:
        """
        print(f'check box {state}')
        self.model.fix_sort = state == 2

    def show_bottom_rps(self, state):
        """
        显示底部rps dw_bottom
        :param state:
        :return:
        """
        self.dw_bottom.setVisible(state == 2)

    def show_main_window(self):
        # 大屏显示
        self.showMinimized()
        # self.showMaximized()

    def _warning_value(self):
        # 观察20个股
        return (1 - 20 / self.model.data_len()) * 100


if __name__ == '__main__':
    # 项目启动类
    application = StockApplication()
    application.start(True)
    config = application.config

    # 创建QApplication
    app = QApplication(sys.argv)
    # 设置样式表
    app.setStyleSheet(qdarkstyle.load_stylesheet())

    # 主窗口
    main_window = StockMainWindow(application)
    main_window.show_main_window()

    sys.exit(app.exec_())
