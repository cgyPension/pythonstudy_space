import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from PyQt5.QtCore import Qt
from qtpy import uic
from back_stockui.UiDataUtils import *
from back_stockui.base.basewidget import BaseTabWidget
from back_stockui.viewmodel.pdviewmodel import PdTable
# from back_stockui.view.DwBottomRps import DwBottomRps
# from back_stockui.view.BlockIndWindow import BlockIndWindow



class IndustryTabWidget(BaseTabWidget,UiDataUtils):
    """
    主界面行业tab布局面板
    """

    def __init__(self, mainWindow, tabPosition):
        super(IndustryTabWidget, self).__init__(mainWindow, tabPosition)
        # 表格
        self.model = PdTable(column=2)

        self._init_view()
        self._init_data()
        # self._init_listener()

    def _init_view(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/tab_industry.ui'), self)

        # 底部的显示 QDockWidget
        # self.dw_bottom = DwBottomRps()
        # self.mainWindow.addDockWidget(Qt.BottomDockWidgetArea, self.dw_bottom)
        # self.dw_bottom.setVisible(False)
        #
        # # 板块子窗口
        # self.child_window = BlockIndWindow(self.application)

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
        self.ui.dateTimeEdit.setDate(self.data_max_date())

    def current_change_tab(self, position):
        # DockWidget 的显示和隐藏
        self.dw_bottom.setVisible(True if position == self.tabPosition and self.ui.cb_show_rps.checkState() == 2
                                  else False)

        if position != self.tabPosition:
            pass
        else:
            pass

    def tab_close_requested(self, position):
        pass

    def load_block_rps(self, date):
        df = self.get_plate(date)

        if df.empty:
            print('{} 板块数据为空'.format(date.toString('yyyy-MM-dd')))
            return
        df['turnover'] = df['turnover'].map(str_amount)
        df['volume'] = df['volume'].map(str_value)

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
        self.ui.dateTimeEdit.setDate(trade_date(date,False))

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
        name = s['plate_name']
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
        name = s['plate_name']
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

    def _warning_value(self):
        # 观察20个股
        return (1 - 20 / self.model.data_len()) * 100
