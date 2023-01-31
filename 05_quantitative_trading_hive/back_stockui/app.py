import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
from back_stockui.view.StockTabWidget import StockTabWidget

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import qdarkstyle
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QApplication
from qtpy import uic

from back_stockui.UiDataUtils import *


class AppMainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(AppMainWindow, self).__init__()

        self.tabWidgetList = list()
        self._init_view()
        self._init_listener()


    def _init_view(self):
        # create main window
        self.setObjectName('main_window')
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/app_main.ui'), self)
        self.setWindowTitle('量化系统')

        # 主页面大盘tab

        # 主页面行业tab
        # self.industry_tab_widget = IndustryTabWidget(self, 1)
        # self.tabWidgetList.append(self.industry_tab_widget)
        # self.ui.industryLayout.addWidget(self.industry_tab_widget)


        # 主页面个股tab
        self.stock_tab_widget = StockTabWidget(self, 2)
        self.tabWidgetList.append(self.stock_tab_widget)
        self.ui.stockLayout.addWidget(self.stock_tab_widget)


        # 底部状态栏
        statusbar = QtWidgets.QStatusBar()
        statusbar.addWidget(QtWidgets.QLabel('版本号：version 1.0.0'))
        self.setStatusBar(statusbar)

    def _init_listener(self):
        self.ui.tabWidget.currentChanged.connect(self.current_change_tab)
        self.ui.tabWidget.tabCloseRequested.connect(self.tab_close_requested)

    def current_change_tab(self, position):
        """
        通知子tabWidget
        :param position:
        :return:
        """
        for tabWidget in self.tabWidgetList:
            tabWidget.current_change_tab(position)

    def tab_close_requested(self, position):
        """
        通知子tabWidget
        :param position:
        :return:
        """
        for tabWidget in self.tabWidgetList:
            tabWidget.tab_close_requested(position)

    def show_main_window(self):
        # 大屏显示
        self.showMinimized()

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/back_stockui/app.py
if __name__ == '__main__':
    # 创建QApplication
    app = QApplication(sys.argv)
    # 设置样式表
    app.setStyleSheet(qdarkstyle.load_stylesheet())

    # 主窗口
    main_window = AppMainWindow()
    main_window.show_main_window()

    sys.exit(app.exec_())
