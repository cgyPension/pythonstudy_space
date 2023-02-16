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
from back_stockui.view.StockTabWidget import StockTabWidget
from back_stockui.view.IndustryTabWidget import IndustryTabWidget

class AppMainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(AppMainWindow, self).__init__()

        self.tabWidgetList = list()
        self._init_view()
        self._init_listener()

        self.show_main_window()


    def _init_view(self):
        # create main window
        self.setObjectName('main_window')
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/main.ui'), self)
        self.setWindowTitle('量化系统')

        # 主页面大盘tab

        # 主页面板块tab
        self.plate_tab_widget = IndustryTabWidget(self, 1)
        self.tabWidgetList.append(self.plate_tab_widget)
        self.ui.plate_Layout.addWidget(self.plate_tab_widget)

        # 主页面个股tab
        # self.stock_tab_widget = StockTabWidget(self, 2)
        # self.tabWidgetList.append(self.stock_tab_widget)
        # self.ui.stock_Layout.addWidget(self.stock_tab_widget)

        # 主页面回测tab



        # 底部状态栏
        statusbar = QtWidgets.QStatusBar()
        statusbar.addWidget(QtWidgets.QLabel('版本号：version 1.0.0'))
        self.setStatusBar(statusbar)

    def _init_listener(self):
        self.ui.tabWidget.currentChanged.connect(self.current_change_tab)
        self.ui.tabWidget.tabCloseRequested.connect(self.tab_close_requested)

    def current_change_tab(self, position):
        """通知子tabWidget"""
        for tabWidget in self.tabWidgetList:
            tabWidget.current_change_tab(position)

    def tab_close_requested(self, position):
        """通知子tabWidget"""
        for tabWidget in self.tabWidgetList:
            tabWidget.tab_close_requested(position)

    def show_main_window(self):
        '''窗口居中，大屏显示'''
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        self.showMinimized()

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/main.py
if __name__ == '__main__':
    app = QApplication(sys.argv)        # 创建QApplication
    app.setStyleSheet(qdarkstyle.load_stylesheet())     # 设置样式表

    main_window = AppMainWindow()
    sys.exit(app.exec_())
    print('{}：程序运行完毕！！！'.format(os.path.basename(__file__)))