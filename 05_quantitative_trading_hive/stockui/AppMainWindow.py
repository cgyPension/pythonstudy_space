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
from stockui.view.PlateTabWidget import PlateTabWidget
from stockui.view.GroupTabWidget import GroupTabWidget
from stockui.view.StockTabWidget import StockTabWidget

'''
需要x-shell 的xmanager做图形回传
linux运行：env | grep DISPLAY
py文件编辑配置设置这个环境变量(每次都会变，如果直接在shell运行就不用设置)
DISPLAY=localhost:13.0
'''
class AppMainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(AppMainWindow, self).__init__()
        self.tabWidgetList = list()
        self.initUi()
        self.listener()
        self.showMaximized()


    def initUi(self):
        '''加载ui文件 及组件'''
        self.setObjectName('main_window')
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/app_main.ui'), self)
        self.setWindowTitle('量化系统')
        # 打开时显示Tab第二页
        self.ui.tabWidget.setCurrentIndex(1)
        # 板块tab
        self.plateTab=PlateTabWidget(self, 1)
        self.tabWidgetList.append(self.plateTab)
        self.ui.plateLayout.addWidget(self.plateTab)

        # 个股tab
        # self.stockTab = StockTabWidget(self, 2)
        # self.tabWidgetList.append(self.stockTab)
        # self.ui.stockLayout.addWidget(self.stockTab)

        # 自选分组tab
        # self.groupTab=GroupTabWidget(self, 3)
        # self.tabWidgetList.append(self.groupTab)
        # self.ui.groupLayout.addWidget(self.groupTab)

        # 主页面回测tab


    def listener(self):
        '''组件绑定事件'''
        self.ui.tabWidget.currentChanged.connect(self.changeTabSlot)
        self.ui.tabWidget.tabCloseRequested.connect(self.closeTabSlot)

    # == == == == == == == == == == == == == == == == == == == == 事件方法 == == == == == == == == == == == == == == == == == == == ==
    def changeTabSlot(self, position):
        """通知子tabWidget"""
        for tabWidget in self.tabWidgetList:
            tabWidget.changeTab(position)

    def closeTabSlot(self, position):
        """通知子tabWidget"""
        for tabWidget in self.tabWidgetList:
            tabWidget.closeTab(position)

    def showMain(self):
        '''重写show 窗口居中，大屏显示'''
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        self.showMaximized()

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/AppMainWindow.py
if __name__ == '__main__':
    app = QApplication(sys.argv)        # 创建QApplication
    app.setStyleSheet(qdarkstyle.load_stylesheet())     # 设置样式表

    main_window = AppMainWindow()
    sys.exit(app.exec_())
    print('{}：程序运行完毕！！！'.format(os.path.basename(__file__)))