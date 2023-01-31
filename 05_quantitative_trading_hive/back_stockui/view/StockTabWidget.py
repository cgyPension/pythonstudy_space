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
from stockui.UiDataUtils import *


class StockTabWidget(QtWidgets.QWidget):
    """个股tab布局面板"""
    def __init__(self, mainWindow, tabPosition):
        super(StockTabWidget, self).__init__(mainWindow, tabPosition)

        self._init_view()
        self._init_listener()
        self._init_data()

    def _init_view(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/tab_stock.ui'), self)

    def _init_listener(self):
        pass

    def _init_data(self):
        pass

    def current_change_tab(self, position):
        pass

    def tab_close_requested(self, position):
        pass
