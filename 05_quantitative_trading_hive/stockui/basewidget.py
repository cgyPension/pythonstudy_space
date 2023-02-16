import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from abc import abstractmethod
from PyQt5 import QtWidgets

class BaseTabWidget(QtWidgets.QWidget):
    """主面板的tab基类，注入mainWindow用来控制上下左右的QDockWidget"""
    def __init__(self, mainWindow: QtWidgets.QMainWindow,tabPosition: int):
        super(BaseTabWidget, self).__init__()
        self.mainWindow = mainWindow
        self.tabPosition = tabPosition

    @abstractmethod
    def changeTab(self, position):
        """
        外部切换时通知里面tab
        :param position:
        :return:
        """
        pass

    @abstractmethod
    def closeTab(self, position):
        """
        外部切换时通知里面tab
        :param position:
        :return:
        """
        pass
