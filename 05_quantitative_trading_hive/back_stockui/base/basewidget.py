from abc import abstractmethod

from PyQt5 import QtWidgets



class BaseTabWidget(QtWidgets.QWidget):
    """
    主面板的tab基类，注入mainWindow用来控制上下左右的QDockWidget
    """

    def __init__(self, mainWindow: QtWidgets.QMainWindow,tabPosition: int):
        super(BaseTabWidget, self).__init__()
        self.mainWindow = mainWindow
        self.tabPosition = tabPosition

    @abstractmethod
    def current_change_tab(self, position):
        """
        外部切换时通知里面tab
        :param position:
        :return:
        """
        pass

    @abstractmethod
    def tab_close_requested(self, position):
        """
        外部切换时通知里面tab
        :param position:
        :return:
        """
        pass
