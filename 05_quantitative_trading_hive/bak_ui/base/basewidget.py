from PyQt5 import QtWidgets

from app.stockapplication import StockApplication
from config.stockconfig import StockConfig


class BaseMainWindow(QtWidgets.QMainWindow):
    def __init__(self, application: StockApplication):
        super(BaseMainWindow, self).__init__()
        """
        注入应用类，用来关联没有ui时的的所有数据
        :param app:
        """
        self.application = application

    def config(self) -> StockConfig:
        return self.application.config


class BaseDockWidget(QtWidgets.QDockWidget):
    def __init__(self, application: StockApplication):
        super(BaseDockWidget, self).__init__()
        """
        注入应用类，用来关联没有ui时的的所有数据
        :param app:
        """
        self.application = application

    def config(self) -> StockConfig:
        return self.application.config
