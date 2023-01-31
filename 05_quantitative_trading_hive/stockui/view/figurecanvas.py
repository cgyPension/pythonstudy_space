import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import matplotlib
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas

from stockui.stockplot import StockPlot, RpsPlot, ContrastRpsPlot

matplotlib.use('Qt5Agg')


class MyFigureCanvas(FigureCanvas):
    """
    通过继承FigureCanvas类，使得该类既是一个PyQt5的Qwidget，
    又是一个matplotlib的FigureCanvas，这是连接pyqt5与matplotlib的关键
    """

    def __init__(self, parent=None):
        # 创建一个Figure
        self.sp = StockPlot(3)
        FigureCanvas.__init__(self, self.sp.fig)  # 初始化父类
        self.setParent(parent)

    def set_data(self, data):
        self.sp.set_data(data)

    def kline(self, rps_list, warning_value):
        """
        绘制k线
        :return:
        """
        self.sp.before_draw()
        self.sp.main_plot()
        self.sp.ema_plot([5, 10, 20, 50, 120, 250], False)
        self.sp.volume_addplot()
        self.sp.macd_addplot()
        self.sp.rps_addplot(rps_list, warning_value)
        self.sp.plot(False)


class StockFigureCanvas(FigureCanvas):
    """
    通过继承FigureCanvas类，使得该类既是一个PyQt5的Qwidget，
    又是一个matplotlib的FigureCanvas，这是连接pyqt5与matplotlib的关键
    """

    def __init__(self, parent=None):
        # 创建一个Figure
        self.sp = StockPlot(4)
        FigureCanvas.__init__(self, self.sp.fig)  # 初始化父类
        self.setParent(parent)

    def set_data(self, data, block_data):
        self._block_data = block_data
        self.sp.set_data(data)

    def kline(self, rps_list, warning_value, block_warning_value):
        """
        绘制k线
        :return:
        """
        self.sp.before_draw()
        self.sp.main_plot()
        self.sp.ema_plot([5, 10, 20, 50, 120, 250], False)
        self.sp.volume_addplot()
        self.sp.macd_addplot()
        self.sp.rps_addplot(rps_list, warning_value)
        # 板块的rps
        self.sp.other_rps_addplot(self._block_data, rps_list, block_warning_value, 'block-rps')
        self.sp.plot(False)


class RpsSingleFigureCanvas(FigureCanvas):
    """
    rps单指标
    通过继承FigureCanvas类，使得该类既是一个PyQt5的Qwidget，
    又是一个matplotlib的FigureCanvas，这是连接pyqt5与matplotlib的关键
    """

    def __init__(self, parent=None):
        # 创建一个Figure
        self.sp = RpsPlot()
        FigureCanvas.__init__(self, self.sp.fig)  # 初始化父类
        self.setParent(parent)

    def set_data(self, data):
        self.sp.set_data(data)

    def kline(self, rps_list, warning_value):
        """
        绘制k线
        :return:
        """
        self.sp.before_draw()
        self.sp.plot(rps_list, warning_value)


class ContrastRpsFigureCanvas(FigureCanvas):
    """
    rps单指标
    通过继承FigureCanvas类，使得该类既是一个PyQt5的Qwidget，
    又是一个matplotlib的FigureCanvas，这是连接pyqt5与matplotlib的关键
    """

    def __init__(self, parent=None):
        # 创建一个Figure
        self.sp = ContrastRpsPlot()
        FigureCanvas.__init__(self, self.sp.fig)  # 初始化父类
        self.setParent(parent)

    def set_data(self, data, block_data):
        self.sp.set_data(data, block_data)

    def kline(self, rps_list, warning_value):
        """
        绘制k线
        :return:
        """
        self.sp.before_draw()
        self.sp.plot(rps_list, warning_value)

    def clear_figure(self):
        self.sp.clear()
