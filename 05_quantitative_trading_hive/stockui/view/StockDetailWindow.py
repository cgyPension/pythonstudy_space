import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
from PyQt5.QtCore import Qt

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import pandas as pd
from PyQt5.QtWidgets import QGraphicsScene, QAbstractItemView, QHeaderView
from qtpy import uic
import qdarkstyle
from PyQt5 import QtWidgets, QtWebEngineWidgets
from PyQt5.QtWidgets import QApplication
from stockui.UiDataUtils import *
from stockui.PdTable import PdTable
from stockui.UiPlotly import UiPlotly


class StockDetailWindow(QtWidgets.QMainWindow):
    """板块详情"""
    def __init__(self):
        super(StockDetailWindow, self).__init__()
        self.stockCode = None
        self.stockName = None
        self.start_date, self.end_date = None, None
        self.initUi()
        self.uiDataUtils = UiDataUtils()
        self.UiPlotly = UiPlotly()
        self.listener()
        # 初始化日期 取最近一年
        self.ui.startDateEdit.setDate(self.uiDataUtils.q_start_date)
        self.ui.endDateEdit.setDate(self.uiDataUtils.q_end_date)


    def initUi(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/stock_detail_main.ui'), self)
        # 添加plotly的承载体
        self.browserStockK = QtWebEngineWidgets.QWebEngineView(self)
        self.ui.kLayout.addWidget(self.browserStockK)


    def loadStockTable(self, stock_df):
        '''加载左侧栏'''
        if stock_df.empty:
            print('{} 股票数据为空！！！'.format(date))
            return
        stock_df = stock_df[['code', 'name','change_percent', 'volume_ratio_1d','volume_ratio_5d','turnover_rate','industry_plate','concept_plates','stock_label_names','sub_factor_names','sub_factor_score','hot_rank']]
        # 表格模式
        self.dfModel = PdTable(stock_df)
        self.ui.stockTableView_2.setModel(self.dfModel)
        # --------设置表格属性-------------------------------------------------
        self.ui.stockTableView_2.horizontalHeader().setStretchLastSection(True)
        self.ui.stockTableView_2.horizontalHeader().setSectionsClickable(True)
        self.ui.stockTableView_2.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.ui.stockTableView_2.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.ui.stockTableView_2.setSelectionMode(QAbstractItemView.SingleSelection)
        self.ui.stockTableView_2.horizontalHeader().setStretchLastSection(True)
        self.ui.stockTableView_2.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.ui.stockTableView_2.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        # 设置选中颜色
        self.ui.stockTableView_2.setStyleSheet("QTableView::item:selected{background-color: rgb(0, 170, 255);}")
        # 设置键盘事件
        self.ui.stockTableView_2.keyPressEvent = self.keyPressEvent

    def loadStockKGraph(self):
        '''加载股票K线面图'''
        df = self.uiDataUtils.query_stock(self.stockCode, self.start_date, self.end_date)
        if df.empty:
            print('{} {}~{} 板块数据为空！！！'.format(self.stockCode,self.start_date, self.end_date))
            return
        melt_list = ['rps_5d', 'rps_10d', 'rps_20d', 'rps_50d']
        threshold = 90
        fig = self.UiPlotly.k_rps_fig(df,melt_list,threshold,is_plate=False)
        self.browserStockK.setHtml(fig.to_html(include_plotlyjs='cdn'))
        
    # == == == == == == == == == == == == == == == == == == == == 事件或小部件显示 == == == == == == == == == == == == == == == == == == == ==
    def listener(self):
        self.ui.startDateEdit.dateChanged.connect(self.startDateChangeSlot)
        self.ui.endDateEdit.dateChanged.connect(self.endDateChangeSlot)
        self.ui.loadK.clicked.connect(self.loadStockKGraph)
        # self.ui.stockTableView_2.clicked.connect(self.stockCodeChange)

    def keyPressEvent(self, event):
        self.clipboard = QApplication.clipboard()
        if event.key() == Qt.Key_Down:
            self.ui.stockTableView_2.selectRow(self.ui.stockTableView_2.currentIndex().row() + 1)
            index = self.ui.stockTableView_2.currentIndex()
            code = self.ui.stockTableView_2.model().data(index)
            self.stockCode = code
            self.loadStockKGraph()
            self.clipboard.setText(code)
        elif event.key() == Qt.Key_Up:
            self.ui.stockTableView_2.selectRow(self.ui.stockTableView_2.currentIndex().row() - 1)
            index = self.ui.stockTableView_2.currentIndex()
            code = self.ui.stockTableView_2.model().data(index)
            self.stockCode = code
            self.loadStockKGraph()
            self.clipboard.setText(code)
        elif event.key() == Qt.Key_Return:
            print(self.ui.stockTableView_2.currentIndex().row())
        else:
            super(StockDetailWindow, self).keyPressEvent(event)

    def stockCodeChange(self, item):
        """单击左侧栏 加载股票k线图"""
        row = item.row()
        model = item.model()
        dfRow = model.getRow(row)
        code = dfRow['code']
        # name = dfRow['name']
        # print(name)
        self.stockCode = code
        self.loadStockKGraph()

    def startDateChangeSlot(self):
        self.start_date = self.ui.startDateEdit.date().toString('yyyy-MM-dd')

    def endDateChangeSlot(self):
        self.end_date = self.ui.endDateEdit.date().toString('yyyy-MM-dd')

    def showWindow(self, stock_df,dfRow):
        code = dfRow['code']
        name = dfRow['name']
        if self.stockCode == code:
            return
        self.stockCode = code
        self.stockName = name
        self.loadStockTable(stock_df)
        self.loadStockKGraph()
        self.setWindowTitle(name)
        self.showMaximized()

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/back_stockui/view/BlockIndWindow.py
if __name__ == '__main__':
    # 创建QApplication
    app = QApplication(sys.argv)
    # 设置样式表
    app.setStyleSheet(qdarkstyle.load_stylesheet())
    # 主窗口
    main_window = StockDetailWindow()
    main_window.show_main_window()

    sys.exit(app.exec_())