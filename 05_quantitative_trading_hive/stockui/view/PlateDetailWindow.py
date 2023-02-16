import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
from PyQt5.QtCore import Qt
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import pandas as pd
from PyQt5.QtWidgets import QGraphicsScene, QAbstractItemView, QHeaderView, QTableView
from qtpy import uic
import qdarkstyle
from PyQt5 import QtWidgets, QtWebEngineWidgets
from PyQt5.QtWidgets import QApplication
from stockui.UiDataUtils import *
from stockui.PdTable import PdTable
from stockui.UiPlotly import UiPlotly
from stockui.widgets.MyTableView import MyTableView


class PlateDetailWindow(QtWidgets.QMainWindow):
    """板块详情"""
    def __init__(self):
        super(PlateDetailWindow, self).__init__()
        self.plateCode = None
        self.plateName = None
        self.stockCode = None
        self.start_date, self.end_date = None, None
        self.initUi()
        self.uiDataUtils = UiDataUtils()
        self.UiPlotly = UiPlotly()
        self.listener()
        # 初始化日期 取最近一年
        self.ui.startDateEdit.setDate(self.uiDataUtils.q_start_date)
        self.ui.endDateEdit.setDate(self.uiDataUtils.q_end_date)


    def initUi(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/plate_detail_main.ui'), self)
        # 添加plotly的承载体
        self.browserPlateK = QtWebEngineWidgets.QWebEngineView(self)
        self.browserStockK = QtWebEngineWidgets.QWebEngineView(self)
        # self.browserStockRps = QtWebEngineWidgets.QWebEngineView(self)
        # self.ui.rpsLayout.addWidget(self.browserStockRps)
        self.ui.kLayout.addWidget(self.browserPlateK)
        self.ui.rpsLayout.addWidget(self.browserStockK)

        # 股票详情
        # self.stockDetailWindow = StockDetailWindow()


    def loadPlateKGraph(self):
        '''加载板块k线图'''
        df = self.uiDataUtils.query_plate(self.plateCode, self.start_date, self.end_date)
        # print('loadPlateKGraph！！！',self.start_date, self.end_date)
        if df.empty:
            print('{}~{} 板块数据为空！！！'.format(self.start_date, self.end_date))
            return
        melt_list = ['rps_5d', 'rps_10d', 'rps_15d', 'rps_20d', 'rps_50d']
        threshold = 90
        fig = self.UiPlotly.k_rps_fig(df,melt_list,threshold,is_plate=True)
        self.browserPlateK.setHtml(fig.to_html(include_plotlyjs='cdn'))

    def loadPlateStock(self):
        '''加载板块成分股'''
        df = self.uiDataUtils.query_plate_stock(self.plateName)
        if df.empty:
            print('{} 成分股为空！！！'.format(self.plateName))
            return
        # 设置默认成分股第一个
        self.stockCode = df['code'].iloc[0]
        # 表格模式
        dfModel = PdTable(df)
        self.ui.stockTableView.setModel(dfModel)
        # --------设置表格属性-------------------------------------------------
        self.ui.stockTableView.horizontalHeader().setStretchLastSection(True)
        self.ui.stockTableView.horizontalHeader().setSectionsClickable(True)
        self.ui.stockTableView.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.ui.stockTableView.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.ui.stockTableView.setSelectionMode(QAbstractItemView.SingleSelection)
        self.ui.stockTableView.horizontalHeader().setStretchLastSection(True)
        self.ui.stockTableView.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        # self.ui.stockTableView.verticalHeader().setSectionResizeMode(QHeaderView.Interactive)
        # 随内容自动分配
        # self.ui.stockTableView.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.ui.stockTableView.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        # 设置隔行变色
        self.ui.stockTableView.setAlternatingRowColors(True)
        # 设置用户可以拖动行
        self.ui.stockTableView.verticalHeader().setSectionsMovable(True)
        # 设置选中颜色
        self.ui.stockTableView.setStyleSheet("QTableView::item:selected{background-color: rgb(0, 170, 255);}")

        # 设置键盘事件
        self.ui.stockTableView.keyPressEvent = self.keyPressEvent

    def querySlot(self, text):
        '''查询工具'''
        try:
            df = self.uiDataUtils.query_plate_stock(self.plateName,text)
            if df.empty:
                print('{} 成分股为空！！！'.format(self.plateName))
                return
            # 设置默认成分股第一个
            self.stockCode = df['code'].iloc[0]
            # 表格模式
            dfModel = PdTable(df)
            self.ui.stockTableView.setModel(dfModel)

        except Exception as e:
            print(e)

    def loadStockRpsGraph(self):
        '''加载股票rps图'''
        df = self.uiDataUtils.query_stock(self.stockCode, self.start_date, self.end_date)
        if df.empty:
            print('{} {}~{} 板块数据为空！！！'.format(self.stockCode,self.start_date, self.end_date))
            return
        melt_list = ['rps_5d', 'rps_10d', 'rps_20d', 'rps_50d']
        threshold = 90
        fig = self.UiPlotly.zx_fig(df, melt_list, threshold)
        self.browserStockRps.setHtml(fig.to_html(include_plotlyjs='cdn'))

    def loadStockKGraph(self):
        '''加载股票k线图'''
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
        self.ui.loadK.clicked.connect(self.loadPlateKGraph)
        self.ui.loadK.clicked.connect(self.loadStockKGraph)
        # self.ui.stockTableView.clicked.connect(self.stockCodeChange)
        self.ui.query.clicked.connect(lambda: self.querySlot(self.ui.queryText.text()))
        # 回车
        self.ui.queryText.returnPressed.connect(lambda: self.querySlot(self.ui.queryText.text()))

    def keyPressEvent(self, event):
        # 创建剪切板
        self.clipboard = QApplication.clipboard()
        if event.key() == Qt.Key_Down:
            self.ui.stockTableView.selectRow(self.ui.stockTableView.currentIndex().row() + 1)
            index = self.ui.stockTableView.currentIndex()
            code = self.ui.stockTableView.model().data(index)
            self.stockCode = code
            self.loadStockKGraph()
            self.clipboard.setText(code)
        elif event.key() == Qt.Key_Up:
            self.ui.stockTableView.selectRow(self.ui.stockTableView.currentIndex().row() - 1)
            index = self.ui.stockTableView.currentIndex()
            code = self.ui.stockTableView.model().data(index)
            self.stockCode = code
            self.loadStockKGraph()
            self.clipboard.setText(code)
        elif event.key() == Qt.Key_Return:
            print(self.ui.stockTableView.currentIndex().row())
        else:
            super(PlateDetailWindow, self).keyPressEvent(event)

    def stockCodeChange(self, item):
        """单击成分股 加载股票rps图"""
        row = item.row()
        model = item.model()
        dfRow = model.getRow(row)
        code = dfRow['code']
        # name = dfRow['name']
        # print(name)
        self.stockCode = code
        # self.loadStockRpsGraph()
        self.loadStockKGraph()

    def startDateChangeSlot(self):
        self.start_date = self.ui.startDateEdit.date().toString('yyyy-MM-dd')

    def endDateChangeSlot(self):
        self.end_date = self.ui.endDateEdit.date().toString('yyyy-MM-dd')

    def showWindow(self, dfRow):
        code = dfRow['code']
        name = dfRow['name']
        if self.plateCode == code:
            return
        self.plateCode = code
        self.plateName = name
        self.loadPlateKGraph()
        self.loadPlateStock()
        # self.loadStockRpsGraph()
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
    main_window = PlateDetailWindow()
    main_window.show_main_window()

    sys.exit(app.exec_())