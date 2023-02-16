import os
import sys
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import pandas as pd
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
import sys

from stockui.PdTable import PdTable


class MyTableView(QTableView):
    def __init__(self, parent=None):
        super(MyTableView, self).__init__(parent)
        self.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.setSelectionMode(QAbstractItemView.SingleSelection)
        self.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.setAlternatingRowColors(True)
        self.setShowGrid(False)
        self.setSortingEnabled(True)
        self.setFocusPolicy(Qt.NoFocus)
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setHighlightSections(False)
        self.verticalHeader().setVisible(False)
        self.verticalHeader().setDefaultSectionSize(22)
        self.setMouseTracking(True)
        self.setFrameShape(QFrame.NoFrame)
        self.setStyleSheet("QTableView{background-color: rgb(250, 250, 250);"
                           "alternate-background-color: rgb(255, 255, 225);}")

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Down:
            # self.ui.stockTableView.selectRow(self.ui.stockTableView.currentIndex().row() + 1)
            # index = self.currentIndex()
            # print(index.row())
            # print(self.model().data(index))
            print(self.model().index(self.currentIndex().row() + 1, 0).data())
        elif event.key() == Qt.Key_Up:
            print(self.model().index(self.currentIndex().row() - 1, 0).data())
        super(MyTableView, self).keyPressEvent(event)

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/test/222.py
if __name__ == '__main__':
    app = QApplication(sys.argv)
    # model = QStandardItemModel(4, 2)
    data = {'性别': ['男', '女', '女', '男', '男'],
            '姓名': ['小明', '小红', '小芳', '小强', '小美'],
            '年龄': [30, 21, 25, 24, 29]}
    df = pd.DataFrame(data, index=['No.1', 'No.2', 'No.3', 'No.4', 'No.5'],
                      columns=['姓名', '性别', '年龄', '职业'])

    model = PdTable(df)
    tableView = MyTableView()
    tableView.setModel(model)
    # for row in range(4):
    #     for column in range(2):
    #         item = QStandardItem("row %s, column %s" % (row, column))
    #         model.setItem(row, column, item)
    tableView.show()
    sys.exit(app.exec_())

