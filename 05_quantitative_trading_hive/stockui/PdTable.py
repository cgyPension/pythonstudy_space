import os
import sys
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from PyQt5 import QtCore
import pandas as pd
from PyQt5.QtWidgets import (QApplication, QTableView)
from PyQt5.QtCore import (QAbstractTableModel, Qt, QSortFilterProxyModel)
import operator

class PdTable(QAbstractTableModel):
    def __init__(self, data):
        QAbstractTableModel.__init__(self)
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    # 显示数据
    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.iloc[index.row(), index.column()])
        return None

    # 显示行和列头
    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._data.columns[col]
        elif orientation == Qt.Vertical and role == Qt.DisplayRole:
            return self._data.axes[0][col]
        return None

    def sort(self, col, order):
        """Sort table by given column number."""
        # print("col：：：：：",col)
        # print("self._data.columns[col]：：：：：",self._data.columns[col])
        col_name = self._data.columns[col]
        self.layoutAboutToBeChanged.emit()
        self._data = self._data.sort_values(col_name, ascending=order == Qt.AscendingOrder)
        self._data.reset_index(inplace=True, drop=True)
        self.layoutChanged.emit()

    def getRow(self, row):
        '''得到一行数据'''
        return self._data.iloc[row]

    def getDataLen(self):
        return len(self._data)

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/PdTable.py
if __name__ == '__main__':
    app = QApplication(sys.argv)
    data = {'性别': ['男', '女', '女', '男', '男'],
            '姓名': ['小明', '小红', '小芳', '小强', '小美'],
            '年龄': [30, 21, 25, 24, 29]}
    df = pd.DataFrame(data, index=['No.1', 'No.2', 'No.3', 'No.4', 'No.5'],
                      columns=['姓名', '性别', '年龄', '职业'])

    model = PdTable(df)
    view = QTableView()
    view.setModel(model)
    # 表格宽度的自适应调整
    view.horizontalHeader().setStretchLastSection(True)
    view.horizontalHeader().setSectionsClickable(True)
    # ------gtj 隔行颜色设置
    view.setAlternatingRowColors(True)
    view.setStyleSheet(
        "alternate-background-color: rgb(209, 209, 209)""; background-color: rgb(244, 244, 244);")
    # 表头排序
    view.setSortingEnabled(True)
    view.horizontalHeader().setStyleSheet(
        "::section{background-color: pink; color: blue; font-weight: bold}")

    # 设置键盘事件
    # view.keyPressEvent = self.keyPressEvent


    view.setWindowTitle('Pandas 显示')
    view.resize(410, 250)
    view.setAlternatingRowColors(True)
    view.show()

    sys.exit(app.exec_())