import pandas as pd
from PyQt5.QtCore import QAbstractTableModel, Qt
from PyQt5.QtWidgets import QApplication, QTableView


class PdTable(QAbstractTableModel):
    def __init__(self, data=None, column=0):
        QAbstractTableModel.__init__(self)
        self._data = data
        # 默认降序排序，选中第三列
        self.order = 1
        self.column = column
        # 固定排序
        self.fix_sort = False
        self.last_sort_list = []

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

    def sort(self, column, order):
        self.column = column
        self.order = order
        self._notify_data_change()

        # 去重
        list1 = list(self._data['name'])
        self.last_sort_list = sorted(set(list1), key=list1.index)

    def notify_data(self, data):
        self._data = data
        self._notify_data_change()

    def _notify_data_change(self):
        if self.fix_sort:
            # 固定位置，则按上一次的排序来排
            if len(self.last_sort_list) == 0:
                self.layoutChanged.emit()
            else:
                # 按上一次的序列排序
                self.layoutAboutToBeChanged.emit()
                self._data['name'] = self._data['name'].astype('category')
                self._data['name'] = self._data['name'].cat.set_categories(self.last_sort_list)
                self._data.sort_values('name', inplace=True)
                self._data.reset_index(inplace=True, drop=True)
                self.layoutChanged.emit()
                pass
        else:
            col_name = self._data.columns.tolist()[self.column]
            self.layoutAboutToBeChanged.emit()
            self._data.sort_values(col_name, ascending=self.order == Qt.AscendingOrder, inplace=True)
            self._data.reset_index(inplace=True, drop=True)
            self.layoutChanged.emit()

    def get_data(self, row):
        return self._data.iloc[row]

    def data_len(self):
        return len(self._data)


if __name__ == '__main__':
    import sys

    app = QApplication(sys.argv)

    data = {'性别': ['男', '女', '女', '男', '男'],
            '姓名': ['小明', '小红', '小芳', '小强', '小美'],
            '年龄': [20, 21, 25, 24, 29]}
    df = pd.DataFrame(data, index=['No.1', 'No.2', 'No.3', 'No.4', 'No.5'],
                      columns=['姓名', '性别', '年龄', '职业'])

    model = PdTable(df)
    view = QTableView()
    view.setModel(model)
    view.setWindowTitle('Pandas')
    view.resize(410, 250)
    view.setAlternatingRowColors(True)
    view.show()

    sys.exit(app.exec_())