import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from stockui.PdTable import PdTable
import pandas as pd
from PyQt5.QtWidgets import QApplication, QMenu, QVBoxLayout
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QStandardItem, QCursor
from PyQt5 import QtWidgets

class MyTableView(QtWidgets.QTableView):
    '''QTableView支持多行复制的二次开发'''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        # 创建QMenu信号事件
        self.customContextMenuRequested.connect(self.showMenu)
        self.contextMenu = QMenu(self)
        self.CP = self.contextMenu.addAction('复制')
        self.JQ = self.contextMenu.addAction('剪切')
        self.NT = self.contextMenu.addAction('粘贴')
        self.CP.triggered.connect(self.copy)
        self.JQ.triggered.connect(self.cut)
        self.NT.triggered.connect(self.paste)

    def del_tb_text(self):
        try:
            indexes = self.selectedIndexes()
            for index in indexes:
                row, column = index.row(), index.column()
                model = self.model()
                item = QStandardItem()
                model.setItem(row, column, item)
            self.setModel(model)
        except BaseException as e:
            print(e)
            return

    def paste_tb_text(self):
        try:
            indexes = self.selectedIndexes()
            for index in indexes:
                index = index
                break
            r, c = index.row(), index.column()
            text = QApplication.clipboard().text()
            ls = text.split('\n')
            ls1 = []
            for row in ls:
                ls1.append(row.split('\t'))
            model = self.model()
            rows = len(ls)
            columns = len(ls1[0])
            for row in range(rows):
                for column in range(columns):
                    item = QStandardItem()
                    item.setText((str(ls1[row][column])))
                    model.setItem(row + r, column + c, item)
        except Exception as e:
            print(e)
            return

    def selected_tb_text(self):
        try:
            indexes = self.selectedIndexes()  # 获取表格对象中被选中的数据索引列表
            indexes_dict = {}
            for index in indexes:  # 遍历每个单元格
                row, column = index.row(), index.column()  # 获取单元格的行号，列号
                if row in indexes_dict.keys():
                    indexes_dict[row].append(column)
                else:
                    indexes_dict[row] = [column]

            # 将数据表数据用制表符(\t)和换行符(\n)连接，使其可以复制到excel文件中
            text = ''
            for row, columns in indexes_dict.items():
                row_data = ''
                for column in columns:
                    data = self.model().item(row, column).text()
                    if row_data:
                        row_data = row_data + '\t' + data
                    else:
                        row_data = data

                if text:
                    text = text + '\n' + row_data
                else:
                    text = row_data
            return text
        except BaseException as e:
            print(e)
            return ''

    def copy(self):
        text = self.selected_tb_text()  # 获取当前表格选中的数据
        if text:
            clipboard = QApplication.clipboard()
            clipboard.setText(text)
            # pyperclip.copy(text) # 复制数据到粘贴板

    def cut(self):
        self.copy()
        self.del_tb_text()

    def paste(self):
        self.paste_tb_text()

    def showMenu(self, pos):
        # pos 鼠标位置
        # 菜单显示前,将它移动到鼠标点击的位置
        self.contextMenu.exec_(QCursor.pos())  # 在鼠标位置显示

    def keyPressEvent(self, event):  # 重写键盘监听事件
        # 监听 CTRL+C 组合键，实现复制数据到粘贴板
        if (event.key() == Qt.Key_C) and QApplication.keyboardModifiers() == Qt.ControlModifier:
            self.copy()  # 获取当前表格选中的数据
        elif (event.key() == Qt.Key_X) and QApplication.keyboardModifiers() == Qt.ControlModifier:
            self.cut()
        elif (event.key() == Qt.Key_V) and QApplication.keyboardModifiers() == Qt.ControlModifier:
            self.paste()
        else:
            super().keyPressEvent(event)

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/stockui/MyTableView2.py
if __name__ == '__main__':
    app = QApplication(sys.argv)
    # layout = QVBoxLayout()
    data = {'性别': ['男', '女', '女', '男', '男'],
            '姓名': ['小明', '小红', '小芳', '小强', '小美'],
            '年龄': [30, 21, 25, 24, 29]}
    df = pd.DataFrame(data, index=['No.1', 'No.2', 'No.3', 'No.4', 'No.5'],
                      columns=['姓名', '性别', '年龄', '职业'])

    model = PdTable(df)
    view = MyTableView()
    view.setModel(model)
    # 表格宽度的自适应调整
    view.horizontalHeader().setStretchLastSection(True)
    view.horizontalHeader().setSectionsClickable(True)

    view.setWindowTitle('Pandas 显示')
    view.resize(410, 250)
    view.setAlternatingRowColors(True)
    view.show()

    # layout.addWidget()

    sys.exit(app.exec_())