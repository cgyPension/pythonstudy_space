import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import qdarkstyle
from PyQt5.QtWidgets import QComboBox, QLineEdit, QListWidgetItem, QListWidget, QCheckBox, \
    QApplication, QVBoxLayout, QWidget, QPushButton, QDesktopWidget, QTextBrowser


# 继承下拉列表
class ComboCheckBox(QComboBox):
    def __init__(self, items):
        super().__init__()
        self.items = ['全选'] + items  # 下拉列表
        self.box_list = []  # 复选框列表
        self.text = QLineEdit()  # 输入框
        self.state = 0  # 选择中状态
        q = QListWidget()  # 列表单元组件
        for i in range(len(self.items)):
            self.box_list.append(QCheckBox())
            self.box_list[i].setText(self.items[i])
            item = QListWidgetItem(q)
            q.setItemWidget(item, self.box_list[i])
            if i == 0:
                self.box_list[i].stateChanged.connect(self.all_selected)
            else:
                self.box_list[i].stateChanged.connect(self.show_selected)
        self.setFixedWidth(100)
        self.text.setReadOnly(True)  # 设置输入框只读
        self.setLineEdit(self.text)
        self.setModel(q.model())
        self.setView(q)

    # 全选
    def all_selected(self):
        if self.state == 0:
            self.state = 1
            for i in range(1, len(self.items)):
                self.box_list[i].setChecked(True)
        else:
            self.state = 0
            for i in range(1, len(self.items)):
                self.box_list[i].setChecked(False)
        self.show_selected()

    def set_select(self, select_list):
        for key_rps in select_list:
            index = self.items.index(key_rps)
            self.box_list[index].setChecked(True)

    # 反选
    def get_selected(self):
        ret = []
        for i in range(1, len(self.items)):
            if self.box_list[i].isChecked():
                ret.append(self.box_list[i].text())
        return ret

    def show_selected(self):
        self.text.clear()
        ret = '; '.join(self.get_selected())
        self.text.setText(ret)


class MyWin(QWidget):
    def __init__(self):
        super(MyWin, self).__init__()
        self.MyUi()

    def MyUi(self):
        # self.resize(400, 600)
        self.setWindowTitle("信息")
        self.center()
        self.combo = ComboCheckBox(["Python", "Java", "Go", "C++", "JavaScript", "PHP"])
        b = QPushButton()
        b.setText('获取')
        b.clicked.connect(self.onClick)
        self.t = QTextBrowser()
        layout = QVBoxLayout()
        layout.addWidget(self.combo)
        layout.addWidget(b)
        layout.addWidget(self.t)
        self.setLayout(layout)

    def onClick(self):
        print(self.combo.text.text())
        self.t.setText(self.combo.text.text())

    # 实现居中方式2
    def center(self):
        # 获得屏幕坐标系
        screen = QDesktopWidget().screenGeometry()
        # 获得窗口坐标系
        size = self.geometry()
        # 获得窗口相关坐标
        L = (screen.width() - size.width()) // 2
        T = (screen.height() - size.height()) // 2
        # 移动窗口使其居中
        self.move(L, T)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setStyleSheet(qdarkstyle.load_stylesheet())  # 设置样式表
    w = MyWin()
    w.show()
    sys.exit(app.exec_())
