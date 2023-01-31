import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from PyQt5.QtWidgets import (QWidget, QLabel,
    QComboBox, QApplication)

'''QSplitter组件能让用户通过拖拽分割线的方式改变子窗口大小的组件。本例中我们展示用两个分割线隔开的三个QFrame组件'''
class Example(QWidget):

    def __init__(self):
        super().__init__()

        self.initUI()


    def initUI(self):

        self.lbl = QLabel("Ubuntu", self)

        combo = QComboBox(self)
        combo.addItem("Ubuntu")
        combo.addItem("Mandriva")
        combo.addItem("Fedora")
        combo.addItem("Arch")
        combo.addItem("Gentoo")

        combo.move(50, 50)
        self.lbl.move(50, 150)

        combo.activated[str].connect(self.onActivated)

        self.setGeometry(300, 300, 300, 200)
        self.setWindowTitle('QComboBox')
        self.show()


    def onActivated(self, text):

        self.lbl.setText(text)
        self.lbl.adjustSize()

# python /opt/code/pythonstudy_space/07_pyqt5/test/33_下拉选框.py
if __name__ == '__main__':

    app = QApplication(sys.argv)
    ex = Example()
    print(os.path.join(curPath, 'designer/cat.png'))
    sys.exit(app.exec_())
