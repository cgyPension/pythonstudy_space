import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from PyQt5.QtWidgets import (QWidget, QLabel,
    QLineEdit, QApplication)

'''QLineEdit组件提供了编辑文本的功能，自带了撤销、重做、剪切、粘贴、拖拽等功能'''
class Example(QWidget):

    def __init__(self):
        super().__init__()

        self.initUI()


    def initUI(self):

        self.lbl = QLabel(self)
        qle = QLineEdit(self)

        qle.move(60, 100)
        self.lbl.move(60, 40)

        qle.textChanged[str].connect(self.onChanged)

        self.setGeometry(300, 300, 280, 170)
        self.setWindowTitle('QLineEdit')
        self.show()


    def onChanged(self, text):

        self.lbl.setText(text)
        self.lbl.adjustSize()

# python /opt/code/pythonstudy_space/07_pyqt5/test/31_行编辑.py
if __name__ == '__main__':

    app = QApplication(sys.argv)
    ex = Example()
    print(os.path.join(curPath, 'designer/cat.png'))
    sys.exit(app.exec_())
