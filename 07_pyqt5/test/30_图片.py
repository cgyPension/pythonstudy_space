import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from PyQt5.QtWidgets import (QWidget, QHBoxLayout,
    QLabel, QApplication)
from PyQt5.QtGui import QPixmap

class Example(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        hbox = QHBoxLayout(self)
        # pixmap = QPixmap("cat.png")
        pixmap = QPixmap(os.path.join(curPath, 'designer/cat.png'))

        lbl = QLabel(self)
        lbl.setPixmap(pixmap)
        lbl.setScaledContents(True)  # 图片自适应QLabel大小


        hbox.addWidget(lbl)
        self.setLayout(hbox)

        self.move(300, 200)
        self.setWindowTitle('Red Rock')
        self.show()

# python /opt/code/pythonstudy_space/07_pyqt5/test/30_图片.py
if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Example()
    print(os.path.join(curPath, 'designer/cat.png'))
    sys.exit(app.exec_())
