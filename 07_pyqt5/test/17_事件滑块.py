import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (QWidget, QLCDNumber, QSlider,
    QVBoxLayout, QApplication)

'''
所有的应用都是事件驱动的。事件大部分都是由用户的行为产生的，当然也有其他的事件产生方式，比如网络的连接，窗口管理器或者定时器等。调用应用的exec_()方法时，应用会进入主循环，主循环会监听和分发事件。
在事件模型中，有三个角色：
事件源
事件
事件目标
事件源就是发生了状态改变的对象。事件是这个对象状态改变的内容。事件目标是事件想作用的目标。事件源绑定事件处理函数，然后作用于事件目标身上。
'''
class Example(QWidget):

    def __init__(self):
        super().__init__()

        self.initUI()


    def initUI(self):

        lcd = QLCDNumber(self)
        sld = QSlider(Qt.Horizontal, self)

        vbox = QVBoxLayout()
        vbox.addWidget(lcd)
        vbox.addWidget(sld)

        self.setLayout(vbox)
        sld.valueChanged.connect(lcd.display)

        self.setGeometry(300, 300, 250, 150)
        self.setWindowTitle('Signal and slot')
        self.show()

# python /opt/code/pythonstudy_space/07_pyqt5/test/17_事件滑块.py
if __name__ == '__main__':

    app = QApplication(sys.argv)
    ex = Example()
    sys.exit(app.exec_())