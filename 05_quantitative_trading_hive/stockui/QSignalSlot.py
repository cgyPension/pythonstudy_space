import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import pandas as pd
from PyQt5.QtCore import QObject,pyqtSignal
from stockui.UiDataUtils import *
from stockui.PdTable import PdTable

class QSignal(QObject):
    ''''自定义信号'''
    sendmsg=pyqtSignal(str)
    senddf=pyqtSignal(pd.DataFrame)

    def __init__(self):
        super(QSignal, self).__init__()

    def run(self):
        #发射信号
        self.sendmsg.emit("Python数据分析实例")


class QSlot(QObject):
    '''公共槽函数 事件函数'''
    def __init__(self):
        super(QSlot, self).__init__()

    def get(self,msg):
        print("QSlot get msg=>"+msg)

    def lastDaySlot(self):
        """上一日"""
        date = self.ui.dateTimeEdit.date()
        date = date.addDays(-1)
        self.ui.dateTimeEdit.setDate(date)

    def nextDaySlot(self):
        """日期递增"""
        date = self.ui.dateTimeEdit.date()
        date = date.addDays(1)
        self.ui.dateTimeEdit.setDate(date)



    # == == == == == == == == == == == == == == == == == == == == 加载数据 == == == == == == == == == == == == == == == == == == == ==


if __name__ =="__main__":
    # 把信号绑定到槽对象中的槽函数get()上，槽函数能接受到所发射的信号”Python数据分析实例“，数据成功传递。
    send =QSignal()
    slot =QSlot()
    #**************将信号与槽函数绑定起来**************开始
    send.sendmsg.connect(slot.get)
    send.run()

    #*************把信号与槽函数连接断开***************开始
    send.sendmsg.disconnect(slot.get)

    send.run()
    #*************把信号与槽函数连接断开***************结束