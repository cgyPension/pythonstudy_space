import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
from PyQt5.QtWidgets import QAction

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from PyQt5.QtCore import Qt
from qtpy import uic
from stockui.UiDataUtils import *
from stockui.QSignalSlot import *
from stockui.basewidget import BaseTabWidget
from stockui.PdTable import PdTable
from stockui.view.PlateDetailWindow import PlateDetailWindow


class GroupTabWidget(BaseTabWidget,QSlot):
    """主界面板块tab"""
    def __init__(self, mainWindow, tabPosition):
        super(GroupTabWidget, self).__init__(mainWindow, tabPosition)
        self.initUi()
        self.listener()
        self.loadGroupTable_1()

    def initUi(self):
        self.ui = uic.loadUi(os.path.join(curPath, 'designer/tab_group.ui'), self)


    def loadGroupTable_1(self):
        '''加载 分组1'''
        # 表格模式
        self.Model_1 = PdTable(pd.DataFrame(columns=['code', 'name']))
        self.ui.groupTable_1.setModel(self.Model_1)
        # --------设置表格属性-------------------------------------------------
        # 表格宽度的自适应调整
        self.ui.groupTable_1.horizontalHeader().setStretchLastSection(True)
        self.ui.groupTable_1.horizontalHeader().setSectionsClickable(True)
        # ------gtj 隔行颜色设置
        self.ui.groupTable_1.setAlternatingRowColors(True)
        self.ui.groupTable_1.setStyleSheet("alternate-background-color: rgb(209, 209, 209)""; background-color: rgb(244, 244, 244);")
        # 表头排序
        self.ui.groupTable_1.setSortingEnabled(True)


    # == == == == == == == == == == == == == == == == == == == == 事件或小部件显示 == == == == == == == == == == == == == == == == == == == ==
    def listener(self):
        pass

