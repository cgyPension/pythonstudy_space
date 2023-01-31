import pandas as pd
import plotly.express as px
from PyQt5 import QtWebEngineWidgets
from PyQt5.QtCore import Qt
from qtpy import uic

from ui.base.basewidget import BaseTabWidget
from ui.mvpimpl.BackTestViewImpl import BackTestViewImpl
from ui.utils.uiutils import *
from ui.view.DwStrategyResult import DwStrategyResult
from ui.view.DwStrategyStockPool import DwStrategyStockPool


class BackTestTabWidget(BaseTabWidget, BackTestViewImpl):
    """
    回测tab布局面板
    """

    def __init__(self, mainWindow, application, tabPosition):
        super(BackTestTabWidget, self).__init__(mainWindow, application, tabPosition)

        self._init_view()
        self._init_listener()
        self._init_data()

    def _init_view(self):
        self.ui = uic.loadUi(os.path.join(ui_dir_path, 'designer/tab_back_test.ui'), self)

        # 添加plotly的承载体
        self.browser = QtWebEngineWidgets.QWebEngineView(self)
        self.ui.vl_plot_container.addWidget(self.browser)

        # 已交易的股票dw
        self.dw_strategy_stock_pool = DwStrategyStockPool(self.application)
        self.mainWindow.addDockWidget(Qt.RightDockWidgetArea, self.dw_strategy_stock_pool)
        self.dw_strategy_stock_pool.setVisible(True)

        # 回测结果dw
        self.dw_strategy_result = DwStrategyResult(self.application)
        self.mainWindow.addDockWidget(Qt.BottomDockWidgetArea, self.dw_strategy_result)
        self.dw_strategy_result.setVisible(True)

        # 设置拖动的初始化窗口比例
        self.ui.splitter.setStretchFactor(0, 10)
        self.ui.splitter.setStretchFactor(1, 90)

        # 设置时间
        self.ui.dte_end.setDate(trade_date(QDate.currentDate(), self.config()))
        end_date = self.ui.dte_end.date()
        # 开始时间为当前时间前推一年
        start_date = end_date.addDays(-365)
        self.ui.dte_start.setDate(start_date)
        self._start_date = start_date.toString('yyyy-MM-dd')
        self._end_date = end_date.toString('yyyy-MM-dd')

    def _init_listener(self):
        self.ui.pb_start.clicked.connect(self.start_back_test)
        self.ui.pb_load_data.clicked.connect(self.load_stock_data)
        self.ui.dte_start.dateChanged.connect(self._start_date_change)
        self.ui.dte_end.dateChanged.connect(self._end_date_change)
        self.ui.treeWidget.clicked.connect(self.tree_widget_item_clicked)

    def _init_data(self):
        pass

    def tree_widget_item_clicked(self, item):
        item = self.ui.treeWidget.currentItem()
        print("key=%s ,value=%s" % (item.text(0), item.text(1)))

    def _start_date_change(self, date):
        """
        改变开始时间
        :param date:
        :return:
        """
        date = trade_date(date, self.config())
        self.ui.dte_start.setDate(date)
        self._start_date = date.toString('yyyy-MM-dd')

    def _end_date_change(self, date):
        """
        改变结束时间
        :param date:
        :return:
        """
        date = trade_date(date, self.config())
        self.ui.dte_end.setDate(date)
        self._end_date = date.toString('yyyy-MM-dd')

    def start_back_test(self):
        # 开始跑回测
        self.presenter_impl().start_run(self._start_date, self._end_date)

    def load_stock_data(self):
        self.presenter_impl().load_data(self._start_date, self._end_date)
        self.dw_strategy_stock_pool.show()

    def load_data_success(self, data: pd.DataFrame):
        """
        数据加载成功
        :param data:
        :return:
        """
        self.dw_strategy_stock_pool.set_data(data)

    def strategy_result(self, zx_df, cc_df, analyzer_df, tl_df, trace_dict):
        """
        回测结果显示
        :param zx_df:
        :param cc_df:
        :param analyzer_df:
        :param tl_df:
        :param trace_dict: 股票交易字典，用来绘制买卖信号
        :return:
        """

        analyzer_df.drop(columns='index', inplace=True)

        # 交易订单详情表格
        self.dw_strategy_result.show()
        tl_df.to_csv('tl_df.csv')
        self.dw_strategy_result.set_data(analyzer_df, tl_df)
        # 绘制收益率
        self.plotly_result(zx_df)
        # 已经交易的股票
        self.dw_strategy_stock_pool.show()
        self.dw_strategy_stock_pool.set_data(trace_dict)

    def plotly_result(self, zx_df):
        """
        绘制结果
        :return:
        """

        # df = px.data.tips()
        # fig = px.box(df, x="day", y="total_bill", color="smoker")
        # fig.update_traces(quartilemethod="exclusive")  # or "inclusive", or "linear" by default
        # self.browser.setHtml(fig.to_html(include_plotlyjs='cdn'))

        zx_fig = px.line(
            zx_df,  # 绘图数据
            x=zx_df['交易日期'],  # x轴标签
            y=zx_df['累计收益率'],
            color='标签',
            color_discrete_sequence=['#FF0000', '#2776B6', '#8F4E4F'],
            hover_data={'标签': False,
                        '交易日期': "|%Y-%m-%d"}
        )
        zx_fig.update_xaxes(
            ticklabelmode='instant',  # ticklabelmode模式：居中 'instant', 'period'
            tickformatstops=[
                dict(dtickrange=[3600000, "M1"], value='%Y-%m-%d'),
                dict(dtickrange=["M1", "M12"], value='%Y-%m'),
                dict(dtickrange=["M12", None], value='%Y')
            ],
            rangeselector=dict(
                # 增加固定范围选择
                buttons=list([
                    dict(count=1, label='1M', step='month', stepmode='backward'),
                    dict(count=6, label='6M', step='month', stepmode='backward'),
                    dict(count=1, label='1Y', step='year', stepmode='backward'),
                    dict(count=1, label='YTD', step='year', stepmode='todate'),
                    dict(step='all')]))
        )
        zx_fig.update_yaxes(ticksuffix='%', side='right', ticklabelposition='inside')
        zx_fig.update_layout(xaxis_title=None, yaxis_title=None, legend_title_text=None,
                             legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        self.browser.setHtml(zx_fig.to_html(include_plotlyjs='cdn'))

    def current_change_tab(self, position):
        # DockWidget 的显示和隐藏
        self.dw_strategy_stock_pool.setVisible(True if position == self.tabPosition else False)
        self.dw_strategy_result.setVisible(True if position == self.tabPosition else False)

        if position != self.tabPosition:
            pass
        else:
            pass

    def tab_close_requested(self, position):
        pass
