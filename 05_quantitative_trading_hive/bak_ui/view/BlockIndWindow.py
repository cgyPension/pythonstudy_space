import pandas as pd
from PyQt5.QtWidgets import QGraphicsScene
from qtpy import uic

from database import stockdatabase as sdb
from ui.base.basewidget import BaseMainWindow
from ui.utils.uiutils import *
from ui.view.StockIndWindow import StockIndWindow
from ui.view.figurecanvas import MyFigureCanvas, ContrastRpsFigureCanvas
from ui.viewmodel.pdviewmodel import PdTable
from ui.widget.ComboCheckBox import ComboCheckBox


class BlockIndWindow(BaseMainWindow):
    """
    板块详情
    """

    def __init__(self, application):
        super(BlockIndWindow, self).__init__(application)

        # 板块的属性
        self._block_code = ''
        self._block_start_date = ''
        self._block_end_date = ''
        self._block_data = None
        # rps红值提醒
        self.block_rps_list = ['rps10', 'rps20', 'rps50']
        self.block_warning_value = 90
        self.canvas = None  # 主视图画布
        self.stock_brief_canvas = None  # 底部股票摘要画布

        # 板块成分股，指定初始排序列
        self.all_model = PdTable(column=1)
        self.my_model = PdTable(column=1)

        self.stock_df = pd.DataFrame()
        # 选中股的属性
        # 表格
        self.stock_model = PdTable(column=2)
        self._stock_code = ''
        self._stock_start_date = ''
        self._stock_end_date = ''
        self._stock_data = None
        # rps红值提醒
        self.stock_rps_list = ['rps10', 'rps20', 'rps50']
        self.stock_warning_value = 87

        # 股票详情
        self.stock_details_window = StockIndWindow(self.application)

        self._init_view()
        # 初始化底部板块
        self._init_bottom_view()
        self._init_listener()
        self._init_bottom_listener()

    def _init_view(self):

        self.ui = uic.loadUi(os.path.join(ui_dir_path, 'designer/main_block_indicator.ui'), self)

        # 添加rps勾选布局
        self.combo = ComboCheckBox(['rps05', 'rps10', 'rps20', 'rps50', 'rps120', 'rps250'])
        self.ui.layout_rps.addWidget(self.combo)
        self.combo.set_select(self.block_rps_list)

        self.ui.dte_end.setDate(trade_date(QDate.currentDate(), self.config()))
        end_date = self.ui.dte_end.date()
        # 开始时间为当前时间前推一年
        start_date = end_date.addDays(-365)
        self.ui.dte_start.setDate(start_date)
        self._block_start_date = start_date.toString('yyyy-MM-dd')
        self._block_end_date = end_date.toString('yyyy-MM-dd')
        # k线画布
        self.graphic_scene = QGraphicsScene()

        # 设置主链接控件占比，即0号窗口大小占80 %，即1号窗口大小占20 %
        # 摸鱼时刻，主图搞小点
        is_to_catch_fish = True
        self.ui.splitter_main.setStretchFactor(0, 90 if not is_to_catch_fish else 10)
        self.ui.splitter_main.setStretchFactor(1, 10 if is_to_catch_fish else 90)

    def _init_bottom_view(self):
        """
        底部成分股view初始化
        :return:
        """
        # 底部先隐藏
        # self.ui.splitter_bottom.setVisible(False)

        # 添加rps勾选布局
        self.combo_bottom = ComboCheckBox(['rps05', 'rps10', 'rps20', 'rps50', 'rps120', 'rps250'])
        self.ui.layout_stock_rps.addWidget(self.combo_bottom)
        self.combo_bottom.set_select(self.stock_rps_list)

        # 先把时间初始化好
        date_str = self.config().rps_stock_start_time()
        end_date = QDate.fromString(date_str, 'yyyy-MM-dd')
        self.ui.dte_block_date.setDate(trade_date(end_date, self.config()))

        self.ui.dte_stock_end.setDate(trade_date(QDate.currentDate(), self.config()))
        end_date = self.ui.dte_stock_end.date()
        # 开始时间为当前时间前推一年
        start_date = end_date.addDays(-365)
        self.ui.dte_stock_start.setDate(start_date)
        self._stock_start_date = start_date.toString('yyyy-MM-dd')
        self._stock_end_date = end_date.toString('yyyy-MM-dd')

        # 个股线画布
        self.stock_graphic_scene = QGraphicsScene()

    def _init_listener(self):
        self.ui.dte_start.dateChanged.connect(self._start_date_change)
        self.ui.dte_end.dateChanged.connect(self._end_date_change)
        self.ui.pb_refresh_rps.clicked.connect(self._refresh_rps)
        self.ui.cb_show_bottom.stateChanged.connect(self.show_bottom_brief)

        # 板块股票列表点击监听
        self.ui.tv_all_stock.doubleClicked.connect(self.double_clicked_bottom_table_view)
        self.ui.tv_my_stock.doubleClicked.connect(self._double_clicked_my_table)

        self.ui.tv_all_stock.clicked.connect(self.clicked_bottom_table_view_item)

    def on_block_date_change(self, date):
        self.load_block_stock(date)

    def fix_block_sort(self, state):
        self.stock_model.fix_sort = state == 2

    def show_bottom_rps_view(self, state):
        self.ui.widget_layout_rps.setVisible(state == 2)

    def double_clicked_bottom_table_view(self, item):
        """
        打印被选中的单元格
        :return:
        """
        data = item.data()
        column = item.column()
        row = item.row()
        model = item.model()

        s = model.get_data(row)
        name = s['name']
        # 右侧的model
        if self.all_model == model:
            df = self.stock_df.reset_index()
            s = df[df['name'] == name].iloc[0]

        self.stock_details_window.show_with_data(s, self._block_code, self.block_warning_value)

    def clicked_bottom_table_view_item(self, item):
        """
        打印被选中的单元格
        :return:
        """
        # 未选中显示，直接返回
        if self.ui.cb_show_rps.checkState() != 2:
            return

        data = item.data()
        column = item.column()

        row = item.row()
        model = item.model()
        s = model.get_data(row)
        name = s['name']
        # 右侧的model
        if self.all_model == model:
            df = self.stock_df.reset_index()
            s = df[df['name'] == name].iloc[0]
        print(name)
        self._set_stock_rps_code(s)

    def _init_bottom_listener(self):
        """
        成分股view监听
        :return:
        """
        self.ui.dte_block_date.dateChanged.connect(self.on_block_date_change)
        self.ui.pb_load.clicked.connect(self.btn_clk)
        self.ui.pb_add.clicked.connect(self.btn_add)
        self.ui.pb_sub.clicked.connect(self.btn_sub)
        self.ui.pb_stock_refresh_rps.clicked.connect(self._refresh_stock_rps)
        self.ui.cb_block_sort.stateChanged.connect(self.fix_block_sort)
        self.ui.cb_show_rps.stateChanged.connect(self.show_bottom_rps_view)
        self.ui.table_view_bottom.doubleClicked.connect(self.double_clicked_bottom_table_view)
        self.ui.table_view_bottom.clicked.connect(self.clicked_bottom_table_view_item)
        pass

    def _init_block_data(self):
        # 查询板块所有成分股
        df = sdb.board_constituent_stock(self._block_code)
        if not df.empty:
            df = df[['stock_code', 'stock_name']]
            df.columns = ['code', 'name']
            df.set_index('code', inplace=True)
            self.stock_df = df.copy(True)

            date_time = self.config().rps_stock_start_time()
            ind_df = sdb.stock_pool_ind_on_date(self._stock_code_list(), date_time)
            # 评分
            if not ind_df.empty:
                ind_df.set_index('code', inplace=True)
                ind_df.reindex_like(df, copy=False)
                df['rps20'] = ind_df['rps20']

            # 右侧dw的数据
            self.all_model.notify_data(df)
            self.ui.tv_all_stock.setModel(self.all_model)
            # self.ui.tv_my_stock.setModel(self.my_model)

            # 如果底部面板显示，直接加载数据
            if self.ui.cb_show_bottom.checkState() == 2:
                date = self.ui.dte_block_date.date()
                self.load_block_stock(date)

                # 清除底部画布
                if not self.stock_brief_canvas is None:
                    self.stock_brief_canvas.clear_figure()
                    self.stock_brief_canvas.draw()
                    self.ui.stock_graphics_view.show()

    def load_block_stock(self, date):
        date = trade_date(date, self.config())
        date_str = date.toString('yyyy-MM-dd')
        df = sdb.stock_pool_ind_on_date(self._stock_code_list(), date_str)
        use_col = ['name', 'volume', 'amount', 'open', 'close']
        df1 = sdb.stock_daily_on_date(self._stock_code_list(), date_str, use_col)

        if df.empty and df1.empty:
            print(f'{date_str} rps 数据为空')
            return

        # 补全
        df.set_index('code', inplace=True)
        df1.set_index('code', inplace=True)
        df[['open', 'close', 'amount', 'volume']] = df1[['open', 'close', 'amount', 'volume']]
        df['name'] = self.stock_df['name']
        df.reset_index(inplace=True)
        df1.reset_index(inplace=True)

        df['amount'] = df['amount'].map(str_amount)
        df['volume'] = df['volume'].map(str_value)
        df['change'] = df[['open', 'close']].apply(str_change, axis=1)
        # 改变列位置
        df = df[['code', 'name', 'rps20', 'change', 'amount', 'rps05', 'rps10', 'rps50', 'rps120', 'rps250', 'volume']]
        df.drop_duplicates('code', inplace=True)
        self.stock_model.notify_data(df)
        self.ui.table_view_bottom.setModel(self.stock_model)

    def btn_clk(self):
        date = self.ui.dte_block_date.date()
        self.load_block_stock(date)

    def btn_add(self):
        """
        日期递增
        :param ui:
        :return:
        """
        date = self.ui.dte_block_date.date()
        date = date.addDays(1)
        self.ui.dte_block_date.setDate(trade_date(date, self.config(), False))

    def btn_sub(self):
        """
        日期递减少
        :param ui:
        :return:
        """
        date = self.ui.dte_block_date.date()
        date = date.addDays(-1)
        self.ui.dte_block_date.setDate(trade_date(date, self.config()))

    def show_bottom_brief(self, state):
        """
        显示底部
        :param state:
        :return:
        """
        self.ui.splitter_bottom.setVisible(state == 2)

    def _double_clicked_all_table(self):
        pass

    def _double_clicked_my_table(self):
        pass

    def _refresh_rps(self):
        """
        刷新绘制底部rps
        :return:
        """
        self.block_rps_list = self.combo.get_selected()
        print(self.block_rps_list)
        self._refresh_kline_canvas(False)

    def _start_date_change(self, date):
        """
        改变开始时间
        :param date:
        :return:
        """
        date = trade_date(date, self.config())
        self.ui.dte_start.setDate(date)
        self._block_start_date = date.toString('yyyy-MM-dd')
        self._refresh_kline_canvas()

    def _end_date_change(self, date):
        """
        改变结束时间
        :param date:
        :return:
        """
        date = trade_date(date, self.config())
        self.ui.dte_end.setDate(date)
        self._block_end_date = date.toString('yyyy-MM-dd')
        self._refresh_kline_canvas()

    def _refresh_kline_canvas(self, again_load_data=True):
        """
        整个k线 canvas 重新刷新
        :param again_load_data:
        :return:
        """

        if self.canvas is None:
            self.canvas = MyFigureCanvas()

        # 是否需要重新加载数据
        if again_load_data:
            self._block_data = sdb.block_stock_daily(self._block_code, self._block_start_date, self._block_end_date)

        self.canvas.set_data(self._block_data)
        self.canvas.kline(self.block_rps_list, self.block_warning_value)

        # 创建一个QGraphicsScene
        self.graphic_scene.addWidget(self.canvas)
        # 把QGraphicsScene放入QGraphicsView
        self.ui.graphicsView.setScene(self.graphic_scene)
        self.canvas.draw()
        self.ui.graphicsView.show()
        self.show()

    def show_with_data(self, s, warning_value=90):
        code = s['code']
        if self._block_code == code:
            return

        self.block_warning_value = warning_value
        self._block_code = code
        name = s['name']
        self._init_block_data()

        self._refresh_kline_canvas()
        self.setWindowTitle(name)
        self.show()

    def _stock_code_list(self):
        return list(self.stock_df.index)

    def _set_stock_rps_code(self, s):
        """
        刷新个股rps
        :param s:
        :param warning_value:
        :return:
        """
        code = s['code']
        name = s['name']
        if self._stock_code == code:
            return

        self._stock_code = code
        self._refresh_stock_rps_canvas()
        # self.setWindowTitle(f'{name}rps 摘要')

    def _refresh_stock_rps(self):
        """
        刷新绘制底部rps
        :return:
        """
        self.stock_rps_list = self.combo_bottom.get_selected()
        print(self.stock_rps_list)
        self._refresh_stock_rps_canvas(False)

    def _refresh_stock_rps_canvas(self, again_load_data=True):
        """
        个股rps画布刷新
        :param again_load_data:
        :return:
        """

        if self.stock_brief_canvas is None:
            self.stock_brief_canvas = ContrastRpsFigureCanvas()
        # 是否需要重新加载数据
        if again_load_data:
            self._stock_data = sdb.stock_pool_indicator(self._stock_code, self._stock_start_date, self._stock_end_date)

        # 板块数据对齐个股
        block_data = self._block_data.reindex_like(self._stock_data)

        self.stock_brief_canvas.set_data(self._stock_data, block_data)
        self.stock_brief_canvas.kline(self.stock_rps_list, self.stock_warning_value)

        # 创建一个QGraphicsScene
        self.stock_graphic_scene.addWidget(self.stock_brief_canvas)
        # 把QGraphicsScene放入QGraphicsView
        self.ui.stock_graphics_view.setScene(self.stock_graphic_scene)
        self.stock_brief_canvas.draw()
        self.ui.stock_graphics_view.show()
        self.show()
