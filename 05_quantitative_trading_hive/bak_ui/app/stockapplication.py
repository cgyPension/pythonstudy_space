import asyncio

from config.stockconfig import StockConfig
from core.stockdata import StockData
from core.stockindicator import StockIndicator


class StockApplication:
    """
    application 程序控制
    """

    def __init__(self):
        # 配置信息
        self.config = StockConfig()
        # 数据管理类注入配置信息
        self.data = StockData(self.config)
        # 指标计算类注入配置信息
        self.indicator = StockIndicator(self.config)

    def start(self, skip_update=False):
        if skip_update:
            return

        # 更新板块日行情数据
        asyncio.run(self.data.update_board_daily_multi_io())
        # 更新板块成分股
        asyncio.run(self.data.update_board_constituent_stock(True))
        # 更新板块指标
        self.indicator.calculate_board_ind()

        # 更新网上所有代码列表
        self.data.update_all_code()
        # 更新日行情数据
        asyncio.run(self.data.update_daily_multi_io())
        # 更新我的股票池
        self.data.update_stock_pool()
        # 更新相关的指标
        self.indicator.update_common_ind()
        # 计算rps
        stock_poll = self.data.stock_poll()
        self.indicator.calculate_rps(stock_poll)
        pass

    pass


if __name__ == '__main__':
    # 项目启动类
    application = StockApplication()
    application.start()
    pass
