import pandas as pd
import datetime
from collections import OrderedDict


def initialize(context):
    """初始化，启动程序后只调用一次

    :param context: Context对象，存放有当前的账户及持仓信息
    :return: None
    """

    # 定义一个周期处理函数，每3秒执行一次
    run_interval(context, interval_handle, seconds = 3)
    # 资金少于limit_value则不买入
    g.limit_value = 5000
    # 持仓股票只数限制
    g.limit_stock_num = 3
    # 持股天数限制
    g.limit_hold_days = 6
    g.security = '000300.SS'
    set_universe(g.security)


def before_trading_start(context, data):
    """在每天开始交易前被调用，此处添加每天都要初始化的信息

    :param context: Context对象，存放有当前的账户及持仓信息
    :param data: 保留字段暂无数据
    :return: None
    """

    # 当日提交买单的股票代码集合
    g.buy_submitted_set = set()

    # 读取数据库文件
    file_path = get_research_path() + 'upload_file/trade_data.csv'
    g.db_df = pd.read_csv(file_path, encoding='utf-8', converters={'code': str})

    # code转换
    g.db_df['code_ptrade'] = g.db_df['code'].map(lambda x: x + '.SS' if '6' == x[0] else x + '.SZ')

    # 获取待交易数据
    g.to_trade_df = g.db_df[1 == g.db_df['to_trade']]

    # 获取待买入数据
    g.to_buy_df = g.to_trade_df[g.to_trade_df['date_buy'].isna()]
    g.to_buy_stock_list = g.to_buy_df['code_ptrade'].tolist()

    # 生成买点字典
    g.buy_point_dict = g.to_buy_df[['code_ptrade', 'buy_point']].set_index('code_ptrade').to_dict(into=OrderedDict)['buy_point']
    log.info('买点字典：{}'.format(g.buy_point_dict))

    # 获取待止盈、止损卖出数据
    g.to_sell_df = g.to_trade_df[(~g.to_trade_df['date_buy'].isna()) & (g.to_trade_df['hold_days'] <= g.limit_hold_days)]
    g.to_sell_stock_list = g.to_sell_df['code_ptrade'].tolist()

    # 获取待立刻卖出股票
    g.to_sell_immi_df = g.to_trade_df[(~g.to_trade_df['date_buy'].isna()) & (g.to_trade_df['hold_days'] > g.limit_hold_days)]
    g.to_sell_immi_list = g.to_sell_immi_df['code_ptrade'].tolist()
    g.sell_immi_done = False

    # 生成止盈、止损字典
    g.take_profit_dict = g.to_sell_df[['code_ptrade', 'price_take_profit']].set_index('code_ptrade').to_dict()['price_take_profit']
    log.info('止盈字典：{}'.format(g.take_profit_dict))
    g.stop_loss_dict = g.to_sell_df[['code_ptrade', 'price_stop_loss']].set_index('code_ptrade').to_dict()['price_stop_loss']
    log.info('止损字典：{}'.format(g.stop_loss_dict))

    # 设置待交易股票
    g.security =  g.to_trade_df['code_ptrade'].tolist()
    set_universe(g.security)

    # 重置ipo标识
    g.ipo_done = False
    log.info('当前可用资金：{}'.format(context.portfolio.cash))

    # 盘前信息
    log.info('盘前持股{}只：{}'.format(get_position_count(context), get_position_list(context)))
    log.info('单只股票买入金额：{}'.format(value_per_stock(context)))


def enough_cash(context, limit_value):
    """判断资金余额是否充足

    :param context: Context对象，存放有当前的账户及持仓信息
    :param limit_value: 资金限制，当前账户余额需大于等于该值，才判断为余额充足
    :return: 资金充足则返回True，否则返回False
    """

    if context.portfolio.cash < limit_value:
        log.info('余额不足')
        return False
    else:
        return True


def bought_stock_set(context):
    """已买股票的集合

    对已提交买入的股票代码的集合、持仓股票代码的集合求并集

    :param context: Context对象，存放有当前的账户及持仓信息
    :return: 已买股票的集合
    """
    return g.buy_submitted_set | set(get_position_list(context))


def available_position_count(context):
    """计算当前可买的股票只数

    对已提交买入的股票代码的集合、持仓股票代码的集合求并集
    再用持股只数限制减去上面并集中元素的个数，即为当前可买的股票只数

    :param context: Context对象，存放有当前的账户及持仓信息
    :return: 当前可买的股票只数
    """

    return g.limit_stock_num - len(bought_stock_set(context))


def value_per_stock(context):
    """计算单只股票买入金额

    资金余额除以当前可买的股票只数
    当可买的股票只数为0时返回0.0

    :param context: Context对象，存放有当前的账户及持仓信息
    :return: 单只股票买入金额，当可买的股票只数为0时返回0.0
    """

    # 计算当前可买的股票只数
    available_count = available_position_count(context)

    # 当可买的股票只数为0时返回0.0
    if 0 == available_count:
        return 0.0

    return context.portfolio.cash / available_count


def get_position_count(context):
    """获取当前持股只数

    调用get_position_list获取当前持有股票的代码列表
    使用len获取持股只数

    :param context: 存放有当前的账户及持仓信息
    :return: 当前持有股票的只数
    """

    return len(get_position_list(context))


def get_position_list(context):
    """获取当前持股列表

    context.portfolio.positions包含持股信息，但需要通过amount!=0来获取真实持股
    因为当股票卖出成功时，当日清仓的股票信息仍会保存在context.portfolio.positions中，只是amount等于0

    :param context: 存放有当前的账户及持仓信息
    :return: 当前持有股票的代码列表
    """

    return [x for x in context.portfolio.positions if context.portfolio.positions[x].amount != 0]


def handle_sell(context):
    """处理卖出逻辑

    :param context: Context对象，存放有当前的账户及持仓信息
    :return: None
    """

    # 处理到达持仓天数限制的股票，用当前价下跌1.8%提交卖单，确保卖出
    if not g.sell_immi_done:
        for stock in g.to_sell_immi_list:
            snapshot = get_snapshot(stock)
            order_target(stock, 0, limit_price=round(snapshot[stock]['last_px'] * 0.982, 2))
            g.sell_immi_done = True
            log.info('{}持仓天数限制卖单提交'.format(stock))

    # 遍历待卖出股票
    for stock in g.to_sell_stock_list.copy():

        # 获取实时行情快照
        snapshot = get_snapshot(stock)

        # 判断是否停盘，停盘则跳过
        trade_status = snapshot[stock]['trade_status']
        if trade_status == 'STOPT':
            log.info(stock, '该股为停盘状态，不进行交易判断')
            continue

        # 获取股票最高价、最低价、当前价
        high_price = snapshot[stock]['high_px']
        low_price = snapshot[stock]['low_px']
        current_price = snapshot[stock]['last_px']

        # 限价，创业板和科创板有价格笼子限制，卖出申报价格不得低于卖出基准价格的98%
        limit_price = round(current_price * 0.982, 2)

        # 如果达到止盈或者止损条件，则挂限价卖出
        if high_price >= g.take_profit_dict[stock] or low_price <= g.stop_loss_dict[stock]:
            log.info('{}到达卖点'.format(stock))

            # 下指定市值卖单
            order_target(stock, 0, limit_price=limit_price)

            # 在待卖出股票列表中删除该股票
            g.to_sell_stock_list.remove(stock)
            log.info('{}卖单提交'.format(stock))


def handle_buy(context):
    """处理买入逻辑

    :param context: Context对象，存放有当前的账户及持仓信息
    :return: None
    """

    # 判断剩余资金是否大于最小买入金额限制，单只股票买入金额太小，没有意义
    if context.portfolio.cash < g.limit_value:
        return

    # 判断如果已达最大持股只数，则不买入
    if available_position_count(context) <= 0:
        return


    # 遍历每只候选买入股票
    for stock in g.to_buy_stock_list.copy():

        # 判断如果已达最大持股只数，则不买入
        if available_position_count(context) <= 0:
            return

        # 不重复买入股票
        if stock in bought_stock_set(context):
            continue

        # 获取实时行情快照
        snapshot = get_snapshot(stock)

        # 判断是否停盘，停盘则跳过
        trade_status = snapshot[stock]['trade_status']
        if trade_status == 'STOPT':
            log.info((stock, '该股为停盘状态，不进行交易判断'))
            continue

        # 获取股票最低价和当前价
        low_price = snapshot[stock]['low_px']
        current_price = snapshot[stock]['last_px']

        # 获取计算单只股票买入金额
        target_value = value_per_stock(context)

        # 如果余额不足买1手，则跳过该股票
        if target_value < current_price * 100 * 1.0003:
            continue

        # 限价，创业板和科创板有价格笼子限制，买入申报价格不得高于买入基准价格的102%
        limit_price = round(current_price * 1.018, 2)

        # 最低价低于买点，且limit_price不超过买点的3.82%，再下买单。避免有股票卖出后，余额充足后买入新股票的价格过高
        if (low_price <= g.buy_point_dict[stock]) and (limit_price / g.buy_point_dict[stock] <= 1.0382):
            log.info('{}到达买点'.format(stock))

            # 将股票代码添加到已提交买单字典
            g.buy_submitted_set.add(stock)

            # 下指定市值买单，用限价提交
            log.info('targe_value={}, limit_price={}'.format(target_value, limit_price))
            order_target_value(stock, target_value, limit_price=limit_price)

            # 在待买入股票列表中删除该股票
            g.to_buy_stock_list.remove(stock)
            log.info('{}买单提交'.format(stock))


def handle_ipo():
    """处理打新

    11:13申购

    :return: None
    """

    # 获取当前时间
    if not g.ipo_done and datetime.datetime.now().time() >= datetime.time(11, 13, 0):

        # 申购上证普通新股
        ipo_stocks_order(market_type=0)

        # 申购深证普通新股
        ipo_stocks_order(market_type=2)

        # 申购上证普通新股
        ipo_stocks_order(market_type=3)

        # 标记当日已申购
        g.ipo_done = True


def interval_handle(context):
    """周期处理函数

    :param context: 存放有当前的账户及持仓信息
    :return: None
    """

    # 卖出
    handle_sell(context)

    # 买入
    handle_buy(context)

    # 打新
    handle_ipo()


def on_order_response(context, order_list):
    """在委托回报返回时响应

    :param context: 存放有当前的账户及持仓信息
    :param order_list: 一个列表，当前委托单发生变化时，发生变化的委托单列表。委托单以字典形式展现，内容包括：'entrust_no'(委托单号),
                       'order_time'(委托时间), 'stock_code'(股票代码), 'amount'(委托数量), 'price'(委托价格), 'business_amount'(成交数量),
                       'status'(委托状态), 'order_id'(委托订单号), 'entrust_type'(委托类别), 'entrust_prop'(委托属性)
    :return: None
    """

    # 打印委托数据
    for order in order_list:
        bs = '买入' if order['amount'] > 0 else '卖出'
        info = '订单提交，股票代码：{}，数量：{}{:.0f}'.format(order['stock_code'], bs, abs(order['amount']))
        log.info(info)


def on_trade_response(context, trade_list):
    """在成交回报返回时响应

    :param context: 存放有当前的账户及持仓信息
    :param trade_list： 一个列表，当前成交单发生变化时，发生变化的成交单列表。成交单以字典形式展现，内容包括：'entrust_no'(委托单号),
                        'business_time'(成交时间), 'stock_code'(股票代码), 'entrust_bs'(成交方向), 'business_amount'(成交数量),
                        'business_price'(成交价格), 'business_balance'(成交额), 'business_id'(成交编号), 'status'(委托状态)
    :return: None
    """

    # 打印成交数据
    for trade in trade_list:
        bs = '买入' if trade['business_amount'] > 0 else '卖出'
        info = '订单成交，股票代码：{}，数量：{}{:.0f}'.format(trade['stock_code'], bs, abs(trade['business_amount']))
        log.info(info)


def after_trading_end(context, data):
    """在每天交易结束之后调用，用来处理每天收盘后的操作

    :param context: 存放有当前的账户及持仓信息
    :param data： 保留字段暂无数据
    :return: None
    """

    # 打印盘后持股数据
    log.info('盘后持股{}只：{}'.format(get_position_count(context), get_position_list(context)))
