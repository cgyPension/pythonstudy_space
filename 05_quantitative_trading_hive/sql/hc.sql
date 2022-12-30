create database hc;
use hc;

-- TODO =========================================================  xx  =====================================================================
drop table if exists dim_hc_strategy_main;
create table if not exists dim_hc_strategy_main
(
    strategy_id        string comment '股票策略id',
    strategy_name      string comment '股票策略名称 股票标签名称 +拼接',
    hold_day           int comment '持股周期',
    hold_n             int comment '买入排名',
    start_date         date comment '回测数据开始日期',
    end_date           date comment '回测数据结束日期',
    start_cash         decimal(20, 2) comment '期初资金',
    end_cash           decimal(20, 2) comment '期末资金',
    yield_td           decimal(20, 2) comment '累计收益率',
    annual_yield       decimal(20, 2) comment '年化收益率',
    wp                 decimal(20, 2) comment '胜率',
    max_drawdown       decimal(20, 2) comment '最大回撤',
    sharp_ratio        decimal(20, 2) comment '夏普比率',
    yield_1d           decimal(20, 2) comment '今日收益率',
    yield_7d           decimal(20, 2) comment '近7天收益率',
    yield_22d          decimal(20, 2) comment '近1月收益率',
    yield_66d          decimal(20, 2) comment '近3月收益率',
    annual_max_retrace decimal(20, 2) comment '年化收益率/最大回撤',
    update_time        timestamp comment '更新时间'
) comment '策略回测主表'
    partitioned by (hc_strategy string comment '分区_股票策略i')
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress' = 'snappy');

drop table if exists dim_hc_strategy_yield;
create table if not exists dim_hc_strategy_yield
(
    trade_date  date comment '交易日期',
    label       string comment '标签',
    yield_td    decimal(20, 2) comment '累计收益率',
    update_time timestamp comment '更新时间'
) comment '收益统计'
    partitioned by (hc_strategy string comment '分区_股票策略i')
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress' = 'snappy');

drop table if exists dim_hc_strategy_cc;
create table if not exists dim_hc_strategy_cc
(
    trade_date  date comment '交易日期',
    cc          decimal(20, 2) comment '持仓比',
    update_time timestamp comment '更新时间'
) comment '持仓比'
    partitioned by (hc_strategy string comment '分区_股票策略i')
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress' = 'snappy');

drop table if exists dim_hc_strategy_trade;
create table if not exists dim_hc_strategy_trade
(
    order_id       int comment '订单',
    stock_code  string comment '股票代码',
    buy_date    date comment '买入日期',
    buy_price   decimal(20, 2) comment '买价',
    sell_date   date comment '卖出日期',
    sell_price  decimal(20, 2) comment '卖价',
    yield_ratio decimal(20, 2) comment '收益率',
    yield       decimal(20, 2) comment '利润',
    yield_cash  decimal(20, 2) comment '利润总资产比',
    size        int comment '股数',
    cost        decimal(20, 2) comment '股本',
    cc          decimal(20, 2) comment '持仓比',
    yield_td    decimal(20, 2) comment '累计收益率',
    max_yield   decimal(20, 2) comment '最大利润',
    max_loss    decimal(20, 2) comment '最大亏损',
    update_time timestamp comment '更新时间'
) comment '交易详情'
    partitioned by (hc_strategy string comment '分区_股票策略i')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');





