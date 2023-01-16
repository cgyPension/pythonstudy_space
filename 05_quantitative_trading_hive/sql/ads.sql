create database stock;
use stock;

-- TODO =========================================================  xx  =====================================================================
drop table if exists ads_stock_suggest_di;
create table if not exists ads_stock_suggest_di
(
    trade_date               date comment '交易日期',
    stock_code               string comment '股票代码',
    stock_name               string comment '股票名称',
    open_price               decimal(20, 4) comment '开盘价',
    close_price              decimal(20, 4) comment '收盘价',
    high_price               decimal(20, 4) comment '最高价',
    low_price                decimal(20, 4) comment '最低价',
    volume                   decimal(20, 4) comment '成交量',
    volume_ratio_1d          decimal(20, 4) comment '量比_1d 与昨日对比',
    volume_ratio_5d          decimal(20, 4) comment '量比：过去5个交易日',
    turnover                 decimal(20, 4) comment '成交额',
    amplitude                decimal(20, 4) comment '振幅',
    change_percent           decimal(20, 4) comment '涨跌幅',
    change_amount            decimal(20, 4) comment '涨跌额',
    turnover_rate            decimal(20, 4) comment '换手率',
    turnover_rate_5d         decimal(20, 4) comment '5日平均换手率',
    turnover_rate_10d        decimal(20, 4) comment '10日平均换手率',
    total_market_value       decimal(20, 4) comment '总市值',
    z_total_market_value     decimal(20, 4) comment '行业标准差总市值',
    industry_plate           string comment '行业板块',
    concept_plates           string comment '概念板块 ,拼接',

    rps_5d    decimal(20, 15) comment '欧奈尔rps_5d',
    rps_10d    decimal(20, 15) comment '欧奈尔rps_10d',
    rps_20d    decimal(20, 15) comment '欧奈尔rps_20d',
    rps_50d    decimal(20, 15) comment '欧奈尔rps_50d',
    rs decimal(20, 4) comment '欧奈尔RS',
    rsi_6d     decimal(20, 6) comment 'rsi_6d',
    rsi_12d decimal(20, 6) comment 'rsi_12d',
    ma_5d                     decimal(20, 4) comment '5日均线',
    ma_10d                    decimal(20, 4) comment '10日均线',
    ma_20d                    decimal(20, 4) comment '20日均线',
    ma_50d                    decimal(20, 4) comment '50日均线',
    high_price_250d     decimal(20, 4) comment '250日最高价',
    low_price_250d      decimal(20, 4) comment '250日最低价',

    stock_label_names        string comment '股票标签名称 负面标签后缀用- ,拼接',
    stock_label_num          int comment '股票标签数量',
    sub_factor_names             string comment '主观因子标签名称 负面因子后缀用- ,拼接',
    sub_factor_score             int comment '主观因子分数 负面因子后缀用-',
    stock_strategy_name      string comment '股票策略名称 股票标签名称 +拼接',
    stock_strategy_ranking   int comment '策略内排行 dense_rank',

    holding_yield_2d          decimal(20, 4) comment '持股2日后收益率(%)',
    holding_yield_5d          decimal(20, 4) comment '持股5日后收益率(%)',
    hot_rank  int comment '个股热度排名',
    interprets                 string comment '解读 ;拼接',
    reason_for_lhbs            string comment '上榜原因 ;拼接',
    lhb_num_5d                int comment '最近5天_龙虎榜上榜次数',
    lhb_num_10d               int comment '最近10天_龙虎榜上榜次数',
    lhb_num_30d               int comment '最近30天_龙虎榜上榜次数',
    lhb_num_60d               int comment '最近60天_龙虎榜上榜次数',

    pe                        decimal(20, 4) comment '市盈率',
    pe_ttm                    decimal(20, 4) comment '市盈率TTM',
    pb                        decimal(20, 4) comment '市净率',
    ps                        decimal(20, 4) comment '市销率',
    ps_ttm                    decimal(20, 4) comment '市销率TTM',
    dv_ratio                  decimal(20, 4) comment '股息率',
    dv_ttm                    decimal(20, 4) comment '股息率TTM',
    net_profit                decimal(20, 4) comment '净利润',
    net_profit_yr             decimal(20, 4) comment '净利润同比',
    total_business_income     decimal(20, 4) comment '营业总收入',
    total_business_income_yr  decimal(20, 4) comment '营业总收入同比',
    business_fee              decimal(20, 4) comment '营业总支出-营业支出',
    sales_fee                 decimal(20, 4) comment '营业总支出-销售费用',
    management_fee            decimal(20, 4) comment '营业总支出-管理费用',
    finance_fee               decimal(20, 4) comment '营业总支出-财务费用',
    total_business_fee        decimal(20, 4) comment '营业总支出-营业总支出',
    business_profit           decimal(20, 4) comment '营业利润',
    total_profit              decimal(20, 4) comment '利润总额',
    ps_business_cash_flow     decimal(20, 4) comment '每股经营性现金流(元)',
    return_on_equity          decimal(20, 4) comment '净资产收益率(%)',
    npadnrgal                 decimal(20, 4) comment '扣除非经常性损益后的净利润(元)',
    net_profit_growth_rate    decimal(20, 4) comment '净利润增长率(%)',
    update_time              timestamp comment '更新时间'
) comment '股票推荐 （top10）'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ads_strategy_yield_di;
create table if not exists ads_strategy_yield_di
(
    trade_date               date comment '交易日期',
    strategy_id        string comment '股票策略id',
    strategy_name      string comment '股票策略名称 股票标签名称 +拼接',
    hold_day           int comment '持股周期',
    hold_n             int comment '买入排名',
    yield_td           decimal(20, 2) comment '累计收益率',
    annual_yield       decimal(20, 2) comment '年化收益率',
    wp                 decimal(20, 2) comment '胜率 累计',
    -- 这里算不出来
    --max_drawdown       decimal(20, 2) comment '最大回撤',
    --没有必要算
    --sharp_ratio        decimal(20, 2) comment '夏普比率',
    yield_1d           decimal(20, 2) comment '今日收益率',
    yield_7d           decimal(20, 2) comment '近7天收益率',
    yield_22d          decimal(20, 2) comment '近1月收益率',
    yield_66d          decimal(20, 2) comment '近3月收益率',
    --没有必要算
    --annual_max_retrace decimal(20, 2) comment '年化收益率/最大回撤',
    update_time        timestamp comment '更新时间'
) comment '策略收益风控表 这里的收益率会偏高直接计算没有手续费'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress' = 'snappy');













