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
    industry_plate           string comment '行业板块',
    concept_plates           string comment '概念板块 ,拼接',
    pe                       decimal(20, 4) comment '市盈率',
    pe_ttm                   decimal(20, 4) comment '市盈率TTM',
    pb                       decimal(20, 4) comment '市净率',
    ps                       decimal(20, 4) comment '市销率',
    ps_ttm                   decimal(20, 4) comment '市销率TTM',
    dv_ratio                 decimal(20, 4) comment '股息率',
    dv_ttm                   decimal(20, 4) comment '股息率TTM',
    net_profit               decimal(20, 4) comment '净利润',
    net_profit_yr            decimal(20, 4) comment '净利润同比',
    total_business_income    decimal(20, 4) comment '营业总收入',
    total_business_income_yr decimal(20, 4) comment '营业总收入同比',
    business_fee             decimal(20, 4) comment '营业总支出-营业支出',
    sales_fee                decimal(20, 4) comment '营业总支出-销售费用',
    management_fee           decimal(20, 4) comment '营业总支出-管理费用',
    finance_fee              decimal(20, 4) comment '营业总支出-财务费用',
    total_business_fee       decimal(20, 4) comment '营业总支出-营业总支出',
    business_profit          decimal(20, 4) comment '营业利润',
    total_profit             decimal(20, 4) comment '利润总额',
    ps_business_cash_flow    decimal(20, 4) comment '每股经营性现金流(元)',
    return_on_equity         decimal(20, 4) comment '净资产收益率(%)',
    npadnrgal                decimal(20, 4) comment '扣除非经常性损益后的净利润(元)',
    net_profit_growth_rate   decimal(20, 4) comment '净利润增长率(%)',
    interprets                 string comment '解读 ;拼接',
    reason_for_lhbs            string comment '上榜原因 ;拼接',
    lhb_num_5d               int comment '最近5天_龙虎榜上榜次数',
    lhb_num_10d              int comment '最近10天_龙虎榜上榜次数',
    lhb_num_30d              int comment '最近30天_龙虎榜上榜次数',
    lhb_num_60d              int comment '最近60天_龙虎榜上榜次数',
    ma_5d                    decimal(20, 4) comment '5日均线',
    ma_10d                   decimal(20, 4) comment '10日均线',
    ma_20d                   decimal(20, 4) comment '20日均线',
    ma_30d                   decimal(20, 4) comment '30日均线',
    ma_60d                   decimal(20, 4) comment '60日均线',
    stock_label_names        string comment '股票标签名称 负面标签后缀用- ,拼接',
    stock_label_num          int comment '股票标签数量',
    sub_factor_names             string comment '主观因子标签名称 负面因子后缀用- ,拼接',
    sub_factor_score             int comment '主观因子分数 负面因子后缀用-',
    stock_strategy_name      string comment '股票策略名称 股票标签名称 +拼接',
    stock_strategy_ranking   string comment '策略内排行 dense_rank',
    holding_yield_2d         decimal(20, 4) comment '持股2日后收益率(%)',
    holding_yield_5d         decimal(20, 4) comment '持股5日后收益率(%)',
    update_time              timestamp comment '更新时间'
) comment '股票推荐 （top10）'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');















