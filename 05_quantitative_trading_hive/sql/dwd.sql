create database stock;
use stock;

-- TODO =========================================================  xx  =====================================================================
drop table if exists dwd_stock_technical_indicators_df;
create table if not exists dwd_stock_technical_indicators_df
(
    trade_date     date comment '交易日期',
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    rps_5d    decimal(20, 15) comment '欧奈尔rps_5d',
    rps_10d    decimal(20, 15) comment '欧奈尔rps_10d',
    rps_20d    decimal(20, 15) comment '欧奈尔rps_20d',
    rps_50d    decimal(20, 15) comment '欧奈尔rps_50d',
    rps_120d    decimal(20, 15) comment '欧奈尔rps_120d',
    rps_250d    decimal(20, 15) comment '欧奈尔rps_250d',
    rs decimal(20, 4) comment '欧奈尔RS',
    rsi_6d     decimal(20, 6) comment 'rsi_6d',
    rsi_12d decimal(20, 6) comment 'rsi_12d',
    ma_5d                     decimal(20, 4) comment '5日均线',
    ma_10d                    decimal(20, 4) comment '10日均线',
    ma_20d                    decimal(20, 4) comment '20日均线',
    ma_50d                    decimal(20, 4) comment '50日均线',
    ma_120d                    decimal(20, 4) comment '120日均线',
    ma_150d                    decimal(20, 4) comment '150日均线',
    ma_200d                    decimal(20, 4) comment '200日均线',
    ma_250d                    decimal(20, 4) comment '250日均线',
    high_price_250d     decimal(20, 4) comment '250日最高价',
    low_price_250d      decimal(20, 4) comment '250日最低价',
    update_time    timestamp comment '更新时间'
) comment 'python计算的指标'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists dwd_stock_quotes_di;
create table if not exists dwd_stock_quotes_di
(
    trade_date                date comment '交易日期',
    stock_code                string comment '股票代码',
    stock_name                string comment '股票名称',
    open_price                decimal(20, 4) comment '开盘价',
    close_price               decimal(20, 4) comment '收盘价',
    high_price                decimal(20, 4) comment '最高价',
    low_price                 decimal(20, 4) comment '最低价',
    volume                    decimal(20, 4) comment '成交量',
    volume_ratio_1d           decimal(20, 4) comment '量比_1d 与昨日对比',
    volume_ratio_5d           decimal(20, 4) comment '量比：过去5个交易日',
    turnover                  decimal(20, 4) comment '成交额',
    amplitude                 decimal(20, 4) comment '振幅',
    change_percent            decimal(20, 4) comment '涨跌幅',
    change_amount             decimal(20, 4) comment '涨跌额',
    turnover_rate             decimal(20, 4) comment '换手率',
    turnover_rate_5d          decimal(20, 4) comment '5日平均换手率',
    turnover_rate_10d         decimal(20, 4) comment '10日平均换手率',
    total_market_value        decimal(20, 4) comment '总市值',
    industry_plate            string comment '行业板块',
    concept_plates            string comment '概念板块 ,拼接',
    rps_5d    decimal(20, 15) comment '欧奈尔rps_5d',
    rps_10d    decimal(20, 15) comment '欧奈尔rps_10d',
    rps_20d    decimal(20, 15) comment '欧奈尔rps_20d',
    rps_50d    decimal(20, 15) comment '欧奈尔rps_50d',
    rps_120d    decimal(20, 15) comment '欧奈尔rps_120d',
    rps_250d    decimal(20, 15) comment '欧奈尔rps_250d',
    rs decimal(20, 4) comment '欧奈尔RS',
    rsi_6d     decimal(20, 6) comment 'rsi_6d',
    rsi_12d decimal(20, 6) comment 'rsi_12d',
    ma_5d                     decimal(20, 4) comment '5日均线',
    ma_10d                    decimal(20, 4) comment '10日均线',
    ma_20d                    decimal(20, 4) comment '20日均线',
    ma_50d                    decimal(20, 4) comment '50日均线',
    ma_120d                    decimal(20, 4) comment '120日均线',
    ma_150d                    decimal(20, 4) comment '150日均线',
    ma_200d                    decimal(20, 4) comment '200日均线',
    ma_250d                    decimal(20, 4) comment '250日均线',
    high_price_250d     decimal(20, 4) comment '250日最高价',
    low_price_250d      decimal(20, 4) comment '250日最低价',
    stock_label_names         string comment '股票标签名称 负面标签后缀用- ,拼接',
    stock_label_num           int comment '股票标签数量',
    sub_factor_names              string comment '主观因子标签名称 负面因子后缀用- ,拼接',
    sub_factor_score              int comment '主观因子分数 负面因子后缀用-',
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
    suspension_time           string comment '停牌时间',
    suspension_deadline       string comment '停牌截止时间',
    suspension_period         string comment '停牌期限',
    suspension_reason         string comment '停牌原因',
    belongs_market            string comment '所属市场',
    estimated_resumption_time string comment '预计复牌时间',
    update_time               timestamp comment '更新时间'
) comment '沪深A股行情表'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists dwd_stock_quotes_stand_di;
create table if not exists dwd_stock_quotes_stand_di
(
    trade_date                date comment '交易日期',
    stock_code                string comment '股票代码',
    stock_name                string comment '股票名称',
    open_price                decimal(20, 4) comment '开盘价',
    close_price               decimal(20, 4) comment '收盘价',
    high_price                decimal(20, 4) comment '最高价',
    low_price                 decimal(20, 4) comment '最低价',
    volume                    decimal(20, 4) comment '成交量',
    volume_ratio_1d           decimal(20, 4) comment '量比_1d 与昨日对比',
    volume_ratio_5d           decimal(20, 4) comment '量比：过去5个交易日',
    turnover                  decimal(20, 4) comment '成交额',
    amplitude                 decimal(20, 4) comment '振幅',
    change_percent            decimal(20, 4) comment '涨跌幅',
    change_amount             decimal(20, 4) comment '涨跌额',
    turnover_rate             decimal(20, 4) comment '换手率',
    turnover_rate_5d          decimal(20, 4) comment '5日平均换手率',
    turnover_rate_10d         decimal(20, 4) comment '10日平均换手率',
    total_market_value        decimal(20, 4) comment '总市值',
    industry_plate            string comment '行业板块',
    concept_plates            string comment '概念板块 ,拼接',
    rps_5d                    decimal(20, 15) comment '欧奈尔rps_5d',
    rps_10d                   decimal(20, 15) comment '欧奈尔rps_10d',
    rps_20d                   decimal(20, 15) comment '欧奈尔rps_20d',
    rps_50d                   decimal(20, 15) comment '欧奈尔rps_50d',
    rps_120d    decimal(20, 15) comment '欧奈尔rps_120d',
    rps_250d    decimal(20, 15) comment '欧奈尔rps_250d',
    rs                        decimal(20, 4) comment '欧奈尔RS',
    rsi_6d                    decimal(20, 6) comment 'rsi_6d',
    rsi_12d                   decimal(20, 6) comment 'rsi_12d',
    ma_5d                     decimal(20, 4) comment '5日均线',
    ma_10d                    decimal(20, 4) comment '10日均线',
    ma_20d                    decimal(20, 4) comment '20日均线',
    ma_50d                    decimal(20, 4) comment '50日均线',
    ma_120d                   decimal(20, 4) comment '120日均线',
    ma_150d                   decimal(20, 4) comment '150日均线',
    ma_200d                   decimal(20, 4) comment '200日均线',
    ma_250d                   decimal(20, 4) comment '250日均线',
    high_price_250d           decimal(20, 4) comment '250日最高价',
    low_price_250d            decimal(20, 4) comment '250日最低价',
    stock_label_names         string comment '股票标签名称 负面标签后缀用- ,拼接',
    stock_label_num           int comment '股票标签数量',
    sub_factor_names          string comment '主观因子标签名称 负面因子后缀用- ,拼接',
    sub_factor_score          int comment '主观因子分数 负面因子后缀用-',
    holding_yield_2d          decimal(20, 4) comment '持股2日后收益率(%)',
    holding_yield_5d          decimal(20, 4) comment '持股5日后收益率(%)',
    hot_rank                  int comment '个股热度排名',
    interprets                string comment '解读 ;拼接',
    reason_for_lhbs           string comment '上榜原因 ;拼接',
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
    suspension_time           string comment '停牌时间',
    suspension_deadline       string comment '停牌截止时间',
    suspension_period         string comment '停牌期限',
    suspension_reason         string comment '停牌原因',
    belongs_market            string comment '所属市场',
    estimated_resumption_time string comment '预计复牌时间',
    f_volume                  decimal(20, 10) comment '极值标准中性化_成交量',
    f_volume_ratio_1d         decimal(20, 10) comment '极值标准中性化_量比_1d 与昨日对比',
    f_volume_ratio_5d         decimal(20, 10) comment '极值标准中性化_量比：过去5个交易日',
    f_turnover                decimal(20, 10) comment '极值标准中性化_成交额',
    f_turnover_rate           decimal(20, 10) comment '极值标准中性化_换手率',
    f_turnover_rate_5d        decimal(20, 10) comment '极值标准中性化_5日平均换手率',
    f_turnover_rate_10d       decimal(20, 10) comment '极值标准中性化_10日平均换手率',
    f_total_market_value      decimal(20, 10) comment '极值标准中性化_总市值',
    f_pe                      decimal(20, 10) comment '极值标准中性化_市盈率',
    f_pe_ttm                  decimal(20, 10) comment '极值标准中性化_市盈率TTM',
    update_time               timestamp comment '更新时间'
) comment '沪深A股行情表-部分字段去极值标准化行业市值中性化'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists dwd_stock_zt_di;
create table if not exists dwd_stock_zt_di
(
    trade_date                date comment '交易日期',
    stock_code                string comment '股票代码',
    stock_name                string comment '股票名称',
    open_price                decimal(20, 4) comment '开盘价',
    close_price               decimal(20, 4) comment '收盘价',
    high_price                decimal(20, 4) comment '最高价',
    low_price                 decimal(20, 4) comment '最低价',
    volume                    decimal(20, 4) comment '成交量',
    volume_ratio_1d           decimal(20, 4) comment '量比_1d 与昨日对比',
    volume_ratio_5d           decimal(20, 4) comment '量比：过去5个交易日',
    change_percent            decimal(20, 4) comment '涨跌幅',
    turnover_rate             decimal(20, 4) comment '换手率',
    turnover_rate_5d          decimal(20, 4) comment '5日平均换手率',
    turnover_rate_10d         decimal(20, 4) comment '10日平均换手率',
    total_market_value        decimal(20, 4) comment '总市值',
    industry_plate            string comment '行业板块',
    concept_plates            string comment '概念板块 ,拼接',

    rps_5d    decimal(20, 15) comment '欧奈尔rps_5d',
    rps_10d    decimal(20, 15) comment '欧奈尔rps_10d',
    rps_20d    decimal(20, 15) comment '欧奈尔rps_20d',
    rps_50d    decimal(20, 15) comment '欧奈尔rps_50d',
    rps_120d    decimal(20, 15) comment '欧奈尔rps_120d',
    rps_250d    decimal(20, 15) comment '欧奈尔rps_250d',
    rs decimal(20, 4) comment '欧奈尔RS',
    rsi_6d     decimal(20, 6) comment 'rsi_6d',
    rsi_12d decimal(20, 6) comment 'rsi_12d',
    ma_5d                     decimal(20, 4) comment '5日均线',
    ma_10d                    decimal(20, 4) comment '10日均线',
    ma_20d                    decimal(20, 4) comment '20日均线',
    ma_50d                    decimal(20, 4) comment '50日均线',
    high_price_250d     decimal(20, 4) comment '250日最高价',
    low_price_250d      decimal(20, 4) comment '250日最低价',

    stock_label_names         string comment '股票标签名称 负面标签后缀用- ,拼接',
    stock_label_num           int comment '股票标签数量',
    sub_factor_names              string comment '主观因子标签名称 负面因子后缀用- ,拼接',
    sub_factor_score              int comment '主观因子分数 负面因子后缀用-',
    holding_yield_2d          decimal(20, 4) comment '持股2日后收益率(%)',
    holding_yield_5d          decimal(20, 4) comment '持股5日后收益率(%)',
    f_volume                  decimal(20, 10) comment '极值标准中性化_成交量',
    f_volume_ratio_1d         decimal(20, 10) comment '极值标准中性化_量比_1d 与昨日对比',
    f_volume_ratio_5d         decimal(20, 10) comment '极值标准中性化_量比：过去5个交易日',
    f_turnover                decimal(20, 10) comment '极值标准中性化_成交额',
    f_turnover_rate           decimal(20, 10) comment '极值标准中性化_换手率',
    f_turnover_rate_5d        decimal(20, 10) comment '极值标准中性化_5日平均换手率',
    f_turnover_rate_10d       decimal(20, 10) comment '极值标准中性化_10日平均换手率',
    f_total_market_value      decimal(20, 10) comment '极值标准中性化_总市值',
    f_pe                      decimal(20, 10) comment '极值标准中性化_市盈率',
    f_pe_ttm                  decimal(20, 10) comment '极值标准中性化_市盈率TTM',

    Sealing_amount  decimal(20, 4) comment '封板资金',
    first_Sealing_time  string comment '首次封板时间',
    last_Sealing_time  string comment '最后封板时间',
    bomb_Sealing_nums  int comment '炸板数',
    zt_tj  string comment '涨停统计',
    lx_Sealing_nums  int comment '连板数',

    is_new_g  int comment '是否新高',
    selection_reason  int comment '强势股入选理由 1:60日新高, 2:近期多次涨停, 3:60日新高且近期多次涨停',

    hot_rank  int comment '个股热度排名',
    interprets                 string comment '解读 ;拼接',
    reason_for_lhbs            string comment '上榜原因 ;拼接',
    lhb_num_60d               int comment '最近60天_龙虎榜上榜次数',

    update_time               timestamp comment '更新时间'
) comment '打板涨停'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');


drop table if exists dwd_stock_strong_di;
create table if not exists dwd_stock_strong_di
(
    trade_date                date comment '交易日期',
    stock_code                string comment '股票代码',
    stock_name                string comment '股票名称',
    open_price                decimal(20, 4) comment '开盘价',
    close_price               decimal(20, 4) comment '收盘价',
    high_price                decimal(20, 4) comment '最高价',
    low_price                 decimal(20, 4) comment '最低价',
    volume                    decimal(20, 4) comment '成交量',
    volume_ratio_1d           decimal(20, 4) comment '量比_1d 与昨日对比',
    volume_ratio_5d           decimal(20, 4) comment '量比：过去5个交易日',
    change_percent            decimal(20, 4) comment '涨跌幅',
    turnover_rate             decimal(20, 4) comment '换手率',
    turnover_rate_5d          decimal(20, 4) comment '5日平均换手率',
    turnover_rate_10d         decimal(20, 4) comment '10日平均换手率',
    total_market_value        decimal(20, 4) comment '总市值',
    industry_plate            string comment '行业板块',
    concept_plates            string comment '概念板块 ,拼接',

    rps_5d    decimal(20, 15) comment '欧奈尔rps_5d',
    rps_10d    decimal(20, 15) comment '欧奈尔rps_10d',
    rps_20d    decimal(20, 15) comment '欧奈尔rps_20d',
    rps_50d    decimal(20, 15) comment '欧奈尔rps_50d',
    rps_120d    decimal(20, 15) comment '欧奈尔rps_120d',
    rps_250d    decimal(20, 15) comment '欧奈尔rps_250d',
    rs decimal(20, 4) comment '欧奈尔RS',
    rsi_6d     decimal(20, 6) comment 'rsi_6d',
    rsi_12d decimal(20, 6) comment 'rsi_12d',
    ma_5d                     decimal(20, 4) comment '5日均线',
    ma_10d                    decimal(20, 4) comment '10日均线',
    ma_20d                    decimal(20, 4) comment '20日均线',
    ma_50d                    decimal(20, 4) comment '50日均线',
    high_price_250d     decimal(20, 4) comment '250日最高价',
    low_price_250d      decimal(20, 4) comment '250日最低价',


    stock_label_names         string comment '股票标签名称 负面标签后缀用- ,拼接',
    stock_label_num           int comment '股票标签数量',
    sub_factor_names              string comment '主观因子标签名称 负面因子后缀用- ,拼接',
    sub_factor_score              int comment '主观因子分数 负面因子后缀用-',
    holding_yield_2d          decimal(20, 4) comment '持股2日后收益率(%)',
    holding_yield_5d          decimal(20, 4) comment '持股5日后收益率(%)',
    f_volume                  decimal(20, 10) comment '极值标准中性化_成交量',
    f_volume_ratio_1d         decimal(20, 10) comment '极值标准中性化_量比_1d 与昨日对比',
    f_volume_ratio_5d         decimal(20, 10) comment '极值标准中性化_量比：过去5个交易日',
    f_turnover                decimal(20, 10) comment '极值标准中性化_成交额',
    f_turnover_rate           decimal(20, 10) comment '极值标准中性化_换手率',
    f_turnover_rate_5d        decimal(20, 10) comment '极值标准中性化_5日平均换手率',
    f_turnover_rate_10d       decimal(20, 10) comment '极值标准中性化_10日平均换手率',
    f_total_market_value      decimal(20, 10) comment '极值标准中性化_总市值',
    f_pe                      decimal(20, 10) comment '极值标准中性化_市盈率',
    f_pe_ttm                  decimal(20, 10) comment '极值标准中性化_市盈率TTM',

    is_new_g  int comment '是否新高',
    selection_reason  int comment '强势股入选理由 1:60日新高, 2:近期多次涨停, 3:60日新高且近期多次涨停',

    Sealing_amount  decimal(20, 4) comment '封板资金',
    first_Sealing_time  string comment '首次封板时间',
    last_Sealing_time  string comment '最后封板时间',
    bomb_Sealing_nums  int comment '炸板数',
    zt_tj  string comment '涨停统计',
    lx_Sealing_nums  int comment '连板数',

    hot_rank  int comment '个股热度排名',
    interprets                 string comment '解读 ;拼接',
    reason_for_lhbs            string comment '上榜原因 ;拼接',
    lhb_num_60d               int comment '最近60天_龙虎榜上榜次数',

    update_time               timestamp comment '更新时间'
) comment '强势股'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

















