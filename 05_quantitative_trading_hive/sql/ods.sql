create database stock;
use stock;

-- TODO =========================================================  xx  =====================================================================
drop table if exists ods_dc_stock_concept_plate_name_di;
create table if not exists ods_dc_stock_concept_plate_name_di
(
    trade_date                   date comment '交易日期',
    concept_plate_code           string comment '概念板块代码',
    concept_plate                string comment '概念板块名称',
    new_price                    decimal(20, 4) comment '最新价',
    change_amount                decimal(20, 4) comment '涨跌额',
    change_percent               decimal(20, 4) comment '涨跌幅',
    total_market_value           decimal(20, 4) comment '总市值',
    turnover_rate                decimal(20, 4) comment '换手率',
    rise_num                     int comment '上涨家数',
    fall_num                     int comment '下跌家数',
    leading_stock_name           string comment '领涨股票名称',
    leading_stock_change_percent decimal(20, 4) comment '领涨股票-涨跌幅',
    update_time                  timestamp comment '更新时间'
) comment '东方财富-沪深板块-概念板块'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_dc_stock_concept_plate_di;
create table if not exists ods_dc_stock_concept_plate_di
(
    trade_date    date comment '交易日期',
    stock_code    string comment '股票代码',
    stock_name    string comment '股票名称',
    concept_plate string comment '概念板块',
    update_time   timestamp comment '更新时间'
) comment ' 东方财富-沪深板块-概念板块-板块成份股'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_dc_stock_industry_plate_di;
create table if not exists ods_dc_stock_industry_plate_di
(
    trade_date    date comment '交易日期',
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    industry_plate string comment '行业板块',
    update_time    timestamp comment '更新时间'
) comment '东方财富-沪深板块-行业板块-板块成份股'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_dc_stock_industry_plate_name_di;
create table if not exists ods_dc_stock_industry_plate_name_di
(
    trade_date                   date comment '交易日期',
    industry_plate_code          string comment '行业板块代码',
    industry_plate               string comment '行业板块名称',
    new_price                    decimal(20, 4) comment '最新价',
    change_amount                decimal(20, 4) comment '涨跌额',
    change_percent               decimal(20, 4) comment '涨跌幅',
    total_market_value           decimal(20, 4) comment '总市值',
    turnover_rate                decimal(20, 4) comment '换手率',
    rise_num                     int comment '上涨家数',
    fall_num                     int comment '下跌家数',
    leading_stock_name           string comment '领涨股票名称',
    leading_stock_change_percent decimal(20, 4) comment '领涨股票-涨跌幅',
    update_time                  timestamp comment '更新时间'
) comment '东方财富-沪深京板块-行业板块'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_dc_stock_quotes_di;
create table if not exists ods_dc_stock_quotes_di
(
    trade_date     date comment '交易日期',
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    open_price     decimal(20, 4) comment '开盘价',
    close_price    decimal(20, 4) comment '收盘价',
    high_price     decimal(20, 4) comment '最高价',
    low_price      decimal(20, 4) comment '最低价',
    volume         bigint comment '成交量',
    turnover       decimal(20, 4) comment '成交额',
    amplitude      decimal(20, 4) comment '振幅',
    change_percent decimal(20, 4) comment '涨跌幅',
    change_amount  decimal(20, 4) comment '涨跌额',
    turnover_rate  decimal(20, 4) comment '换手率',
    update_time    timestamp comment '更新时间'
) comment '东财沪深A股行情表 （后复权）'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_dc_stock_tfp_di;
create table if not exists ods_dc_stock_tfp_di
(
    trade_date                date comment '交易日期',
    stock_code                string comment '股票代码',
    stock_name                string comment '股票名称',
    suspension_time           string comment '停牌时间',
    suspension_deadline       string comment '停牌截止时间',
    suspension_period         string comment '停牌期限',
    suspension_reason         string comment '停牌原因',
    belongs_market            string comment '所属市场',
    estimated_resumption_time string comment '预计复牌时间',
    update_time               timestamp comment '更新时间'
) comment '东方财富网-数据中心-特色数据-两市停复牌表'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_financial_analysis_indicator_di;
create table if not exists ods_financial_analysis_indicator_di
(
    announcement_date      date comment '公告日期',
    stock_code             string comment '股票代码',
    stock_name             string comment '股票名称',
    ps_business_cash_flow  decimal(20, 4) comment '每股经营性现金流(元)',
    return_on_equity       decimal(20, 4) comment '净资产收益率(%)',
    npadnrgal              decimal(20, 4) comment '扣除非经常性损益后的净利润(元)',
    net_profit_growth_rate decimal(20, 4) comment '净利润增长率(%)',
    update_time            timestamp comment '更新时间'
) comment '新浪财经-财务分析-财务指标'
    partitioned by (td date comment '分区_公告日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_lg_indicator_di;
create table if not exists ods_lg_indicator_di
(
    trade_date         date comment '交易日期',
    stock_code         string comment '股票代码',
    stock_name         string comment '股票名称',
    pe                 decimal(20, 4) comment '市盈率',
    pe_ttm             decimal(20, 4) comment '市盈率TTM',
    pb                 decimal(20, 4) comment '市净率',
    ps                 decimal(20, 4) comment '市销率',
    ps_ttm             decimal(20, 4) comment '市销率TTM',
    dv_ratio           decimal(20, 4) comment '股息率',
    dv_ttm             decimal(20, 4) comment '股息率TTM',
    total_market_value decimal(20, 4) comment '总市值',
    update_time        timestamp comment '更新时间'
) comment '乐咕乐股-A 股个股指标表 没有京股数据会是随机数'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_stock_lhb_detail_em_di;
create table if not exists ods_stock_lhb_detail_em_di
(
    trade_date               date comment '交易日期',
    ranking                  int comment '排名',
    stock_code               string comment '股票代码',
    stock_name               string comment '股票名称',
    close_price              decimal(20, 4) comment '收盘价',
    change_percent           decimal(20, 4) comment '涨跌幅',
    circulating_market_value decimal(20, 4) comment '流通市值',
    turnover_rate            decimal(20, 4) comment '换手率',
    interpret                string comment '解读',
    reason_for_lhb           string comment '上榜原因',
    lhb_net_buy              decimal(20, 4) comment '龙虎榜净买额',
    lhb_buy_amount           decimal(20, 4) comment '龙虎榜买入额',
    lhb_sell_amount          decimal(20, 4) comment '龙虎榜卖出额',
    lhb_turnover             decimal(20, 4) comment '龙虎榜成交额',
    total_turnover           decimal(20, 4) comment '市场总成交额',
    nbtt                     decimal(20, 4) comment '净买额占总成交比',
    ttt                      decimal(20, 4) comment '成交额占总成交比',
    update_time              timestamp comment '更新时间'
) comment '东方财富网-数据中心-龙虎榜单-龙虎榜详情'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_stock_lrb_em_di;
create table if not exists ods_stock_lrb_em_di
(
    announcement_date        date comment '公告日期',
    stock_code               string comment '股票代码',
    stock_name               string comment '股票名称',
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
    update_time              timestamp comment '更新时间'
) comment '东方财富-数据中心-年报季报-业绩快报-利润表'
    partitioned by (td date comment '分区_公告日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

--股票指数