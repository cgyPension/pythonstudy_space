create database stock;
use stock;

-- TODO =========================================================  xx  =====================================================================
drop table if exists ods_dc_stock_concept_plate_rt_di;
create table if not exists ods_dc_stock_concept_plate_rt_di
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

drop table if exists ods_dc_stock_concept_plate_cons_di;
create table if not exists ods_dc_stock_concept_plate_cons_di
(
    trade_date    date comment '交易日期',
    stock_code    string comment '股票代码',
    stock_name    string comment '股票名称',
    concept_plate_code string comment '概念板块代码',
    concept_plate string comment '概念板块名称',
    update_time   timestamp comment '更新时间'
) comment ' 东方财富-沪深板块-概念板块-板块成份股'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_dc_stock_concept_plate_hist_di;
create table if not exists ods_dc_stock_concept_plate_hist_di
(
    trade_date     date comment '交易日期',
    concept_plate_code  string comment '概念板块代码',
    concept_plate  string comment '概念板块名称',
    open_price     decimal(20, 4) comment '开盘价',
    close_price    decimal(20, 4) comment '收盘价',
    high_price     decimal(20, 4) comment '最高价',
    low_price      decimal(20, 4) comment '最低价',
    change_percent decimal(20, 4) comment '涨跌幅',
    change_amount  decimal(20, 4) comment '涨跌额',
    volume         bigint comment '成交量',
    turnover       decimal(20, 4) comment '成交额',
    amplitude      decimal(20, 4) comment '振幅',
    turnover_rate  decimal(20, 4) comment '换手率',
    update_time                  timestamp comment '更新时间'
) comment '东方财富-沪深板块-概念板块-历史行情数据'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_dc_stock_industry_plate_rt_di;
create table if not exists ods_dc_stock_industry_plate_rt_di
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

drop table if exists ods_dc_stock_industry_plate_cons_di;
create table if not exists ods_dc_stock_industry_plate_cons_di
(
    trade_date    date comment '交易日期',
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    industry_plate_code string comment '行业板块代码',
    industry_plate string comment '行业板块名称',
    update_time    timestamp comment '更新时间'
) comment '东方财富-沪深板块-行业板块-板块成份股'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_dc_stock_industry_plate_hist_di;
create table if not exists ods_dc_stock_industry_plate_hist_di
(
    trade_date     date comment '交易日期',
    industry_plate_code string comment '行业板块代码',
    industry_plate string comment '行业板块名称',
    open_price     decimal(20, 4) comment '开盘价',
    close_price    decimal(20, 4) comment '收盘价',
    high_price     decimal(20, 4) comment '最高价',
    low_price      decimal(20, 4) comment '最低价',
    change_percent decimal(20, 4) comment '涨跌幅',
    change_amount  decimal(20, 4) comment '涨跌额',
    volume         bigint comment '成交量',
    turnover       decimal(20, 4) comment '成交额',
    amplitude      decimal(20, 4) comment '振幅',
    turnover_rate  decimal(20, 4) comment '换手率',
    update_time                  timestamp comment '更新时间'
) comment '东方财富-沪深板块-行业板块-历史行情数据'
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


drop table if exists ods_trade_date_hist_sina_df;
create table if not exists ods_trade_date_hist_sina_df
(
    date_id     int comment '日期id',
    trade_date  date comment '交易日期',
    update_time timestamp comment '更新时间'
) comment '新浪财经的股票交易日历数据'
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');


drop table if exists ods_stock_zt_pool_di;
create table if not exists ods_stock_zt_pool_di
(
    trade_date     date comment '交易日期',
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    change_percent decimal(20, 4) comment '涨跌幅',
    new_price    decimal(20, 4) comment '最新价',
    turnover       decimal(20, 4) comment '成交额',
    circulating_market_value decimal(20, 4) comment '流通市值',
    total_market_value decimal(20, 4) comment '总市值',
    turnover_rate  decimal(20, 4) comment '换手率',
    Sealing_amount  decimal(20, 4) comment '封板资金',
    first_Sealing_time  string comment '首次封板时间',
    last_Sealing_time  string comment '最后封板时间',
    bomb_Sealing_nums  int comment '炸板数',
    zt_tj  string comment '涨停统计',
    lx_Sealing_nums  int comment '连板数',
    industry_plate string comment '行业板块',
    update_time    timestamp comment '更新时间'
) comment '东方财富网-行情中心-涨停板行情-涨停股池'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_stock_strong_pool_di;
create table if not exists ods_stock_strong_pool_di
(
    trade_date     date comment '交易日期',
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    change_percent decimal(20, 4) comment '涨跌幅',
    new_price    decimal(20, 4) comment '最新价',
    zt_price    decimal(20, 4) comment '涨停价',
    turnover       decimal(20, 4) comment '成交额',
    circulating_market_value decimal(20, 4) comment '流通市值',
    total_market_value decimal(20, 4) comment '总市值',
    turnover_rate  decimal(20, 4) comment '换手率',
    speed_up decimal(20, 4) comment '涨速',
    is_new_g  int comment '是否新高',
    volume_ratio_5d  decimal(20, 4) comment '量比：过去5个交易日',
    zt_tj  string comment '涨停统计',
    selection_reason  int comment '强势股入选理由 1:60日新高, 2:近期多次涨停, 3:60日新高且近期多次涨停',
    industry_plate string comment '行业板块',
    update_time    timestamp comment '更新时间'
) comment '东方财富网-行情中心-涨停板行情-强势股池'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_stock_hot_rank_wc_di;
create table if not exists ods_stock_hot_rank_wc_di
(
    trade_date     date comment '交易日期',
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    new_price     decimal(20, 4) comment '最新价',
    change_percent decimal(20, 4) comment '涨跌幅',
    hot  decimal(20, 1) comment '个股热度',
    hot_rank  int comment '个股热度排名',
    update_time    timestamp comment '更新时间'
) comment '问财-热门股票排名数据'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

--股票指数
drop table if exists ods_dc_index_di;
create table if not exists ods_dc_index_di
(
    trade_date     date comment '交易日期',
    index_code     string comment '指数代码',
    index_name     string comment '指数名称',
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
) comment '东方财富网-中国股票指数-行情数据'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_stock_hsgt_stock_di;
create table if not exists ods_stock_hsgt_stock_di
(
    trade_date     date comment '交易日期',
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    change_percent decimal(20, 4) comment '涨跌幅',
    hold_stock_nums  decimal(20, 4) comment '持股数量(万股)',
    hold_market  decimal(20, 4) comment '持股市值(万元)',
    hold_stock_nums_rate  decimal(20, 4) comment '持股数量占发行股百分比',
    hold_market_1d decimal(20, 4) comment '持股市值变化-1日',
    hold_market_5d  decimal(20, 4) comment '持股市值变化-5日',
    hold_market_10d  decimal(20, 4) comment '持股市值变化-10日',
    update_time    timestamp comment '更新时间'
) comment '东方财富网-数据中心-沪深港通-沪深港通持股-每日个股统计'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 接口有问题
drop table if exists ods_stock_fund_holder_di;
create table if not exists ods_stock_fund_holder_di
(
    announcement_date      date comment '公告日期',
    stock_code             string comment '股票代码',
    stock_name             string comment '股票名称',
    fund_code             string comment '基金代码',
    fund_name             string comment '基金名称',
    hold_stock_nums  decimal(20, 4) comment '持股数量',
    hold_circulating_nums_rate decimal(20, 4) comment '持股占流通股比例(%)',
    hold_market  decimal(20, 4) comment '持股市值',
    update_time            timestamp comment '更新时间'
) comment '新浪财经-股本股东-基金持股'
    partitioned by (td date comment '分区_公告日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists ods_stock_fund_hold_detail_di;
create table if not exists ods_stock_fund_hold_detail_di
(
    announcement_date      date comment '公告日期',
    stock_code             string comment '股票代码',
    stock_name             string comment '股票名称',
    fund_code             string comment '基金代码',
    fund_name             string comment '基金名称',
    hold_stock_nums  decimal(20, 4) comment '持股数量',
    hold_market  decimal(20, 4) comment '持股市值',
    hold_nums_rate decimal(20, 4) comment '持股占总股本比例(%)',
    hold_nums_circulating_rate decimal(20, 4) comment '持股占流通股比例(%)',
    update_time            timestamp comment '更新时间'
) comment '东方财富网-数据中心-主力数据-基金持仓-基金持仓明细表'
    partitioned by (td date comment '分区_公告日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

