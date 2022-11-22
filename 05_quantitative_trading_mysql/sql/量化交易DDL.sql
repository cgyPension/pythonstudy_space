create database stock;
use stock;

-- TODO =========================================================  ods  =====================================================================
drop table if exists ods_dc_stock_quotes_di;

create table ods_dc_stock_quotes_di
(
    trade_date date not null comment '交易日期',
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',
    open_price  decimal(20, 4) null comment '开盘价',
    close_price  decimal(20, 4) null comment '收盘价',
    high_price  decimal(20, 4) null comment '最高价',
    low_price  decimal(20, 4) null comment '最低价',
    volume  bigint null comment '成交量',
    turnover  decimal(20, 4) null comment '成交额',
    amplitude   decimal(20, 4) null comment '振幅',
    change_percent  decimal(20, 4) null comment '涨跌幅',
    change_amount  decimal(20, 4) null comment '涨跌额',
    turnover_rate  decimal(20, 4) null comment '换手率',
     create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (trade_date, stock_code)
) comment '东财沪深A股行情表 （后复权）';

-- --财务数据
create table ods_lg_indicator_di
(
    trade_date date not null comment '交易日期',
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',
    pe  decimal(20, 4) null comment '市盈率',
    pe_ttm  decimal(20, 4) null comment '市盈率TTM',
    pb  decimal(20, 4) null comment '市净率',
    ps  decimal(20, 4) null comment '市销率',
    ps_ttm  decimal(20, 4) null comment '市销率TTM',
    dv_ratio  decimal(20, 4) null comment '股息率',
    dv_ttm  decimal(20, 4) null comment '股息率TTM',
    total_market_value  decimal(20, 4) null comment '总市值',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (trade_date, stock_code)
) comment '乐咕乐股-A 股个股指标表 没有京股数据会是随机数';

create table ods_163_stock_quotes_di
(
    trade_date date not null comment '交易日期',
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',
    before_open_price  decimal(20, 4) null comment '前收盘价',
    open_price  decimal(20, 4) null comment '开盘价',
    close_price  decimal(20, 4) null comment '收盘价',
    high_price  decimal(20, 4) null comment '最高价',
    low_price  decimal(20, 4) null comment '最低价',
    volume  decimal(20, 4) null comment '成交量',
    turnover  decimal(20, 4) null comment '成交额',
    change_percent  decimal(20, 4) null comment '涨跌幅',
    change_amount  decimal(20, 4) null comment '涨跌额',
    turnover_rate  decimal(20, 4) null comment '换手率',
    total_market_value  decimal(20, 4) null comment '总市值',
    circulating_market_value  decimal(20, 4) null comment '流通市值',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (trade_date, stock_code)
) comment '网易财经沪深A股行情表（不复权）';

--行业板块
create table ods_dc_stock_industry_plate_name_di
(
    trade_date date not null comment '交易日期',
    industry_plate_code varchar(26) not null comment '行业板块代码',
    industry_plate varchar(26) null comment '行业板块名称',

    new_price  decimal(20, 4) null comment '最新价',
    change_amount  decimal(20, 4) null comment '涨跌额',
    change_percent  decimal(20, 4) null comment '涨跌幅',
    total_market_value  decimal(20, 4) null comment '总市值',
    turnover_rate  decimal(20, 4) null comment '换手率',

    rise_num  int null comment '上涨家数',
    fall_num  int null comment '下跌家数',
    leading_stock_name varchar(26) null comment '领涨股票名称',
    leading_stock_change_percent  decimal(20, 4) null comment '领涨股票-涨跌幅',

    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (trade_date, industry_plate_code)
) comment '东方财富-沪深京板块-行业板块';

create table ods_dc_stock_industry_plate_df
(
    stock_code varchar(26) not null comment '股票代码' primary key,
    stock_name varchar(26) null comment '股票名称',
    industry_plate   varchar(26) null comment '行业板块',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间'
) comment '东方财富-沪深板块-行业板块-板块成份股';


--概念板块
create table ods_dc_stock_concept_plate_name_di
(
    trade_date date not null comment '交易日期',
    concept_plate_code varchar(26) not null comment '概念板块代码',
    concept_plate varchar(26) null comment '概念板块名称',

    new_price  decimal(20, 4) null comment '最新价',
    change_amount  decimal(20, 4) null comment '涨跌额',
    change_percent  decimal(20, 4) null comment '涨跌幅',
    total_market_value  decimal(20, 4) null comment '总市值',
    turnover_rate  decimal(20, 4) null comment '换手率',

    rise_num  int null comment '上涨家数',
    fall_num  int null comment '下跌家数',
    leading_stock_name varchar(26) null comment '领涨股票名称',
    leading_stock_change_percent  decimal(20, 4) null comment '领涨股票-涨跌幅',

    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (trade_date, concept_plate_code)
) comment '东方财富-沪深板块-概念板块';

create table ods_dc_stock_concept_plate_df
(
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',
    concept_plate   varchar(26) not null comment '概念板块',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (stock_code, concept_plate)
) comment ' 东方财富-沪深板块-概念板块-板块成份股';

create table ods_dc_stock_tfp_di
(
    trade_date date not null comment '交易日期',
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',
    suspension_time  varchar(26) null comment '停牌时间',
    suspension_deadline  varchar(26) null comment '停牌截止时间',
    suspension_period  varchar(26) null comment '停牌期限',
    suspension_reason  varchar(26) null comment '停牌原因',
    belongs_market  varchar(26) null comment '所属市场',
    estimated_resumption_time  varchar(26) null comment '预计复牌时间',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (trade_date, stock_code)
) comment '东方财富网-数据中心-特色数据-两市停复牌表';

create table ods_stock_lrb_em_di
(
    announcement_date date not null comment '公告日期',
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',
    net_profit  decimal(20, 4) null comment '净利润',
    net_profit_yr  decimal(20, 4) null comment '净利润同比',
    total_business_income  decimal(20, 4) null comment '营业总收入',
    total_business_income_yr decimal(20, 4) null comment '营业总收入同比',
    business_fee  decimal(20, 4) null comment '营业总支出-营业支出',
    sales_fee  decimal(20, 4) null comment '营业总支出-销售费用',
    management_fee  decimal(20, 4) null comment '营业总支出-管理费用',
    finance_fee  decimal(20, 4) null comment '营业总支出-财务费用',
    total_business_fee  decimal(20, 4) null comment '营业总支出-营业总支出',
    business_profit  decimal(20, 4) null comment '营业利润',
    total_profit  decimal(20, 4) null comment '利润总额',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (announcement_date, stock_code)
) comment '东方财富-数据中心-年报季报-业绩快报-利润表';

create table ods_financial_analysis_indicator_di
(
    announcement_date date not null comment '公告日期',
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',

    ps_business_cash_flow  decimal(20, 4) null comment '每股经营性现金流(元)',
    return_on_equity  decimal(20, 4) null comment '净资产收益率(%)',
    npadnrgal  decimal(20, 4) null comment '扣除非经常性损益后的净利润(元)',
    net_profit_growth_rate decimal(20, 4) null comment '净利润增长率(%)',

    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (announcement_date, stock_code)
) comment '新浪财经-财务分析-财务指标 这接口太脆了会有部分丢数据';

create table ods_stock_lhb_detail_em_di
(
    trade_date date not null comment '交易日期',
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',
    close_price  decimal(20, 4) null comment '收盘价',
    change_percent  decimal(20, 4) null comment '涨跌幅',
    circulating_market_value  decimal(20, 4) null comment '流通市值',
    turnover_rate  decimal(20, 4) null comment '换手率',

    interpret varchar(26) null comment '解读',
    reason_for_lhb varchar(26) null comment '上榜原因',

    lhb_net_buy  decimal(20, 4) null comment '龙虎榜净买额',
    lhb_buy_amount  decimal(20, 4) null comment '龙虎榜买入额',
    lhb_sell_amount  decimal(20, 4) null comment '龙虎榜卖出额',
    lhb_turnover  decimal(20, 4) null comment '龙虎榜成交额',
    total_turnover  decimal(20, 4) null comment '市场总成交额',
    nbtt  decimal(20, 4) null comment '净买额占总成交比',
    ttt  decimal(20, 4) null comment '成交额占总成交比',

    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (trade_date, stock_code)
) comment '东方财富网-数据中心-龙虎榜单-龙虎榜详情';



-- TODO =========================================================  dim  =====================================================================
create table dim_dc_stock_plate_df
(
    stock_code varchar(26) not null comment '股票代码' primary key,
    stock_name varchar(26) null comment '股票名称',
    industry_plate   varchar(26) null comment '行业板块',
    concept_plates   varchar(226) null comment '概念板块 ,拼接',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间'
) comment ' 东方财富-板块维表';

--股票加个标签拼接s字段
create table dim_stock_label
(
    stock_label_id    int auto_incremen comment '股票标签id' primary key,
    stock_label_name    varchar(26) null comment '股票标签名称 负面标签后缀用-',
    business_caliber   varchar(226) null comment '业务口径（计算逻辑、文字指标口径）',
--     technical_caliber        varchar(226) null comment '技术口径',
    label_type int comment '标签类型：1规则标签; 2统计标签; 3挖掘标签',
    is_factor  int default 0 comment '是否因子 0否 1是',
    stock_label_description varchar(226) null comment '标签描述',
    create_time       datetime(3) default CURRENT_TIMESTAMP (3) comment '创建时间',
    update_time       datetime(3) on update current_timestamp (3) comment '更新时间'
) comment '股票标签';
insert into dim_stock_label (stock_label_name,business_caliber,label_type,is_factor,stock_label_description)
VALUES ('小市值','总市值<33.3%排名',1,1,'小市值官网为20~30亿;用percent_rank() <33.3%排名'),
       ('上穿5日均线','最高价>N日移动平均线=N日收盘价之和/N',2,0,''),
       ('上穿10日均线','最高价>N日移动平均线=N日收盘价之和/N',2,0,''),
       ('上穿20日均线','最高价>N日移动平均线=N日收盘价之和/N',2,0,''),
       ('上穿30日均线','最高价>N日移动平均线=N日收盘价之和/N',2,0,''),
       ('上穿60日均线','最高价>N日移动平均线=N日收盘价之和/N',2,0,''),

       ('连续两天放量-','连续两天量比>1',2,1,''),
       ('连续两天放量且低收-','连续两天量比>1 连续两天放量且连续两天收盘比开盘低',2,1,''),
       ('今天龙虎榜','东财龙虎榜',1,1,''),
       ('最近60天龙虎榜','东财龙虎榜',1,1,''),

       ('adtm买入信号','',''),
       ('boll买入信号','',''),
       ('bbiboll买入信号','',''),
       ('dpo买入信号','',''),
       ('cci买入信号','',''),
       ('rsi金叉','',''),
       ('cr金叉','',''),
       ('塔形底','',''),
       ('业绩预增','',''),

       ('adtm卖出信号','',''),
       ('boll卖出信号','',''),
       ('bbiboll卖出信号','',''),
       ('dpo卖出信号','',''),
       ('cci卖出信号','',''),
       ('超卖','',''),
       ('首板涨停','',''),
       ('高量柱','',''),
       ('消费股','',''),
       ('低价股','',''),
       ('平台突破','',''),
       ('股权集中','',''),
       ('机构重仓','',''),
       ('持续放量','',''),
       ('放巨量','',''),
       ('价升量涨','',''),
       ('看涨吞没','','');

-- 标签组成策略
-- 回测时间 回测的一些指标子段输入到dws（股票角度） ads dim（策略角度）
create table dim_stock_strategy
(
    stock_strategy_id    int auto_incremen comment '股票策略id' primary key,
    stock_strategy_name varchar(226) null comment '股票策略名称 股票标签名称 +拼接',
--     stock_label_ids    varchar(26) auto_incremen comment '股票标签id ,拼接',
    holding_yield_td decimal(20, 4) null comment '截止当天持股收益率',
    holding_yield_before decimal(20, 4) null comment '上期持股收益率',
    strategy_type  int default 0 comment '策略类型：0选股策略;1择时策略',
    annual_yield  decimal(20, 4) null comment '年化收益率',
    max_retrace  decimal(20, 4) null comment '最大回撤',
    annual_max_retrace  decimal(20, 4) null comment '年化收益率/最大回撤',
    backtest_start_date   date null comment '回测数据开始日期',
    backtest_end_date   date null comment '回测数据结束日期',
    create_time    datetime(3) default CURRENT_TIMESTAMP (3) comment '创建时间',
    update_time    datetime(3) on update current_timestamp (3) comment '更新时间'
) comment '股票策略';
-- 截止当天持股收益率 上期持股收益率等 这些字段用set 更新操作
insert into dim_stock_strategy (stock_strategy_name,strategy_type)
VALUES ('小市值+PEG+EBIT+',0),

-- 成交额来说：大概至少要有20元以上的利润，1%的收益率
-- 买2000元的手续费：佣金=2000*0.0005=1<5=5 过户费=2000*0.002%=0.04 总：5.04
-- 卖2000元的手续费：佣金=2000*0.0005=1<5=5 印花税=2000*0.1%=2 过户费=2000*0.002%=0.04	总：7.04（成交额上涨 则会上涨）
create table dim_stock_fee_rate
(
    commission_fee decimal(20, 4) null comment '佣金费率',
    transfer_fee decimal(20, 4) null comment '过户费率',
    stamp_duty_fee decimal(20, 4) null comment '印花税率 只有卖时收'
) comment '股票手续费率';
insert into dim_stock_fee_rate (commission_fee, transfer_fee, stamp_duty_fee)
VALUES (0.0005,0.00002,0.001);

-- TODO =========================================================  dwd  =====================================================================
create table dwd_stock_quotes_di
(
    trade_date date not null comment '交易日期',
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',
    open_price  decimal(20, 4) null comment '开盘价',
    close_price  decimal(20, 4) null comment '收盘价',
    high_price  decimal(20, 4) null comment '最高价',
    low_price  decimal(20, 4) null comment '最低价',
    volume  decimal(20, 4) null comment '成交量',
    volume_ratio_1d  decimal(20, 4) null comment '量比_1d 与昨日对比',
    volume_ratio_5d  decimal(20, 4) null comment '量比：过去5个交易日',
    turnover  decimal(20, 4) null comment '成交额',
    amplitude   decimal(20, 4) null comment '振幅',
    change_percent  decimal(20, 4) null comment '涨跌幅',
    change_amount  decimal(20, 4) null comment '涨跌额',
    turnover_rate  decimal(20, 4) null comment '换手率',
    turnover_rate_5d  decimal(20, 4) null comment '5日平均换手率',
    turnover_rate_10d  decimal(20, 4) null comment '10日平均换手率',

    total_market_value decimal(20, 4) null comment '总市值',
    industry_plate   varchar(26) null comment '行业板块',
    concept_plates   varchar(226) null comment '概念板块 ,拼接',

    pe                 decimal(20, 4) null comment '市盈率',
    pe_ttm             decimal(20, 4) null comment '市盈率TTM',
    pb                 decimal(20, 4) null comment '市净率',
    ps                 decimal(20, 4) null comment '市销率',
    ps_ttm             decimal(20, 4) null comment '市销率TTM',
    dv_ratio           decimal(20, 4) null comment '股息率',
    dv_ttm             decimal(20, 4) null comment '股息率TTM',

    net_profit  decimal(20, 4) null comment '净利润',
    net_profit_yr  decimal(20, 4) null comment '净利润同比',
    total_business_income  decimal(20, 4) null comment '营业总收入',
    total_business_income_yr decimal(20, 4) null comment '营业总收入同比',
    business_fee  decimal(20, 4) null comment '营业总支出-营业支出',
    sales_fee  decimal(20, 4) null comment '营业总支出-销售费用',
    management_fee  decimal(20, 4) null comment '营业总支出-管理费用',
    finance_fee  decimal(20, 4) null comment '营业总支出-财务费用',
    total_business_fee  decimal(20, 4) null comment '营业总支出-营业总支出',
    business_profit  decimal(20, 4) null comment '营业利润',
    total_profit  decimal(20, 4) null comment '利润总额',
    ps_business_cash_flow  decimal(20, 4) null comment '每股经营性现金流(元)',
    return_on_equity  decimal(20, 4) null comment '净资产收益率(%)',
    npadnrgal  decimal(20, 4) null comment '扣除非经常性损益后的净利润(元)',
    net_profit_growth_rate decimal(20, 4) null comment '净利润增长率(%)',

    interpret varchar(26) null comment '解读',
    reason_for_lhb varchar(26) null comment '上榜原因',
    lhb_net_buy  decimal(20, 4) null comment '龙虎榜净买额',
    lhb_buy_amount  decimal(20, 4) null comment '龙虎榜买入额',
    lhb_sell_amount  decimal(20, 4) null comment '龙虎榜卖出额',
    lhb_turnover  decimal(20, 4) null comment '龙虎榜成交额',
    total_turnover  decimal(20, 4) null comment '市场总成交额',
    nbtt  decimal(20, 4) null comment '净买额占总成交比',
    ttt  decimal(20, 4) null comment '成交额占总成交比',
    lhb_num_5d   int default 0 comment '最近5天_龙虎榜上榜次数',
    lhb_num_10d   int default 0 comment '最近10天_龙虎榜上榜次数',
    lhb_num_30d   int default 0 comment '最近30天_龙虎榜上榜次数',
    lhb_num_60d   int default 0 comment '最近60天_龙虎榜上榜次数',

    ma_5d decimal(20, 4) null comment '5日均线',
    ma_10d decimal(20, 4) null comment '10日均线',
    ma_20d decimal(20, 4) null comment '20日均线',
    ma_30d decimal(20, 4) null comment '30日均线',
    ma_60d decimal(20, 4) null comment '60日均线',

--     stock_label_ids   varchar(26) null comment '股票标签id ,拼接',
    stock_label_names   varchar(226) null comment '股票标签名称 负面标签后缀用- ,拼接',
    stock_label_num   int default 0 comment '股票标签数量',
--     factor_ids   varchar(26) null comment '因子标签id ,拼接',
    factor_names   varchar(226) null comment '因子标签名称 负面因子后缀用- ,拼接',
    factor_score   int default 0 comment '因子分数 负面因子后缀用-',

    holding_yield_2d  decimal(20, 2) null comment '持股2日后收益率',
    holding_yield_5d  decimal(20, 2) null comment '持股5日后收益率',

    suspension_time  varchar(26) null comment '停牌时间',
    suspension_deadline  varchar(26) null comment '停牌截止时间',
    suspension_period  varchar(26) null comment '停牌期限',
    suspension_reason  varchar(26) null comment '停牌原因',
    belongs_market  varchar(26) null comment '所属市场',
    estimated_resumption_time  varchar(26) null comment '预计复牌时间',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (trade_date, stock_code),
    index index_total_market_value (total_market_value)
) comment '沪深A股行情表';
--持股收益 用开盘价 收盘价

-- TODO =========================================================  ads  =====================================================================
-- 排除停牌数据
create table ads_stock_suggest_di
(
    trade_date date not null comment '交易日期',
    stock_code varchar(26) not null comment '股票代码',
    stock_name varchar(26) null comment '股票名称',
    open_price  decimal(20, 4) null comment '开盘价',
    close_price  decimal(20, 4) null comment '收盘价',
    high_price  decimal(20, 4) null comment '最高价',
    low_price  decimal(20, 4) null comment '最低价',
    volume  decimal(20, 4) null comment '成交量',
    volume_ratio_1d  decimal(20, 4) null comment '量比_1d 与昨日对比',
    volume_ratio_5d  decimal(20, 4) null comment '量比：过去5个交易日',
    turnover  decimal(20, 4) null comment '成交额',
    amplitude   decimal(20, 4) null comment '振幅',
    change_percent  decimal(20, 4) null comment '涨跌幅',
    change_amount  decimal(20, 4) null comment '涨跌额',
    turnover_rate  decimal(20, 4) null comment '换手率',
    turnover_rate_5d  decimal(20, 4) null comment '5日平均换手率',
    turnover_rate_10d  decimal(20, 4) null comment '10日平均换手率',

    total_market_value decimal(20, 4) null comment '总市值',
    industry_plate   varchar(26) null comment '行业板块',
    concept_plates   varchar(226) null comment '概念板块 ,拼接',

    pe                 decimal(20, 4) null comment '市盈率',
    pe_ttm             decimal(20, 4) null comment '市盈率TTM',
    pb                 decimal(20, 4) null comment '市净率',
    ps                 decimal(20, 4) null comment '市销率',
    ps_ttm             decimal(20, 4) null comment '市销率TTM',
    dv_ratio           decimal(20, 4) null comment '股息率',
    dv_ttm             decimal(20, 4) null comment '股息率TTM',

    net_profit  decimal(20, 4) null comment '净利润',
    net_profit_yr  decimal(20, 4) null comment '净利润同比',
    total_business_income  decimal(20, 4) null comment '营业总收入',
    total_business_income_yr decimal(20, 4) null comment '营业总收入同比',
    business_fee  decimal(20, 4) null comment '营业总支出-营业支出',
    sales_fee  decimal(20, 4) null comment '营业总支出-销售费用',
    management_fee  decimal(20, 4) null comment '营业总支出-管理费用',
    finance_fee  decimal(20, 4) null comment '营业总支出-财务费用',
    total_business_fee  decimal(20, 4) null comment '营业总支出-营业总支出',
    business_profit  decimal(20, 4) null comment '营业利润',
    total_profit  decimal(20, 4) null comment '利润总额',
    ps_business_cash_flow  decimal(20, 4) null comment '每股经营性现金流(元)',
    return_on_equity  decimal(20, 4) null comment '净资产收益率(%)',
    npadnrgal  decimal(20, 4) null comment '扣除非经常性损益后的净利润(元)',
    net_profit_growth_rate decimal(20, 4) null comment '净利润增长率(%)',

    interpret varchar(26) null comment '解读',
    reason_for_lhb varchar(26) null comment '上榜原因',
    lhb_net_buy  decimal(20, 4) null comment '龙虎榜净买额',
    lhb_buy_amount  decimal(20, 4) null comment '龙虎榜买入额',
    lhb_sell_amount  decimal(20, 4) null comment '龙虎榜卖出额',
    lhb_turnover  decimal(20, 4) null comment '龙虎榜成交额',
    total_turnover  decimal(20, 4) null comment '市场总成交额',
    nbtt  decimal(20, 4) null comment '净买额占总成交比',
    ttt  decimal(20, 4) null comment '成交额占总成交比',
    lhb_num_5d   int default 0 comment '最近5天_龙虎榜上榜次数',
    lhb_num_10d   int default 0 comment '最近10天_龙虎榜上榜次数',
    lhb_num_30d   int default 0 comment '最近30天_龙虎榜上榜次数',
    lhb_num_60d   int default 0 comment '最近60天_龙虎榜上榜次数',

    ma_5d decimal(20, 4) null comment '5日均线',
    ma_10d decimal(20, 4) null comment '10日均线',
    ma_20d decimal(20, 4) null comment '20日均线',
    ma_30d decimal(20, 4) null comment '30日均线',
    ma_60d decimal(20, 4) null comment '60日均线',

--     stock_label_ids   varchar(26) null comment '股票标签id ,拼接',
    stock_label_names   varchar(226) null comment '股票标签名称 负面标签后缀用- ,拼接',
    stock_label_num   int default 0 comment '股票标签数量',
--     factor_ids   varchar(26) null comment '因子标签id ,拼接',
    factor_names   varchar(226) null comment '因子标签名称 负面因子后缀用- ,拼接',
    factor_score   int default 0 comment '因子分数 负面因子后缀用-',
    stock_strategy_name varchar(226) null comment '股票策略名称 股票标签名称 +拼接',
    stock_strategy_ranking varchar(26) null comment '策略内排行 dense_rank',
    holding_yield_2d  decimal(20, 2) null comment '持股2日后收益率',
    holding_yield_5d  decimal(20, 2) null comment '持股5日后收益率',

--     suggest_buy_price decimal(20, 2) null comment '推荐买价',
--     suggest_stop_profit decimal(20, 2) null comment '推荐止盈',
--     suggest_stop_loss decimal(20, 2) null comment '推荐止损',
--     is_monitor int default 0 comment '实时监测 0否 1是',
--     annual_yield  decimal(20, 4) null comment '年化收益率',
--     max_retrace  decimal(20, 4) null comment '最大回撤',
--     annual_max_retrace  decimal(20, 4) null comment '年化收益率/最大回撤',
--     backtest_start_date   date null comment '回测数据开始日期',
--     backtest_end_date   date null comment '回测数据结束日期',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (create_time, stock_code),
    index index_trade_stock (trade_date,stock_code)
) comment '股票推荐 （top10）';



--同花顺接口个人资产情况

-- TODO =========================================================  tmp  =====================================================================
create table tmp_ods_dc_stock_quotes_df
(
    trade_date                date                                     not null comment '交易日期',
    stock_code                varchar(26)                              not null comment '股票代码',
    stock_name                varchar(26)                              null comment '股票名称',
    open_price                decimal(20, 4)                           null comment '开盘价',
    close_price               decimal(20, 4)                           null comment '收盘价',
    high_price                decimal(20, 4)                           null comment '最高价',
    low_price                 decimal(20, 4)                           null comment '最低价',
    volume                    decimal(20, 4)                           null comment '成交量',
    volume_ratio_1d           decimal(20, 4)                           null comment '量比_1d 与昨日对比',
    volume_ratio_5d           decimal(20, 4)                           null comment '量比：过去5个交易日',
    turnover                  decimal(20, 4)                           null comment '成交额',
    amplitude                 decimal(20, 4)                           null comment '振幅',
    change_percent            decimal(20, 4)                           null comment '涨跌幅',
    change_amount             decimal(20, 4)                           null comment '涨跌额',
    turnover_rate             decimal(20, 4)                           null comment '换手率',
    turnover_rate_5d         decimal(20, 4)                           null comment '5日平均换手率',
    turnover_rate_10d         decimal(20, 4)                           null comment '10日平均换手率',
    ma_5d                     decimal(20, 4)                           null comment '5日均线',
    ma_10d                    decimal(20, 4)                           null comment '10日均线',
    ma_20d                    decimal(20, 4)                           null comment '20日均线',
    ma_30d                    decimal(20, 4)                           null comment '30日均线',
    ma_60d                    decimal(20, 4)                           null comment '60日均线',
    holding_yield_2d          decimal(20, 2)                           null comment '持股2日后收益率',
    holding_yield_5d          decimal(20, 2)                           null comment '持股5日后收益率',
    primary key (trade_date, stock_code),
    index index_stock_code (stock_code)
);

create table tmp_dwd_01
(
    trade_date               date           not null comment '交易日期',
    stock_code               varchar(26)    not null comment '股票代码',
    stock_name               varchar(26)    null comment '股票名称',
    open_price               decimal(20, 4) null comment '开盘价',
    close_price              decimal(20, 4) null comment '收盘价',
    high_price               decimal(20, 4) null comment '最高价',
    low_price                decimal(20, 4) null comment '最低价',
    volume                   decimal(20, 4) null comment '成交量',
    volume_ratio_1d          decimal(20, 4) null comment '量比_1d 与昨日对比',
    volume_ratio_5d          decimal(20, 4) null comment '量比：过去5个交易日',
    turnover                 decimal(20, 4) null comment '成交额',
    amplitude                decimal(20, 4) null comment '振幅',
    change_percent           decimal(20, 4) null comment '涨跌幅',
    change_amount            decimal(20, 4) null comment '涨跌额',
    turnover_rate            decimal(20, 4) null comment '换手率',
    turnover_rate_5d         decimal(20, 4) null comment '5日平均换手率',
    turnover_rate_10d        decimal(20, 4) null comment '10日平均换手率',
    total_market_value       decimal(20, 4) null comment '总市值',
    pe                       decimal(20, 4) null comment '市盈率',
    pe_ttm                   decimal(20, 4) null comment '市盈率TTM',
    pb                       decimal(20, 4) null comment '市净率',
    ps                       decimal(20, 4) null comment '市销率',
    ps_ttm                   decimal(20, 4) null comment '市销率TTM',
    dv_ratio                 decimal(20, 4) null comment '股息率',
    dv_ttm                   decimal(20, 4) null comment '股息率TTM',
    net_profit               decimal(20, 4) null comment '净利润',
    net_profit_yr            decimal(20, 4) null comment '净利润同比',
    total_business_income    decimal(20, 4) null comment '营业总收入',
    total_business_income_yr decimal(20, 4) null comment '营业总收入同比',
    business_fee             decimal(20, 4) null comment '营业总支出-营业支出',
    sales_fee                decimal(20, 4) null comment '营业总支出-销售费用',
    management_fee           decimal(20, 4) null comment '营业总支出-管理费用',
    finance_fee              decimal(20, 4) null comment '营业总支出-财务费用',
    total_business_fee       decimal(20, 4) null comment '营业总支出-营业总支出',
    business_profit          decimal(20, 4) null comment '营业利润',
    total_profit             decimal(20, 4) null comment '利润总额',
    ps_business_cash_flow    decimal(20, 4) null comment '每股经营性现金流(元)',
    return_on_equity         decimal(20, 4) null comment '净资产收益率(%)',
    npadnrgal                decimal(20, 4) null comment '扣除非经常性损益后的净利润(元)',
    net_profit_growth_rate   decimal(20, 4) null comment '净利润增长率(%)',
    interpret                varchar(26)    null comment '解读',
    reason_for_lhb           varchar(26)    null comment '上榜原因',
    lhb_net_buy              decimal(20, 4) null comment '龙虎榜净买额',
    lhb_buy_amount           decimal(20, 4) null comment '龙虎榜买入额',
    lhb_sell_amount          decimal(20, 4) null comment '龙虎榜卖出额',
    lhb_turnover             decimal(20, 4) null comment '龙虎榜成交额',
    total_turnover           decimal(20, 4) null comment '市场总成交额',
    nbtt                     decimal(20, 4) null comment '净买额占总成交比',
    ttt                      decimal(20, 4) null comment '成交额占总成交比',
    ma_5d                    decimal(20, 4) null comment '5日均线',
    ma_10d                   decimal(20, 4) null comment '10日均线',
    ma_20d                   decimal(20, 4) null comment '20日均线',
    ma_30d                   decimal(20, 4) null comment '30日均线',
    ma_60d                   decimal(20, 4) null comment '60日均线',
    holding_yield_2d         decimal(20, 2) null comment '持股2日后收益率',
    holding_yield_5d         decimal(20, 2) null comment '持股5日后收益率',
    is_lhb                   int default 0  null comment '是否龙虎榜 1是 0否',
    is_rise_ma_5d            int default 0  null comment '是否上穿5日均线 1是 0否',
    is_rise_ma_10d           int default 0  null comment '是否上穿10日均线 1是 0否',
    is_rise_ma_20d           int default 0  null comment '是否上穿20日均线 1是 0否',
    is_rise_ma_30d           int default 0  null comment '是否上穿30日均线 1是 0否',
    is_rise_ma_60d           int default 0  null comment '是否上穿60日均线 1是 0否',
    primary key (trade_date, stock_code),
    index index_stock_code (stock_code),
    index index_total_market_value (total_market_value)
);

create table tmp_dwd_02
(
    trade_date               date           not null comment '交易日期',
    stock_code               varchar(26)    not null comment '股票代码',
    stock_name               varchar(26)    null comment '股票名称',
    open_price               decimal(20, 4) null comment '开盘价',
    close_price              decimal(20, 4) null comment '收盘价',
    high_price               decimal(20, 4) null comment '最高价',
    low_price                decimal(20, 4) null comment '最低价',
    volume                   decimal(20, 4) null comment '成交量',
    volume_ratio_1d          decimal(20, 4) null comment '量比_1d 与昨日对比',
    volume_ratio_5d          decimal(20, 4) null comment '量比：过去5个交易日',
    turnover                 decimal(20, 4) null comment '成交额',
    amplitude                decimal(20, 4) null comment '振幅',
    change_percent           decimal(20, 4) null comment '涨跌幅',
    change_amount            decimal(20, 4) null comment '涨跌额',
    turnover_rate            decimal(20, 4) null comment '换手率',
    turnover_rate_5d         decimal(20, 4) null comment '5日平均换手率',
    turnover_rate_10d        decimal(20, 4) null comment '10日平均换手率',
    total_market_value       decimal(20, 4) null comment '总市值',
    pe                       decimal(20, 4) null comment '市盈率',
    pe_ttm                   decimal(20, 4) null comment '市盈率TTM',
    pb                       decimal(20, 4) null comment '市净率',
    ps                       decimal(20, 4) null comment '市销率',
    ps_ttm                   decimal(20, 4) null comment '市销率TTM',
    dv_ratio                 decimal(20, 4) null comment '股息率',
    dv_ttm                   decimal(20, 4) null comment '股息率TTM',
    net_profit               decimal(20, 4) null comment '净利润',
    net_profit_yr            decimal(20, 4) null comment '净利润同比',
    total_business_income    decimal(20, 4) null comment '营业总收入',
    total_business_income_yr decimal(20, 4) null comment '营业总收入同比',
    business_fee             decimal(20, 4) null comment '营业总支出-营业支出',
    sales_fee                decimal(20, 4) null comment '营业总支出-销售费用',
    management_fee           decimal(20, 4) null comment '营业总支出-管理费用',
    finance_fee              decimal(20, 4) null comment '营业总支出-财务费用',
    total_business_fee       decimal(20, 4) null comment '营业总支出-营业总支出',
    business_profit          decimal(20, 4) null comment '营业利润',
    total_profit             decimal(20, 4) null comment '利润总额',
    ps_business_cash_flow    decimal(20, 4) null comment '每股经营性现金流(元)',
    return_on_equity         decimal(20, 4) null comment '净资产收益率(%)',
    npadnrgal                decimal(20, 4) null comment '扣除非经常性损益后的净利润(元)',
    net_profit_growth_rate   decimal(20, 4) null comment '净利润增长率(%)',
    interpret                varchar(26)    null comment '解读',
    reason_for_lhb           varchar(26)    null comment '上榜原因',
    lhb_net_buy              decimal(20, 4) null comment '龙虎榜净买额',
    lhb_buy_amount           decimal(20, 4) null comment '龙虎榜买入额',
    lhb_sell_amount          decimal(20, 4) null comment '龙虎榜卖出额',
    lhb_turnover             decimal(20, 4) null comment '龙虎榜成交额',
    total_turnover           decimal(20, 4) null comment '市场总成交额',
    nbtt                     decimal(20, 4) null comment '净买额占总成交比',
    ttt                      decimal(20, 4) null comment '成交额占总成交比',
    lhb_num_5d               int default 0  null comment '最近5天_龙虎榜上榜次数',
    lhb_num_10d              int default 0  null comment '最近10天_龙虎榜上榜次数',
    lhb_num_30d              int default 0  null comment '最近30天_龙虎榜上榜次数',
    lhb_num_60d              int default 0  null comment '最近60天_龙虎榜上榜次数',
    ma_5d                    decimal(20, 4) null comment '5日均线',
    ma_10d                   decimal(20, 4) null comment '10日均线',
    ma_20d                   decimal(20, 4) null comment '20日均线',
    ma_30d                   decimal(20, 4) null comment '30日均线',
    ma_60d                   decimal(20, 4) null comment '60日均线',
    holding_yield_2d         decimal(20, 2) null comment '持股2日后收益率',
    holding_yield_5d         decimal(20, 2) null comment '持股5日后收益率',
    is_lhb                   int default 0  null comment '是否龙虎榜 1是 0否',
    is_lhb_60d               int default 0  null comment '是否60天龙虎榜 1是 0否',
    is_min_market_value      int default 0  null comment '是否小市值 1是 0否',
    is_rise_volume_2d        int default 0  null comment '是否连续两天放量- 1是 0否',
    is_rise_volume_2d_low    int default 0  null comment '连续两天放量且低收- 1是 0否',
    is_rise_ma_5d            int default 0  null comment '是否上穿5日均线 1是 0否',
    is_rise_ma_10d           int default 0  null comment '是否上穿10日均线 1是 0否',
    is_rise_ma_20d           int default 0  null comment '是否上穿20日均线 1是 0否',
    is_rise_ma_30d           int default 0  null comment '是否上穿30日均线 1是 0否',
    is_rise_ma_60d           int default 0  null comment '是否上穿60日均线 1是 0否',
    primary key (trade_date, stock_code),
    index index_stock_code (stock_code),
    index index_total_market_value (total_market_value)
);



-- TODO =====================  增量  =====================



























