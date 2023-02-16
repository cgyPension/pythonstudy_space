create database stock;
use stock;

-- TODO =========================================================  xx  =====================================================================
drop table if exists dim_stock_label;
create table if not exists dim_stock_label
(
--     stock_label_id          int comment '股票标签id',
    stock_label_name        string comment '股票标签名称 负面标签后缀用-',
    business_caliber        string comment '业务口径（计算逻辑、文字指标口径）',
--     label_type              int comment '标签类型：1规则标签; 2统计标签; 3挖掘标签',
    is_factor               int comment '是否因子 0否 1是',
    stock_label_description string comment '标签描述',
    update_time             timestamp comment '更新时间'
) comment '股票标签'
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress' = 'snappy');
insert into dim_stock_label VALUES
       ('小市值','总市值<33.3%排名',0,'小市值官网为20~30亿;用percent_rank() <33.3%排名',current_timestamp()),
       --均线的标签要不要删除？
       ('上穿5日均线','最高价>N日移动平均线=N日收盘价之和/N',0,null,current_timestamp()),
       ('上穿10日均线','最高价>N日移动平均线=N日收盘价之和/N',0,null,current_timestamp()),
       ('上穿20日均线','最高价>N日移动平均线=N日收盘价之和/N',0,null,current_timestamp()),
       ('上穿30日均线','最高价>N日移动平均线=N日收盘价之和/N',0,null,current_timestamp()),
       ('上穿60日均线','最高价>N日移动平均线=N日收盘价之和/N',0,null,current_timestamp()),

-- 龙虎榜 解读 要分买 卖 正负 主力做T(T+0)不当 龙虎榜是用买卖，还是用那个总的净买入额
       ('当天龙虎榜_买','东财龙虎榜买',1,null,current_timestamp()),
       ('当天龙虎榜_卖-','东财龙虎榜卖',1,null,current_timestamp()),
       ('最近60天龙虎榜','东财龙虎榜',0,null,current_timestamp()),
       ('预盈预增','概率板标签',1,null,current_timestamp()),
       ('预亏预减-','概率板标签',1,null,current_timestamp()),

       ('个股rps>=87','行业板块三线欧奈尔rps>=87',1,'(收盘价-N日前的收盘价)/N日前的收盘价  排序再归一化 即百分比排序',current_timestamp()),
       ('板块rps>=87','行业板块三线欧奈尔rps>=87',1,'(收盘价-N日前的收盘价)/N日前的收盘价  排序再归一化 即百分比排序',current_timestamp()),
       ('个股rps_10_20金叉','rps_10d>rps_20d and 前一日rps_10d<rps_20d',1,'(收盘价-N日前的收盘价)/N日前的收盘价  排序再归一化 即百分比排序',current_timestamp()),
       ('个股rps_10_20死叉-','rps_10d<rps_20d and 前一日rps_10d>rps_20d',1,'(收盘价-N日前的收盘价)/N日前的收盘价  排序再归一化 即百分比排序',current_timestamp()),

       ('rs>=0','欧奈尔RS',1,'',current_timestamp()),

       ('行业板块涨跌幅前10%%-','百分比排序 desc',1,null,current_timestamp()),
       ('连续N天量价齐升','连续N天量比>1 and 涨幅>0',0,null,current_timestamp()),
       ('连续N天量价齐跌','连续N天量比<1 and 涨幅<0',0,null,current_timestamp()),

       ('rsi_6d超卖','rsi_6d<=20 超卖区 则买入',1,'用的是ta 同花顺上的平滑rsi',current_timestamp()),
       ('rsi_6d超买-','rsi_6d>=80 超买区 则卖出',1,null,current_timestamp()),
       ('rsi_6_12金叉','rsi_6d>rsi_12d and 前一日rsi_6d<rsi_12d',1,null,current_timestamp()),
       ('rsi_6_12死叉-','rsi_6d<rsi_12d and 前一日rsi_6d>rsi_12d',1,null,current_timestamp()),


       ('adtm买入信号','',0,null,current_timestamp()),
       ('boll买入信号','',0,null,current_timestamp()),
       ('bbiboll买入信号','',0,null,current_timestamp()),
       ('dpo买入信号','',0,null,current_timestamp()),
       ('cci买入信号','',0,null,current_timestamp()),
       ('cr金叉','',0,null,current_timestamp()),
       ('塔形底','',0,null,current_timestamp()),
       ('业绩预增','',0,null,current_timestamp()),

       ('adtm卖出信号','',0,null,current_timestamp()),
       ('boll卖出信号','',0,null,current_timestamp()),
       ('bbiboll卖出信号','',0,null,current_timestamp()),
       ('dpo卖出信号','',0,null,current_timestamp()),
       ('cci卖出信号','',0,null,current_timestamp()),
       ('超卖','',0,null,current_timestamp()),
       ('首板涨停','',0,null,current_timestamp()),
       ('高量柱','',0,null,current_timestamp()),
       ('消费股','',0,null,current_timestamp()),
       ('低价股','',0,null,current_timestamp()),
       ('平台突破','',0,null,current_timestamp()),
       ('股权集中','',0,null,current_timestamp()),
       ('机构重仓','',0,null,current_timestamp()),
       ('持续放量','',0,null,current_timestamp()),
       ('放巨量','',0,null,current_timestamp()),
       ('价升量涨','',0,null,current_timestamp()),
       ('看涨吞没','',0,null,current_timestamp());

drop table if exists dim_stock_strategy;
create table if not exists dim_stock_strategy
(
--     stock_strategy_id    int comment '股票策略id',
    stock_strategy_name  string comment '股票策略名称 股票标签名称 +拼接',
--     holding_yield_td     decimal(20, 4) comment '截止当天持股收益率',
--     holding_yield_before decimal(20, 4) comment '上期持股收益率',
--     strategy_type        int comment '策略类型：0选股策略;1择时策略',
    annual_yield         decimal(20, 4) comment '年化收益率',
    max_retrace          decimal(20, 4) comment '最大回撤',
    annual_max_retrace   decimal(20, 4) comment '年化收益率/最大回撤',
    backtest_start_date  date comment '回测数据开始日期',
    backtest_end_date    date comment '回测数据结束日期',
    update_time          timestamp comment '更新时间'
) comment '股票策略'
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress' = 'snappy');

drop table if exists dim_plate_df;
create table if not exists dim_plate_df
(
    trade_date     date comment '交易日期',
    plate_code string comment '板块代码',
    plate_name string comment '板块名称',
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
    rps_5d    decimal(20, 15) comment '欧奈尔rps_5d',
    rps_10d    decimal(20, 15) comment '欧奈尔rps_10d',
    rps_15d    decimal(20, 15) comment '欧奈尔rps_15d',
    rps_20d    decimal(20, 15) comment '欧奈尔rps_20d',
    rps_50d    decimal(20, 15) comment '欧奈尔rps_50d',
    is_rps_red  int comment '20日内rps首次三线翻红 1:是',
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
    update_time                  timestamp comment '更新时间'
) comment '东方财富-沪深板块-行业概念板块汇总'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists dim_stock_fee_rate;
create table if not exists dim_stock_fee_rate
(
    commission_fee decimal(20, 4) comment '佣金费率',
    transfer_fee   decimal(20, 4) comment '过户费率',
    stamp_duty_fee decimal(20, 4) comment '印花税率 只有卖时收'
) comment '股票手续费率'
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress' = 'snappy');
insert into dim_stock_fee_rate values(0.0005,0.00002,0.001);

drop table if exists dim_dc_stock_plate_di;
create table if not exists dim_dc_stock_plate_di
(
    trade_date    date comment '交易日期',
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    industry_plate string comment '行业板块',
    concept_plates string comment '概念板块 ,拼接',
    is_plate_rps_red int comment '20日内板块rps首次三线翻红 1:是',
    update_time    timestamp comment '更新时间'
) comment ' 东方财富-成分股板块维表'
    partitioned by (td date comment '分区_交易日期')
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');









