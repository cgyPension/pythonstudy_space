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