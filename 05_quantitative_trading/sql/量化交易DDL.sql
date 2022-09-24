create database stock;
use stock;

-- TODO =========================================================  DDL  =====================================================================
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
    volume  bigint null comment '成交量',
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

--股票加个标签拼接s字段
create table dim_stock_label
(
    stock_label_id    int auto_incremen comment '股票标签id' primary key,
    stock_label_name    varchar(26) null comment '股票标签名称',
    business_caliber   varchar(226) null comment '业务口径（计算逻辑、文字指标口径）',
    technical_caliber        varchar(226) null comment '技术口径',
    label_type int comment '标签类型：1规则标签; 2统计标签; 3挖掘标签',
    stock_label_description varchar(226) null comment '标签描述',
    create_time       datetime(3) default CURRENT_TIMESTAMP (3) comment '创建时间',
    update_time       datetime(3) on update current_timestamp (3) comment '更新时间'
) comment '股票标签';
insert into dim_stock_label (stock_label_name,business_caliber,label_type,stock_label_description)
VALUES ('小市值','总市值<=50亿',1,'小市值官网为20~30亿'),
       ('上穿5日均线','最高价>N日移动平均线=N日收盘价之和/N',2,''),
       ('上穿10日均线','最高价>N日移动平均线=N日收盘价之和/N',2,''),
       ('上穿20日均线','最高价>N日移动平均线=N日收盘价之和/N',2,''),
       ('上穿30日均线','最高价>N日移动平均线=N日收盘价之和/N',2,''),
       ('上穿60日均线','最高价>N日移动平均线=N日收盘价之和/N',2,''),
       ('超卖','',''),
       ('cr金叉','',''),
       ('首板涨停','',''),
       ('高量柱','',''),
       ('消费股','',''),
       ('cci买入信号','',''),
       ('rsi金叉','',''),
       ('低价股','',''),
       ('平台突破','',''),
       ('业绩预增','',''),
       ('股权集中','',''),
       ('机构重仓','',''),
       ('持续放量','',''),
       ('放巨量','',''),
       ('dpo买入信号','',''),
       ('价升量涨','',''),
       ('看涨吞没','',''),
       ('保险持股','','');

-- 标签组成策略
-- 回测时间 回测的一些指标子段输入到dws（股票角度） ads dim（策略角度）
create table dim_stock_strategy
(
    stock_strategy_id    int auto_incremen comment '股票策略id' primary key,
    stock_strategy_name varchar(226) null comment '股票策略名称 股票标签名称 +拼接',
    stock_label_ids    varchar(26) auto_incremen comment '股票标签id ,拼接',
    holding_period   int default 0 null comment '推荐持股周期：5日、30日',
    holding_yield_td decimal(20, 4) null comment '截止当天持股收益率',
    holding_yield_before decimal(20, 4) null comment '上期持股收益率',
    strategy_type  int default 0 comment '策略类型：0选股策略;1择时策略',
    backtest_yield  decimal(20, 4) null comment '回测收益率',
    max_retrace  decimal(20, 4) null comment '最大回撤',
    backtest_time   datetime(3) null comment '回测时间',
    create_time    datetime(3) default CURRENT_TIMESTAMP (3) comment '创建时间',
    update_time    datetime(3) on update current_timestamp (3) comment '更新时间'
) comment '股票策略';

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

create table dwd_stock_quotes_di
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

    total_market_value  decimal(20, 4) null comment '总市值',
    circulating_market_value  decimal(20, 4) null comment '流通市值',
    industry_sector   varchar(26) null comment '行业板块',

    ma_5d decimal(20, 4) null comment '5日均线',
    ma_10d decimal(20, 4) null comment '10日均线',
    ma_20d decimal(20, 4) null comment '20日均线',
    ma_30d decimal(20, 4) null comment '30日均线',
    ma_60d decimal(20, 4) null comment '60日均线',

    stock_label_ids   varchar(26) null comment '股票标签id ,拼接',
    stock_label_names   varchar(226) null comment '股票标签名称 ,拼接',
    stock_label_num   int default 0 comment '股票标签数量',

    holding_yield_5d  decimal(20, 2) null comment '持股5日后收益率',
    holding_yield_10d  decimal(20, 2) null comment '持股10日后收益率',

    suspension_time  varchar(26) null comment '停牌时间',
    suspension_deadline  varchar(26) null comment '停牌截止时间',
    suspension_period  varchar(26) null comment '停牌期限',
    suspension_reason  varchar(26) null comment '停牌原因',
    belongs_market  varchar(26) null comment '所属市场',
    estimated_resumption_time  varchar(26) null comment '预计复牌时间',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (trade_date, stock_code)
) comment '沪深A股行情表';
--持股收益 用开盘价 收盘价



-- 排除停牌数据
create table dws_stock_candidate_di
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

    total_market_value  decimal(20, 4) null comment '总市值',
    circulating_market_value  decimal(20, 4) null comment '流通市值',
    industry_sector   varchar(26) null comment '行业板块',

    ma_5d decimal(20, 4) null comment '5日均线',
    ma_10d decimal(20, 4) null comment '10日均线',
    ma_20d decimal(20, 4) null comment '20日均线',
    ma_30d decimal(20, 4) null comment '30日均线',
    ma_60d decimal(20, 4) null comment '60日均线',

    stock_label_ids   varchar(26) null comment '股票标签id ,拼接',
    stock_label_names   varchar(226) null comment '股票标签名称 ,拼接',
    stock_label_num   int default 0 comment '股票标签数量',
    stock_strategy_name varchar(226) null comment '股票策略名称 股票标签名称 +拼接',
    scores decimal(20, 4) null comment '分数',
    stock_strategy_ranking varchar(26) null comment '策略内排行rank',

    holding_yield_5d  decimal(20, 2) null comment '持股5日后收益率',
    holding_yield_10d  decimal(20, 2) null comment '持股10后日收益率',

    backtest_yield  decimal(20, 4) null comment '回测收益率',
    max_retrace  decimal(20, 4) null comment '最大回撤',
    backtest_time   datetime(3) null comment '回测时间',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',

    primary key (trade_date, stock_code)
) comment '股票候选 （>=80分 或top10）';

create table ads_stock_suggest_di
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

    total_market_value  decimal(20, 4) null comment '总市值',
    circulating_market_value  decimal(20, 4) null comment '流通市值',
    industry_sector   varchar(26) null comment '行业板块',

    ma_5d decimal(20, 4) null comment '5日均线',
    ma_10d decimal(20, 4) null comment '10日均线',
    ma_20d decimal(20, 4) null comment '20日均线',
    ma_30d decimal(20, 4) null comment '30日均线',
    ma_60d decimal(20, 4) null comment '60日均线',

    stock_label_ids   varchar(26) null comment '股票标签id ,拼接',
    stock_label_names   varchar(226) null comment '股票标签名称 ,拼接',
    stock_label_num   int default 0 comment '股票标签数量',
    stock_strategy_name varchar(226) null comment '股票策略名称 股票标签名称 +拼接',
    scores decimal(20, 4) null comment '分数',
    stock_strategy_ranking varchar(26) null comment '策略内排行rank',

    suggest_buy_price decimal(20, 2) null comment '推荐买价',
    suggest_stop_profit decimal(20, 2) null comment '推荐止盈',
    suggest_stop_loss decimal(20, 2) null comment '推荐止损',
    holding_yield_5d  decimal(20, 2) null comment '持股5日收益率',
    holding_yield_10d  decimal(20, 2) null comment '持股10日收益率',
    is_monitor int default 0 comment '实时监测 0否 1是',

    backtest_yield  decimal(20, 4) null comment '回测收益率',
    max_retrace  decimal(20, 4) null comment '最大回撤',
    backtest_time   datetime(3) null comment '回测时间',
    create_time datetime(3) default current_timestamp(3) comment '创建时间',
    update_time datetime(3) on update current_timestamp (3) comment '更新时间',
    primary key (create_time, stock_code)
) comment '股票推荐 （top3）';



--同花顺接口个人资产情况
-- ods


-- TODO =========================================================  主要DML  =====================================================================
truncate table dwd_stock_quotes_di;
insert into dwd_stock_quotes_di (trade_date, stock_code, stock_name, open_price, close_price, high_price, low_price,
                                 volume, turnover, amplitude, change_percent, change_amount, turnover_rate,
                                 total_market_value, circulating_market_value, industry_sector, ma_5d, ma_10d, ma_20d,
                                 ma_30d, ma_60d, stock_label_ids, stock_label_names, stock_label_num, holding_yield_5d,
                                 holding_yield_10d, suspension_time, suspension_deadline, suspension_period,
                                 suspension_reason, belongs_market, estimated_resumption_time)
with tmp_01 as (
select t1.trade_date,
       t1.stock_code,
       t1.stock_name,
       t1.open_price,
       t1.close_price,
       t1.high_price,
       t1.low_price,
       t1.volume,
       t1.turnover,
       t1.amplitude,
       t1.change_percent,
       t1.change_amount,
       t1.turnover_rate,
       t2.total_market_value,
       t2.circulating_market_value,
       null as industry_sector,#行业板块
       # 如果不够5日数据 则为空
       if(lag(t1.close_price,4,null)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code) is null,null,avg(t1.close_price)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code rows between 4 preceding and current row)) as ma_5d,
       if(lag(t1.close_price,9,null)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code) is null,null,avg(t1.close_price)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code rows between 9 preceding and current row)) as ma_10d,
       if(lag(t1.close_price,19,null)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code) is null,null,avg(t1.close_price)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code rows between 19 preceding and current row)) as ma_20d,
       if(lag(t1.close_price,29,null)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code) is null,null,avg(t1.close_price)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code rows between 29 preceding and current row)) as ma_30d,
       if(lag(t1.close_price,59,null)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code) is null,null,avg(t1.close_price)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code rows between 59 preceding and current row)) as ma_60d,
       if(lead(t1.close_price,4,null)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code) is null,null,(t1.open_price-lead(t1.close_price,4,null)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code))/t1.open_price) as holding_yield_5d,
       if(lead(t1.close_price,9,null)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code) is null,null,(t1.open_price-lead(t1.close_price,9,null)over(partition by t1.trade_date,t1.stock_code order by t1.trade_date,t1.stock_code))/t1.open_price) as holding_yield_10d,
       t3.suspension_time,
       t3.suspension_deadline,
       t3.suspension_period,
       t3.suspension_reason,
       t3.belongs_market,
       t3.estimated_resumption_time
from ods_dc_stock_quotes_di t1
left join ods_163_stock_quotes_di t2
        on t1.trade_date = t2.trade_date
            and t1.stock_code = t2.stock_code
left join ods_dc_stock_tfp_di t3
        on t1.trade_date = t3.trade_date
            and t1.stock_code = t3.stock_code
)
select trade_date,
       stock_code,
       stock_name,
       open_price,
       close_price,
       high_price,
       low_price,
       volume,
       turnover,
       amplitude,
       change_percent,
       change_amount,
       turnover_rate,
       total_market_value,
       circulating_market_value,
       industry_sector,
       ma_5d,
       ma_10d,
       ma_20d,
       ma_30d,
       ma_60d,
       concat_ws(',',if(total_market_value<=5000000000,1,null),
                     if(high_price>ma_5d,2,null),
                     if(high_price>ma_10d,3,null),
                     if(high_price>ma_20d,4,null),
                     if(high_price>ma_30d,5,null),
                     if(high_price>ma_60d,6,null)
       ) as stock_label_ids,
       concat_ws(',',if(total_market_value<=5000000000,'小市值',null),
                     if(high_price>ma_5d,'上穿5日均线',null),
                     if(high_price>ma_10d,'上穿10日均线',null),
                     if(high_price>ma_20d,'上穿20日均线',null),
                     if(high_price>ma_30d,'上穿30日均线',null),
                     if(high_price>ma_60d,'上穿60日均线',null)
       ) as stock_label_names,
       (
           if(total_market_value<=5000000000,1,0)+
           if(high_price>ma_5d,1,0)+
           if(high_price>ma_10d,1,0)+
           if(high_price>ma_20d,1,0)+
           if(high_price>ma_30d,1,0)+
           if(high_price>ma_60d,1,0)
        ) as stock_label_num,
       holding_yield_5d,
       holding_yield_10d,
       suspension_time,
       suspension_deadline,
       suspension_period,
       suspension_reason,
       belongs_market,
       estimated_resumption_time
from tmp_01;



-- TODO =====================  增量  =====================





-- TODO =========================================================  bak  =====================================================================
create table ods_dc_stock_code_list_df
(
    stock_code varchar(26) not null comment '股票代码' primary key,
    stock_name varchar(26) null comment '股票名称',
    create_time datetime(3) default current_timestamp(3) comment '创建时间'
) comment '东财沪深A股实时股票代码表';























