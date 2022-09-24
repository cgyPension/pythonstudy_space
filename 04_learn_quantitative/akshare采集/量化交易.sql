create database stock;
use stock;

#TODO =========================================================  DDL  =====================================================================
drop table if exists ods_dc_stock_quotes_di;

create table ods_dc_stock_quotes_di
(
    交易日期 varchar(26) null,
    股票代码 varchar(26) null,
    股票名称 varchar(26) null,
    开盘价  decimal(20, 4) null,
    收盘价  decimal(20, 4) null,
    最高价  decimal(20, 4) null,
    最低价  decimal(20, 4) null,
    成交量  bigint null,
    成交额  decimal(20, 4) null,
    振幅   decimal(20, 4) null,
    涨跌幅  decimal(20, 4) null,
    涨跌额  decimal(20, 4) null,
    换手率  decimal(20, 4) null,
    primary key (交易日期, 股票代码)
) commit '东财沪深京A股行情表';

create table ods_163_stock_quotes_di
(
    交易日期 varchar(26) null,
    股票代码 varchar(26) null,
    股票名称 varchar(26) null,
    前收盘价  decimal(20, 4) null,
    开盘价  decimal(20, 4) null,
    收盘价  decimal(20, 4) null,
    最高价  decimal(20, 4) null,
    最低价  decimal(20, 4) null,
    成交量  bigint null,
    成交额  decimal(20, 4) null,
    涨跌幅  decimal(20, 4) null,
    涨跌额  decimal(20, 4) null,
    换手率  decimal(20, 4) null,
    总市值  decimal(20, 4) null,
    流通市值  decimal(20, 4) null,
    primary key (交易日期, 股票代码)
) commit '网易财经沪深A股行情表';

create table ods_dc_stock_tfp_di
(
    日期 varchar(26) null,
    股票代码 varchar(26) null,
    股票名称 varchar(26) null,
    停牌时间  varchar(26) null,
    停牌截止时间  varchar(26) null,
    停牌期限  varchar(26) null,
    停牌原因  varchar(26) null,
    所属市场  varchar(26) null,
    预计复牌时间  varchar(26) null,
    primary key (日期, 股票代码)
) commit '东方财富网-数据中心-特色数据-两市停复牌表';
