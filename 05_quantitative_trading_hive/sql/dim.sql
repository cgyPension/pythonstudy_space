create database stock;
use stock;

-- TODO =========================================================  xx  =====================================================================
drop table if exists dim_dc_stock_plate_df;
create table if not exists dim_dc_stock_plate_df
(
    stock_code     string comment '股票代码',
    stock_name     string comment '股票名称',
    industry_plate string comment '行业板块',
    concept_plates string comment '概念板块 ,拼接',
    update_time    timestamp comment '更新时间'
) comment ' 东方财富-板块维表'
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');



















