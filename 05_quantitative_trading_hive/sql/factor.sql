create database factor;
use factor;

-- TODO =========================================================  xx  =====================================================================
--T_ABS均值' : >2则 因子显著有效
--T_ABS>2(%)' : 因子显著性是否稳定
--IC_均值' : 因子整体趋势(spearman 同涨同跌不看幅度)
--IC_IR' : 因子是否稳健，越大越稳定
--IC>0.00(%)' : 是否为正向因子
--IC>0.02(%)' : 是否为可用因子(均值为负，则<-0.02)
--IC>0.05(%)' : 是否为强势因子(均值为负，则<-0.05)
drop table if exists factor;
create table if not exists factor
(
    factor_name string comment '因子名称',
    days        string comment '日期区间',
    pool        string comment 'xxx',
    start_date  date comment '开始日期',
    end_date    date comment '结束日期',
    IC          decimal(20, 6) comment 'IC',
    IR          decimal(20, 6) comment 'IR',
    IRR         decimal(20, 6) comment 'IRR',
    score       decimal(20, 6) comment '分数',
    ranking     int comment '排行 dense_rank',
    update_time timestamp comment '更新时间'
) comment '沪深A股行情表'
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');




















