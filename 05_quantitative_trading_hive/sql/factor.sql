create database factor;
use factor;

-- TODO =========================================================  xx  =====================================================================
--歧义ic一般是指ic均值
--ic_mean(ic均值的绝对值)：>0.05好;>0.1很好;>0.15非常好;>0.2可能错误(未来函数);当ic均值>0是正向因子
--ic_ir(ir=ic均值/ic标准差)：>=0.5认为因子稳定获取超额收益能力较强;越大越好
--ic>0：ic>0的概率 没什么作用
--abs_ic>0.02：ic绝对值>0,02的比例
--t_abs：样本T检验，X对比0，如果t只在1，-1之间，说明X均值为0，假设成立;在这绝对值应该越大越好，t_abs<1：因子有效性差
--p：当p值小于0.05时，认为与0差异显著;在这越小越好
--skew：偏度 为正则是右偏，为负则是左偏，指正态左右偏峰谷在另一则
--kurtosis：峰度 峰度描述的是分布集中趋势高峰的形态，通常与标准正态分布相比较。
--         在归一化到同一方差时，若分布的形状比标准正态分布更瘦高，则称为尖峰分布，若分布的形状比标准正态分布更矮胖，则称为平峰分布。
--         当峰度系数为 0 则为标准正态分布，大于 0 为尖峰分布，小于 0 为平峰分布。
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




















