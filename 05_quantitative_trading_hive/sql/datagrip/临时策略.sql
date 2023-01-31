-- 先重新测试mm策略 量价齐跌 连续下跌
-- 策略：
-- 1.板块放量，板块rps排名靠前
-- 2. 板块个股放量，个股rps符合买入准则，能回踩10日均线的特别好
/*
select trade_date,
       stock_code,
       stock_name,
       industry_plate,
       concept_plates,
       rps_5d,
       rps_10d,
       rps_20d,
       rps_50d,
       rs,
       stock_label_names,
       stock_label_num,
       sub_factor_names,
       sub_factor_score,
       holding_yield_2d,
       holding_yield_5d,
       hot_rank,
       stock_strategy_ranking
from tmp_ads_05
where stock_strategy_ranking<=10
order by trade_date,stock_strategy_ranking;

where td >= '2021-12-01'
select sum(if(stock_strategy_ranking<=3,1,0)) from tmp_ads_05;
*/
