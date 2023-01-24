select count(1) from dwd_stock_quotes_di where ma_60d is null;

select corr(volume_ratio_1d,holding_yield_2d) as volume_ratio_1d,
       corr(volume_ratio_5d,holding_yield_2d) as volume_ratio_5d,
       corr(sub_factor_score,holding_yield_2d) as sub_factor_score,
       corr(total_market_value,holding_yield_2d) as total_market_value,
       corr(z_total_market_value,holding_yield_2d) as z_total_market_value,
       corr(turnover_rate,holding_yield_2d) as turnover_rate,
       corr(pe_ttm,holding_yield_2d) as pe_ttm
from dwd_stock_quotes_di;

-- 两字段


with t1 as (
select 'volume_ratio_1d' as factor_name,corr(volume_ratio_1d,holding_yield_2d) as ic from dwd_stock_quotes_di
union all
select 'volume_ratio_5d' as factor_name,corr(volume_ratio_5d,holding_yield_2d) as ic from dwd_stock_quotes_di
union all
select 'sub_factor_score' as factor_name,corr(sub_factor_score,holding_yield_2d) as ic from dwd_stock_quotes_di
union all
select 'total_market_value' as factor_name,corr(total_market_value,holding_yield_2d) as ic from dwd_stock_quotes_di
union all
select 'z_total_market_value' as factor_name,corr(z_total_market_value,holding_yield_2d) as ic from dwd_stock_quotes_di
union all
select 'turnover_rate' as factor_name,corr(turnover_rate,holding_yield_2d) as ic from dwd_stock_quotes_di
union all
select 'pe_ttm' as factor_name,corr(pe_ttm,holding_yield_2d) as ic from dwd_stock_quotes_di
)
select * from t1 order by abs(ic) desc;

-- factor_name  ic
-- turnover_rate|-0.008097658793228917
-- total_market_value|-0.006506003139837986
-- z_total_market_value|-0.006506002831917188
-- pe_ttm|0.0005942856536266637
-- volume_ratio_1d|0.004187670561014614
-- volume_ratio_5d|0.006935735983189232
-- sub_factor_score|0.021075842876993035

