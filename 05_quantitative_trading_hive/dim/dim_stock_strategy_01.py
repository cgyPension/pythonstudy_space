#     sql = """
# with tmp_ads_01 as (
# select *,
#        dense_rank()over(partition by td order by total_market_value) as dr_tmv,
#        dense_rank()over(partition by td order by turnover_rate) as dr_turnover_rate,
#        dense_rank()over(partition by td order by pe_ttm) as dr_pe_ttm
# from stock.dwd_stock_quotes_di
# where td between '%s' and '%s'
#         -- 剔除京股
#         and substr(stock_code,1,2) != 'bj'
# --         -- 剔除涨停 涨幅<5
# --         and change_percent <5
# --         and turnover_rate between 1 and 30
# --         and stock_label_names rlike '小市值'
# ),
# tmp_ads_02 as (
#                select *,
#                       '小市值+换手率+市盈率TTM' as stock_strategy_name,
#                       dense_rank()over(partition by td order by dr_tmv+dr_turnover_rate+dr_pe_ttm) as stock_strategy_ranking
#                from tmp_ads_01
#                where suspension_time is null
#                        or estimated_resumption_time <= '%s'
# --                        or pe_ttm is null
# --                        or pe_ttm <=30
#                order by stock_strategy_ranking
# )
# select trade_date,
#        stock_code||'_'||stock_name as stock_code,
#        open_price as open,
#        close_price as close,
#        high_price as high,
#        low_price as low,
#        volume,
#        change_percent,
#        turnover_rate,
#        pe_ttm,
#        stock_strategy_ranking
# from tmp_ads_02
#     """ % (start_date,end_date)