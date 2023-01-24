       select trade_date,
              stock_code,
              stock_name,
              change_percent,
--                             if(change_percent<0 and
--                  lag(change_percent,1)over(partition by stock_code order by trade_date)<0 and
--                  ,1,0) as is_lxxd_3d
              sum(if(change_percent<0,1,0))over(partition by stock_code order by trade_date rows between 2 preceding and current row) as is_lxxd_3d
       from stock.dwd_stock_quotes_stand_di
       where td >= '2022-12-01'
               and stock_name not rlike 'ST'
               and (close_price/open_price-1)*100 between -3 and 3
               and ma_250d is not null
               and close_price>low_price_250d*1.3
               and close_price<high_price_250d*0.75
               and turnover_rate between 1 and 30;




select if((11>10 and 1>10) or (51>50 and 100>50),1,0);