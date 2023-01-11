import os
import sys
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from datetime import date, datetime
import datetime
import time
import pandas as pd
import akshare as ak
# 在linux会识别不了包 所以要加临时搜索目录
from util import bt_rank
from util.CommonUtils import get_spark
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/hc/sq_lb_hsl.py 20210101 20230103 2 3 7777
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/hc/sq_lb_hsl.py 20210101 20230103 5 3 8888
if __name__ == '__main__':
    if len(sys.argv) < 6:
        print("请携带所有参数")
    else:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        hold_day = int(sys.argv[3])
        hold_n = int(sys.argv[4])
        port = sys.argv[5]


    appName = os.path.basename(__file__)
    start_time = time.time()
    spark = get_spark(appName)
    start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
    # 为了向后补数据有值填充
    end_date_5 = pd.to_datetime(end_date + datetime.timedelta(5)).date()
    # 导入到bt 不能有str类型的字段
    # 排除主观因子
    sql = """
       with tmp_ads_01 as (
        select *
        from (
               select t1.*,t2.selection_reason,
                      if(t1.change_percent <0 and t1.volume_ratio_1d <1 and 
                         lag(t1.change_percent,1)over(partition by t1.stock_code order by t1.trade_date)<0 and 
                         lag(t1.volume_ratio_1d,1)over(partition by t1.stock_code order by t1.trade_date)<1,1,0) as is_flag
               from stock.dwd_stock_quotes_di t1
               left join stock.dwd_stock_strong_di t2
                    on t1.trade_date = t2.trade_date
                    and t1.stock_code = t2.stock_code
                    and t2.td between '%s' and '%s'
               where t1.td between '%s' and '%s'
               )
        where is_flag=1 and selection_reason is not null
       ),
       tmp_ads_02 as (
       --去除or 停复牌
       select *
       from tmp_ads_01
       where suspension_time is null
             or estimated_resumption_time < date_add(trade_date,1)
       ),
       tmp_ads_03 as (
                      select *,
                             dense_rank()over(partition by td order by volume_ratio_1d) as dr_volume_ratio_1d,
                             dense_rank()over(partition by td order by turnover_rate) as dr_turnover_rate
                      from tmp_ads_02
       ),
       tmp_ads_04 as (
                      select *,
                             '强势股+量比+换手率' as stock_strategy_name,
                             dense_rank()over(partition by td order by dr_volume_ratio_1d+dr_turnover_rate,dr_turnover_rate) as stock_strategy_ranking
                      from tmp_ads_03
       )
            select nvl(t1.trade_date,t2.trade_date) as trade_date,
                   nvl(t1.stock_code||'_'||t1.stock_name,t2.stock_code||'_'||t2.stock_name) as stock_code,
                   nvl(t1.open_price,t2.open_price) as open,
                   nvl(t1.close_price,t2.close_price) as close,
                   nvl(t1.high_price,t2.high_price) as high,
                   nvl(t1.low_price,t2.low_price) as low,
                   nvl(t1.volume,t2.volume) as volume,
                   nvl(t1.stock_strategy_ranking,9999) as stock_strategy_ranking
                   from tmp_ads_04 t1
                   full join stock.dwd_stock_quotes_di t2
                   on t1.trade_date = t2.trade_date
                        and t1.stock_code = t2.stock_code
                        and t2.td between '%s' and '%s'
                   order by stock_strategy_ranking
        """ % (start_date, end_date_5, start_date, end_date_5, start_date, end_date_5)

    # 读取数据
    spark_df = spark.sql(sql)
    pd_df = spark_df.toPandas()
    # 将trade_date设置成index
    pd_df = pd_df.set_index(pd.to_datetime(pd_df['trade_date'])).sort_index()
    print('{} 获取数据 运行完毕!!!'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    bt_rank.hc('强势股+量比+换手率',pd_df, start_date, end_date, end_date_5, hold_day, hold_n, port=port)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))