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

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/hc_zzt.py update 20210101 20221201
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/hc_zzt.py update 20220101 20221201
# nohup python /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/hc_zzt.py update 20210101 20221201 >> my.log 2>&1 &
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/factor/hc_zzt.py update 20210101 20221201
if __name__ == '__main__':
    start_date = date.today().strftime('%Y%m%d')
    end_date = start_date
    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        run_type = sys.argv[1]
        if run_type == 'all':
            start_date = '20210101'
            end_date
        else:
            start_date
            end_date
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]

    # start_date = '20221101'
    # end_date = '20221118'

    appName = os.path.basename(__file__)
    start_time = time.time()
    spark = get_spark(appName)
    start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
    # 为了向后补数据有值填充
    end_date_5 = pd.to_datetime(end_date + datetime.timedelta(5)).date()
    # 导入到bt 不能有str类型的字段
    sql = """
       with tmp_ads_01 as (
       select *
       from stock.dwd_stock_quotes_di
       where td between '%s' and '%s'
               and stock_name not rlike 'ST'
               --rlike语句匹配正则表达式 like rlike会自动把null的数据去掉 要转换
               and nvl(concept_plates,'保留null') not rlike '次新股'
               and change_percent <5
               and turnover_rate between 1 and 30
               and stock_label_names rlike '小市值'
               and pe_ttm between 0 and 30
       ),
       tmp_ads_02 as (
       --去除or 停复牌
       select *
       from tmp_ads_01
       where suspension_time is null
       --      and pe_ttm is null
       --      or pe_ttm <=30
             or estimated_resumption_time < date_add('%s',1)
       ),
       --要剔除玩所有不要股票再排序 否则排名会变动
       tmp_ads_03 as (
       select *,
              dense_rank()over(partition by td order by total_market_value) as dr_tmv,
              dense_rank()over(partition by td order by turnover_rate) as dr_turnover_rate,
              dense_rank()over(partition by td order by pe_ttm,pe) as dr_pe_ttm,
              dense_rank()over(partition by td order by sub_factor_score desc) as dr_sub_factor_score
       from tmp_ads_02
       ),
       tmp_ads_04 as (
                      select *,
                             '小市值+市盈率TTM+换手率' as stock_strategy_name,
                             -- 加上量比排序 避免排名重复
                             -- dense_rank()over(partition by td order by dr_tmv+dr_turnover_rate+dr_pe_ttm,volume_ratio_1d) as stock_strategy_ranking
                             -- 权重 
                             dense_rank()over(partition by td order by dr_tmv+dr_turnover_rate+dr_pe_ttm+dr_sub_factor_score,volume_ratio_1d) as stock_strategy_ranking
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
        """ % (start_date, end_date_5, end_date_5, start_date, end_date_5)

    # 读取数据
    spark_df = spark.sql(sql)
    pd_df = spark_df.toPandas()
    # 将trade_date设置成index
    pd_df = pd_df.set_index(pd.to_datetime(pd_df['trade_date'])).sort_index()
    print('{} 获取数据 运行完毕!!!'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    bt_rank.hc(pd_df, start_date, end_date, end_date_n=end_date_5, strategy_name=appName, hold_day=2, hold_n=3, port='7777')
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))