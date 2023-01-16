import os
import sys
import time
import warnings
import pandas as pd
from datetime import date, datetime, timedelta
import akshare as ak
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

from util.CommonUtils import get_process_num, get_spark


def get_data(start_date, end_date):
    try:
        appName = os.path.basename(__file__)
        # 本地模式
        spark = get_spark(appName)
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        # 增量 因为rps_50d 要提前50个交易日
        s_date = '20210101'
        td_df = ak.tool_trade_date_hist_sina()
        daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < pd.to_datetime(start_date).date())]
        daterange_65 = daterange_df.iloc[-65:, 0].reset_index(drop=True)
        if daterange_65.empty:
            start_date_65 = pd.to_datetime(start_date).date()
        else:
            start_date_65 = pd.to_datetime(daterange_65[0]).date()

        # [0,100] 排序后 归一化
        # python 代码实现, value 是list
        # k = 100/(max(value)-min(value))
        # transform_value=[k*(x-min(value)) for x in value]
        # 这个公式和百分比排序 是一样的
        # 15位小数
        spark.sql(
            """
with t1 as (select trade_date,
       industry_plate,
       close_price,
       percent_rank()over(partition by trade_date order by change_percent desc)*100 as pr_industry_cp,
       if(lag(close_price,4)over(partition by industry_plate order by trade_date) is null,null,(close_price/lag(close_price,4)over(partition by industry_plate order by trade_date)-1)*100) as rps_5d,
       if(lag(close_price,9)over(partition by industry_plate order by trade_date) is null,null,(close_price/lag(close_price,9)over(partition by industry_plate order by trade_date)-1)*100) as rps_10d,
       if(lag(close_price,19)over(partition by industry_plate order by trade_date) is null,null,(close_price/lag(close_price,19)over(partition by industry_plate order by trade_date)-1)*100) as rps_20d,
       if(lag(close_price,49)over(partition by industry_plate order by trade_date) is null,null,(close_price/lag(close_price,49)over(partition by industry_plate order by trade_date)-1)*100) as rps_50d
from stock.ods_dc_stock_industry_plate_hist_di
where td between '%s' and '%s'),
t2 as (select trade_date,
       industry_plate,
       close_price,
       pr_industry_cp,
      percent_rank()over(partition by trade_date order by rps_5d asc)*100 as rps_5d,
      percent_rank()over(partition by trade_date order by rps_10d asc)*100 as rps_10d,
      percent_rank()over(partition by trade_date order by rps_20d asc)*100 as rps_20d,
      percent_rank()over(partition by trade_date order by rps_50d asc)*100 as rps_50d
from t1)
select * from t2
               """ % (start_date_65, end_date)
        ).createOrReplaceTempView('tmp_dim_industry_rps')

        spark.sql(
            """
with tmp_t1 as (select trade_date,
       concept_plate,
       close_price,
       if(lag(close_price,4)over(partition by concept_plate order by trade_date) is null,null,(close_price/lag(close_price,4)over(partition by concept_plate order by trade_date)-1)*100) as rps_5d,
       if(lag(close_price,9)over(partition by concept_plate order by trade_date) is null,null,(close_price/lag(close_price,9)over(partition by concept_plate order by trade_date)-1)*100) as rps_10d,
       if(lag(close_price,19)over(partition by concept_plate order by trade_date) is null,null,(close_price/lag(close_price,19)over(partition by concept_plate order by trade_date)-1)*100) as rps_20d,
       if(lag(close_price,49)over(partition by concept_plate order by trade_date) is null,null,(close_price/lag(close_price,49)over(partition by concept_plate order by trade_date)-1)*100) as rps_50d
from stock.ods_dc_stock_concept_plate_hist_di
where td between '%s' and '%s'),
tmp_t2 as (select trade_date,
       concept_plate,
       close_price,
      percent_rank()over(partition by trade_date order by rps_5d asc)*100 as rps_5d,
      percent_rank()over(partition by trade_date order by rps_10d asc)*100 as rps_10d,
      percent_rank()over(partition by trade_date order by rps_20d asc)*100 as rps_20d,
      percent_rank()over(partition by trade_date order by rps_50d asc)*100 as rps_50d
from tmp_t1)
select t1.trade_date,
       t1.stock_code,
       t1.stock_name,
       concat_ws(',',collect_list(t1.concept_plate)) as concept_plates,
       max(if((tmp_t2.rps_5d >=87 and tmp_t2.rps_10d >=87 and tmp_t2.rps_20d >=87) or
              (tmp_t2.rps_5d >=87 and tmp_t2.rps_10d >=87 and tmp_t2.rps_50d >=87) or
              (tmp_t2.rps_5d >=87 and tmp_t2.rps_20d >=87 and tmp_t2.rps_50d >=87) or
              (tmp_t2.rps_10d >=87 and tmp_t2.rps_20d >=87 and tmp_t2.rps_50d >=87),1,0)) as is_concept_rps
from stock.ods_dc_stock_concept_plate_cons_di t1
left join tmp_t2
    on t1.trade_date =tmp_t2.trade_date
        and t1.concept_plate =tmp_t2.concept_plate
where t1.td between '%s' and '%s'
group by t1.trade_date,t1.stock_code,t1.stock_name
               """ % (start_date_65, end_date,start_date, end_date)
        ).createOrReplaceTempView('tmp_dim_concept_rps')

        sql = '''
             with t1 as (
                        select t1.trade_date,
                               t1.stock_code,
                               t1.stock_name,
                               t1.industry_plate,
                               tmp_dim_industry_rps.pr_industry_cp,
                               tmp_dim_industry_rps.rps_5d,
                               tmp_dim_industry_rps.rps_10d,
                               tmp_dim_industry_rps.rps_20d,
                               tmp_dim_industry_rps.rps_50d
                        from stock.ods_dc_stock_industry_plate_cons_di t1
                        left join tmp_dim_industry_rps
                            on t1.trade_date = tmp_dim_industry_rps.trade_date
                                and t1.industry_plate = tmp_dim_industry_rps.industry_plate
                        where t1.td between '%s' and '%s'
             )
              select nvl(t1.trade_date,t2.trade_date) as trade_date,
                     nvl(t1.stock_code,t2.stock_code) as stock_code,
                     nvl(t1.stock_name,t2.stock_name) as stock_name,
                     t1.industry_plate,
                     t2.concept_plates,
                     t1.pr_industry_cp,
                     t1.rps_5d,
                     t1.rps_10d,
                     t1.rps_20d,
                     t1.rps_50d,
                     t2.is_concept_rps,
                     current_timestamp() as update_time,
                     nvl(t1.trade_date,t2.trade_date) as td
              from  t1
              full join tmp_dim_concept_rps t2
              on t1.trade_date = t2.trade_date
              and t1.stock_code = t2.stock_code;
        '''% (start_date,end_date)

        spark_df = spark.sql(sql)
        spark_df.repartition(1).write.insertInto('stock.dim_dc_stock_plate_di', overwrite=True)
        spark.stop
        print('{}：执行完毕！！！'.format(appName))
    except Exception as e:
        print(e)

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dim/dim_dc_stock_plate_di.py update 20210101 20221216
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/dim/dim_dc_stock_plate_di.py all
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/dim/dim_dc_stock_plate_di.py update
# nohup dim_dc_stock_plate_di.py >> my.log 2>&1 &
# python dim_dc_stock_plate_di.py
if __name__ == '__main__':
    start_date = date.today().strftime('%Y%m%d')
    end_date = start_date
    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        run_type = sys.argv[1]
        if run_type == 'all':
            start_date = '20210101'
            end_date = date.today().strftime('%Y%m%d')
        else:
            start_date = date.today().strftime('%Y%m%d')
            end_date = start_date
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]

    process_num = get_process_num()
    start_time = time.time()
    get_data(start_date, end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))