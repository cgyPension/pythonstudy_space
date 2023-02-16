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
from util.CommonUtils import get_process_num, get_spark, get_trade_date_nd


def get_data(start_date, end_date):
    try:
        appName = os.path.basename(__file__)
        spark = get_spark(appName)
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        # [0,100] 排序后 归一化
        # python 代码实现, value 是list
        # k = 100/(max(value)-min(value))
        # transform_value=[k*(x-min(value)) for x in value]
        # 这个公式和百分比排序 是一样的
        # 15位小数

        spark_df = spark.sql(
            '''
            with t1 as (
                select a.trade_date,
                       a.stock_code,
                       a.stock_name,
                       a.industry_plate,
                       b.is_rps_red
                from stock.ods_dc_stock_industry_plate_cons_di a
                 left join stock.dim_plate_df b
                       on a.trade_date = b.trade_date
                           and a.industry_plate_code = b.plate_code
                where a.td between '%s' and '%s'
                  and b.td between '%s' and '%s'
            ),
            t2 as (select a.trade_date,
                          a.stock_code,
                          a.stock_name,
                          concat_ws(',', collect_list(a.concept_plate)) as concept_plates,
                          max(b.is_rps_red) as is_rps_red
                   from stock.ods_dc_stock_concept_plate_cons_di a
                   left join stock.dim_plate_df b
                         on a.trade_date = b.trade_date
                             and a.concept_plate_code = b.plate_code
                   where a.td between '%s' and '%s'
                     and b.td between '%s' and '%s'
                   group by a.trade_date, a.stock_code, a.stock_name
            )
            select nvl(t1.trade_date, t2.trade_date)      as trade_date,
                   nvl(t1.stock_code, t2.stock_code)      as stock_code,
                   nvl(t1.stock_name, t2.stock_name)      as stock_name,
                   t1.industry_plate,
                   t2.concept_plates,
                   greatest(t1.is_rps_red, t2.is_rps_red) as is_plate_rps_red,
                   current_timestamp()                    as update_time,
                   nvl(t1.trade_date, t2.trade_date)      as td
            from t1
            full join t2
                 on t1.trade_date = t2.trade_date
                     and t1.stock_code = t2.stock_code
                    ''' % (start_date, end_date,start_date, end_date,start_date, end_date,start_date, end_date)
        )
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