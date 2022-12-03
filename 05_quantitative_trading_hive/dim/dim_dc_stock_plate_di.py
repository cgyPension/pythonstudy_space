import os
import sys
import time
import warnings
import pandas as pd
from datetime import date, datetime
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

        sql = '''
              with t2 as (
                  select trade_date,
                         stock_code,
                         stock_name,
                         concat_ws(',',collect_list(concept_plate)) as concept_plates
                  from stock.ods_dc_stock_concept_plate_di
                  where td between '%s' and '%s'
                  group by trade_date,stock_code,stock_name
              )
              select nvl(t1.trade_date,t2.trade_date) as trade_date,
                     nvl(t1.stock_code,t2.stock_code) as stock_code,
                     nvl(t1.stock_name,t2.stock_name) as stock_name,
                     t1.industry_plate,
                     t2.concept_plates,
                     current_timestamp() as update_time,
                     nvl(t1.trade_date,t2.trade_date) as td
              from stock.ods_dc_stock_industry_plate_di t1
              full join t2
              on t1.trade_date = t2.trade_date
              and t1.stock_code = t2.stock_code
              where t1.td between '%s' and '%s';
        '''% (start_date,end_date,start_date,end_date)

        spark_df = spark.sql(sql)
        spark_df.repartition(1).write.insertInto('stock.dim_dc_stock_plate_di', overwrite=True)
        print('{}：执行完毕！！！'.format(appName))
    except Exception as e:
        print(e)

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