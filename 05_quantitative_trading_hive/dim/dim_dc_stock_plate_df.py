import os
import sys
import time
import warnings
import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

from util.CommonUtils import get_process_num, get_spark


def get_data():
    try:
        appName = os.path.basename(__file__)
        # 本地模式
        spark = get_spark(appName)

        sql = '''
              with t2 as (
                  select stock_code,
                         stock_name,
                         concat_ws(',',collect_list(concept_plate)) as concept_plates
                  from stock.ods_dc_stock_concept_plate_df
                  group by stock_code,stock_name
              )
              select nvl(t1.stock_code,t2.stock_code) as stock_code,
                     nvl(t1.stock_name,t2.stock_name) as stock_name,
                     t1.industry_plate,
                     t2.concept_plates,
                     current_timestamp() as update_time
              from stock.ods_dc_stock_industry_plate_df t1
              full join t2
              on t1.stock_code = t2.stock_code;
        '''

        spark_df = spark.sql(sql)
        spark_df.repartition(1).write.insertInto('stock.dim_dc_stock_plate_df', overwrite=True)
        print('{}：执行完毕！！！'.format(appName))
    except Exception as e:
        print(e)

# spark-submit /opt/code/05_quantitative_trading_hive/ods/dim_dc_stock_plate_df.py all
# nohup dim_dc_stock_plate_df.py >> my.log 2>&1 &
# python dim_dc_stock_plate_df.py
if __name__ == '__main__':
    process_num = get_process_num()
    start_time = time.time()
    get_data()
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))