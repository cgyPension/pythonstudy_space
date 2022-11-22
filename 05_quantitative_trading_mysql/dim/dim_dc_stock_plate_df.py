import multiprocessing
import os
import sys
import time
import warnings
from datetime import date

import akshare as ak
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.DBUtils import sqlalchemyUtil
from util.CommonUtils import get_process_num, get_code_group, get_code_list, get_concept_plate_group

def get_data(engine):

    try:
        engine.execute(
            '''truncate table dim_dc_stock_plate_df;'''
        )
        # 写入mysql append replace

        # mysql目前不支持full join 只能用两个join代替
        sql = '''
                    with t2 as (
                        select stock_code,
                               stock_name,
                               group_concat(concept_plate) as concept_plates
                        from ods_dc_stock_concept_plate_df
                        group by stock_code,stock_name
                    )
                    select t1.stock_code,
                           t1.stock_name,
                           t1.industry_plate,
                           t2.concept_plates
                    from ods_dc_stock_industry_plate_df t1
                    left join t2
                    on t1.stock_code = t2.stock_code
                    union all
                    select t2.stock_code,
                           t2.stock_name,
                           t1.industry_plate,
                           t2.concept_plates
                    from ods_dc_stock_industry_plate_df t1
                    right join t2
                    on t1.stock_code = t2.stock_code
                    where t1.stock_code is null;
        '''
        df = pd.read_sql(sql, engine)

        df.to_sql('dim_dc_stock_plate_df', engine, if_exists='append',chunksize=100000, index=None)

        print('dim_dc_stock_plate_df：执行完毕！！！')
    except Exception as e:
        print(e)

# nohup dim_dc_stock_plate_df.py >> my.log 2>&1 &
# python dim_dc_stock_plate_df.py
if __name__ == '__main__':
    engine = sqlalchemyUtil().engine
    process_num = get_process_num()
    start_time = time.time()
    get_data(engine)
    engine.dispose()
    end_time = time.time()
    print('程序运行时间：{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))