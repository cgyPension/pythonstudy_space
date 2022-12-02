import os
import sys
import time
import warnings
from datetime import date
import akshare as ak
import pandas as pd
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


# 太多任务一起执行 mysql不释放内存 要分开断开连接重连
def get_data(start_date, end_date):
    appName = os.path.basename(__file__)
    # 本地模式
    spark = get_spark(appName)
    spark_df = spark.sql("""
select *
from stock.ods_dc_stock_quotes_di
where substr(stock_code,1,2) != 'bj'
    """)
    # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
    spark_df.repartition(1).write.insertInto('stock.ods_dc_stock_quotes_di_new', overwrite=True)  # 如果执行不存在这个表，会报错

    print('{}：执行完毕！！！'.format(appName))

# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/tmp_ods.py all
# nohup tmp_ods.py update 20221101 >> my.log 2>&1 &
# python tmp_ods.py all
# python tmp_ods.py update 20210101 20211231
if __name__ == '__main__':
    # between and 两边都包含
    process_num = get_process_num()
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

    start_time = time.time()
    get_data(start_date, end_date)
    end_time = time.time()
    print('程序运行时间：耗时{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))

