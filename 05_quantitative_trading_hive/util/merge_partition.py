import os
import sys
import time
import warnings
import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.DBUtils import hiveUtil

warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

def merge_partition(tableName):
    """多进程才需要"""
    appName = os.path.basename(__file__)
    engine = hiveUtil().engine

    hive_sql="""show partitions %s""" % (tableName)
    pd_df = pd.read_sql(hive_sql, engine)

    # pt = spark.sql("""show partitions %s;""" % (tableName))
    for i in pd_df.partition:
        # py截取字符串 合并分区那里i
        engine.execute("""alter table %s partition (td ='%s') concatenate""" % (tableName,i.split('=')[1]))
    print('{}：执行完毕！！！'.format(appName))


# python /opt/code/05_quantitative_trading_hive/util/merge_partition.py stock.ods_financial_analysis_indicator_di
# spark-submit /opt/code/05_quantitative_trading_hive/util/merge_partition.py stock.ods_dc_stock_quotes_di
if __name__ == '__main__':
    tableName = ''
    if len(sys.argv) == 1:
        print("请携带一个参数 数据库.表名")
    else:
        tableName = sys.argv[1]

    start_time = time.time()
    merge_partition(tableName)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__), end_time - start_time, (end_time - start_time) / 60))
