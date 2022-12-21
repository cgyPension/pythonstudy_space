import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import get_spark
from util.myUdfUtils import registerUDF


import pandas as pd


# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

'''
集群模式
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512M \
--num-executors 2 \
--executor-memory 512M \
--executor-cores 2 \
--archives hdfs://mycluster/pyspark/pyspark_env.zip#PyEnv \
--conf spark.dynamicAllocation.enabled=false \
/opt/code/05_quantitative_trading_hive/bak/test_spark.py



spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512M \
--num-executors 2 \
--executor-memory 512M \
--executor-cores 2 \
--archives hdfs://mycluster/pyspark/pyspark_env.zip#PyEnv \
--conf spark.dynamicAllocation.enabled=false \
--conf "spark.pyspark.python=/opt/module/anaconda3/envs/pyspark_env/bin/python3" \
--conf "spark.pyspark.driver.python=/opt/module/anaconda3/envs/pyspark_env/bin/python3" \
/opt/code/05_quantitative_trading_hive/bak/test_spark.py

spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512M \
--num-executors 2 \
--executor-memory 512M \
--executor-cores 2 \
--archives hdfs://mycluster/pyspark/pyspark_env.zip#PyEnv \
--conf spark.dynamicAllocation.enabled=false \
--conf "spark.pyspark.python=/opt/module/anaconda3/envs/pyspark_env/bin/python" \
--conf "spark.pyspark.driver.python=/opt/module/anaconda3/envs/pyspark_env/bin/python" \
/opt/code/05_quantitative_trading_hive/bak/test_spark.py


/home/cgy/.local/lib/python3.9/
'''

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/test_spark.py
# nohup python /opt/code/05_quantitative_trading_hive/bak/test_spark.py >> my.log 2>&1 &
# 本地模式
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/bak/test_spark.py
# spark-submit test_spark.py
# spark-submit --conf spark.sql.catalogImplementation=hive test_spark.py
# os.environ["SPARK_HOME"]="/opt/module/spark-3.2.0"
# os.environ["PYSPARK_PYTHON"]="/opt/module/anaconda3/envs/pyspark_env/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/opt/module/anaconda3/envs/pyspark_env/bin/python"
# 这个要用远程调试执行 不然本地读不了linux
if __name__ == '__main__':
    appName= os.path.basename(__file__)
    # 本地模式
    spark = get_spark(appName)


    # todo 有些sql只能hive执行 sql不支持 不能加;号
    # engine = hiveUtil().engine
    # hive_sql="""alter table stock.ods_dc_stock_quotes_di drop if exists partition (td >= '2022-11-07',td <='2022-11-11')"""
    # engine.execute(hive_sql)

    registerUDF(spark)
    spark.sql("""select * from test.student""").show()
    # spark.sql("""
    #     select trade_date,
    #        stock_code,
    #        stock_name,
    #        close_price,
    #        turnover_rate,
    #        cj_func(close_price,turnover_rate) as cj
    # from stock.ods_dc_stock_quotes_di
    # where td >= '2022-12-01'
    #     """).show()

#     spark.sql("""
#     select trade_date,
#        stock_code,
#        stock_name,
#        close_price,
#        turnover_rate,
#        cj_func(close_price,turnover_rate) as cj,
#        ta_rsi(close_price,6) as ta,
#        mt_rsi(close_price,6) as mt
# from stock.ods_dc_stock_quotes_di
# where td >= '2022-12-01'
#     """).show()

    # spark.sql("""
    #     select trade_date,
    #        stock_code,
    #        stock_name,
    #        close_price,
    #        turnover_rate,
    #        cj_func(close_price,turnover_rate) as cj,
    #        ta_rsi(close_price,6)over(partition by stock_code order by trade_date asc) as ta,
    #        mt_rsi(close_price,6)over(partition by stock_code order by trade_date asc) as mt
    # from stock.ods_dc_stock_quotes_di
    # where td >= '2022-12-01'
    #     """).show()

    print('{}：执行完毕！！！'.format(appName))
    # 新浪财经的股票交易日历数据
    # tmp_df = ak.tool_trade_date_hist_sina()
    # df = spark.createDataFrame(tmp_df)
    # df.write.mode('append') \
    #     .saveAsTable("test.insert_table")  # 如果首次执行不存在这个表，会自动创建分区表，不指定分区即创建不带分区的表

    # df.repartition(5).registerTempTable('temp_table')
    #df.createOrReplaceTempView("tmpv")

    # 静态分区  分区字段逗号分割，跟建表顺序一致
    # df.write.mode('append') \
    #     .partitionBy("channel", "event_day", "event_hour")\
    #     .saveAsTable("table_name")  # 如果首次执行不存在这个表，会自动创建分区表，不指定分区即创建不带分区的表

    # 可动态分区设置
    # spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")
    # df 按照字段+分区的顺序对应hive顺序
    # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
    # df.repartition(5).write.insertInto("table_name")  # 如果执行不存在这个表，会报错

    spark.stop()
    # print(df)
    # df.collect()