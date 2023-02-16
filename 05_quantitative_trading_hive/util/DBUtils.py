import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from clickhouse_sqlalchemy import make_session
from util.CommonUtils import get_process_num, get_spark

class sqlalchemyUtil(object):
    def __init__(self):
        self.process_num= get_process_num()
        self.engine = create_engine('mysql+pymysql://root:123456@hadoop102:3306/stock?charset=utf8',
                                    pool_size=self.process_num * 2, max_overflow=self.process_num * 2, pool_timeout=50,
                                    pool_recycle=3600,pool_pre_ping=True)
        self.conn = self.engine.connect()
        self.session = sessionmaker(self.engine)
        self.txn=self.conn.begin()

    #链接数据库
    def mysqlConcnet(self):
        '''
        链接数据库
        '''
        print('连接主机',self.engine.dialect)
    def closeEngine(self):
        self.conn.close()
        self.engine.dispose()


class pymysqlUtil(object):
    def __init__(self,host='hadoop102',user='root',password='123456',port=3306,db='stock',charset='utf8'):
        self.process_num = get_process_num()
        self.db=pymysql.connect(host=host,user=user,password=password,port=port,db=db,charset=charset)
        self.cursor=self.db.cursor()

    #链接数据库
    def mysqlConcnet(self):
        '''
        链接数据库
        '''
        print('连接主机',self.db.get_host_info())

    def closeResource(self):
        self.curson.close()
        self.db.close()

# Liunx系统 window系统可能会有问题
class hiveUtil():
    '''sql末尾不能放;号'''
    def __init__(self):
        self.engine = create_engine('hive://cgy:123456@hadoop102:10000/stock?auth=CUSTOM')
        self.conn = self.engine.connect()

    def __enter__(self):
        return self.engine

    def __exit__(self):
        self.conn.close()
        self.engine.dispose()

class clickhouseUtil():
    '''sql末尾不能放;号'''
    def __init__(self):
        self.process_num = get_process_num()
        self.engine = create_engine('clickhouse://default:''@hadoop102:8123/stock?auth=CUSTOM',
                                    pool_size=self.process_num * 2, max_overflow=self.process_num * 2, pool_timeout=50,
                                    pool_recycle=3600, pool_pre_ping=True
                                    )
        self.session = make_session(self.engine)

    # def execute_query(self,sql):
    #     """查询"""
    #     self.cursor = self.session.execute(sql)
    #     try:
    #         fields = self.cursor._metadata.keys
    #         return pd.DataFrame([dict(zip(fields, item)) for item in self.cursor.fetchall()])
    #     except Exception as e:
    #         print(e)

    def execute(self,sql):
        try:
            self.cursor = self.session.execute(sql)
        except Exception as e:
            print(e)

    def execute_query(self, sql):
        return pd.read_sql(sql, self.engine)

    def execute_insert(self, tableName, df, if_exists='append'):
        # append追加  replace全量覆盖
        df.to_sql(name=tableName, con=self.engine, if_exists=if_exists, index=False, index_label=False, chunksize=10000)
        print('{}插入CK成功！！！'.format(tableName))

    def spark_insert_ck(self, tableName,spark_df,if_exists='append'):
        '''不弄了烦 Caused by: java.lang.ClassNotFoundException: com.clickhouse.client.logging.LoggerFactory'''
        properties = {'driver': 'ru.yandex.clickhouse.ClickHouseDriver',
                      "socket_timeout": "300000",
                      "rewriteBatchedStatements": "true",
                      "batchsize": "10000",
                      "numPartitions": "8",
                      'user': 'default',
                      'password': '',
                      'isolationLevel': 'NONE'}
        spark_df.write.jdbc(url='jdbc:clickhouse://default:''@hadoop102:8123/hive',table=tableName, mode=if_exists, properties=properties)
        # spark_df.write.jdbc(url='jdbc:clickhouse://{url}:8123/hive',table=tableName, mode=if_exists, properties=properties)

    def spark_read_ck(self, tableName,spark_df):
        properties = {'driver': 'ru.yandex.clickhouse.ClickHouseDriver',
                      "socket_timeout": "300000",
                      "rewriteBatchedStatements": "true",
                      "batchsize": "10000",
                      "numPartitions": "8",
                      'user': 'default',
                      'password': ''}
        spark_df.read.jdbc(url='jdbc:clickhouse://{url}:8123/hive',table=tableName, properties=properties)

    def __exit__(self):
        self.cursor.close()
        self.session.close()
        self.engine.dispose()

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/util/DBUtils.py
if __name__ == '__main__':
    # sql = 'SHOW TABLES'
    # sql = 'select * from test'
    appName = os.path.basename(__file__)
    spark = get_spark(appName)
    df = pd.DataFrame({"json": ['c', 'd']})
    print(df)
    # spark_df = spark.sql("""
    #                     select trade_date,
    #                            industry_plate_code as plate_code,
    #                            industry_plate as plate_name,
    #                            open_price
    #                     from stock.ods_dc_stock_industry_plate_hist_di
    #                     where td = '2023-02-07'
    #     """)
    spark_df = spark.createDataFrame(df)
    print(spark_df.show())
    properties = {'driver': 'ru.yandex.clickhouse.ClickHouseDriver',
                  "socket_timeout": "300000",
                  "rewriteBatchedStatements": "true",
                  "batchsize": "10000",
                  "numPartitions": "8",
                  'user': 'default',
                  'password': '',
                  'isolationLevel': 'NONE'}
    # spark_df.write.jdbc(url='jdbc:clickhouse://default:''@hadoop102:8123/hive', table='test', mode='append',properties=properties)
    spark_df.write.jdbc(url='jdbc:clickhouse://{url}:8123/hive', table='test', mode='append',properties=properties)
    clickhouseUtil().spark_insert_ck('test',spark_df)
    spark.stop()
    print('插入成功！！！')