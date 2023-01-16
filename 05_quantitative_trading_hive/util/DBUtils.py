import os
import sys
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import get_process_num

process_num = get_process_num()
class sqlalchemyUtil(object):
    def __init__(self):
        self.process_num= process_num
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
        self.process_num = process_num
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