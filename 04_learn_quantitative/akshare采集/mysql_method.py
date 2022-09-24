

import pymysql
class learn_mysql(object):
    def __init__(self,host='hadoop102',user='root',password='12345',port=3306,db='stock',charset='utf8'):
        self.db=pymysql.connect(host=host,user=user,password=password,port=port,db=db,charset=charset)
        self.cursor=self.db.cursor()

    #链接数据库
    def concnet_mysql(self):
        '''
        链接数据库
        '''
        print('连接主机',self.db.get_host_info())
        self.cursor=self.db.cursor()
