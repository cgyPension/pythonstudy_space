
import pandas as pd
import akshare as ak
import pymysql
class learn_mysql(object):
    def __init__(self,host='127.5.5.2',user='root',password='lxg',port=3306,db='gp_codes',charset='utf8'):
        '''
        host连接的主机
        user使用者一般是root
        password密码
        port默3306
        db链接的数据库，和表不一样
        chartset编码方式,utf8
        '''
        #self.host=host
        #self.user=user
        #self.password=password
        #self.port=port
        #self.db=db
        #self.charset=charset
        self.db=pymysql.connect(host=host,user=user,password=password,port=port,db=db,charset=charset)
        self.cursor=self.db.cursor()
    #链接数据库
    def concnet_mysql(self):
        '''
        链接数据库
        '''
        print('连接主机',self.db.get_host_info())
        self.cursor=self.db.cursor()
    #显示数据库
    def show_databases(self):
        '''
        显示数据库
        '''
        sql='show databases'
        data=self.cursor.execute(sql)
        for i in self.cursor.fetchall():
            print(i[0])
    #建立数据库
    def create_database(self,name='数据库'):
        '''
        建立数据库
        '''
        try:
            sql='create database {}'.format(name)
            self.cursor.execute(sql)
            self.db.commit()
            print('建立成功')
            sql_show='show databases'
            self.cursor.execute(sql_show)
            for i in self.cursor.fetchall():
                print(i[0])
        except:
            print('数据库已经存在')
            sql_show='show databases'
            self.cursor.execute(sql_show)
            for i in self.cursor.fetchall():
                print(i[0])
    #建立表
    def create_table(self,table_name='create2'):
        '''
        建立表
        '''
        try:
            create_sql='create table {}(id int(11))'.format(table_name)
            self.cursor.execute(create_sql)
            self.db.commit()
            print('建立成功')
            #显示表
            learn_mysql().show_table()
        except:
            print('{}表已经存在'.format(table_name))
    #删除数据库
    def drop_database(self,name='basedata_2'):
        '''
        删除数据库
        '''
        try:
            drop_sq='drop database {}'.format(name)
            self.cursor.execute(drop_sq)
            self.db.commit()
            print('删除成功{}'.format(name))
            for i in self.cursor.fetchall():
                print(i[0])
            self.db.close()
        except:
            print('数据库{}已经删除'.format(name))
            sql_show='show databases'
            self.cursor.execute(sql_show)
            self.db.commit()
            for i in self.cursor.fetchall():
                print(i[0])
            self.db.close()
    #显示数据表
    def show_table(self,database='gp_codes'):
        '''
        显示数据表
        database数据库
        '''
        #选择数据库
        try:
            change_database='use {}'.format(database)
            result=self.cursor.execute(change_database)
            if result==0:
                print('改变成功')
                show_tab='show tables'
                self.cursor.execute(show_tab)
                self.db.commit()
                for i in self.cursor.fetchall():
                    print(i[0])
                self.db.close()
            else:
                print('改变失败')
        except:
            print('已经改变')
            show_tab='show tables'
            self.cursor.execute(show_tab)
            self.db.commit()
            for i in self.cursor.fetchall():
                print(i[0])
            self.db.close()
    #显示数据表的内容
    def show_table_context(self,table_name='students'):
        #显示全部的表
        try:
            show_context='select * from {}'.format(table_name)
            self.cursor.execute(show_context)
            self.db.commit()
            colume=[]
            for i in self.cursor.description:
                colume.append(i[0])
            data=[]
            for m in self.cursor.fetchall():
                data.append(list(m))
            df=pd.DataFrame(data,columns=colume)
            print(df)
            return df
        except:
            print('数据表不存在')
    #插入数据到表单独插入
    def insert_data_to_table_on(self,table_name='stock_data',title='open',value=1):
        '''
        插入数据到表单独插入
        table_name表名
        title标题
        value值
        '''
        try:
            insert_sql='insert into {}({}) value({})'.format(table_name,title,value)
            self.cursor.execute(insert_sql)
            self.db.commit()
            print('插入成功')
            #查看结果
            learn_mysql().show_table_context(table_name=table_name)
            self.db.close()
        except:
            print('{}表不存在'.format(table_name))
    #一次性插入全部row
    def insert_data_to_table_all(self,table_name='stock_data',value=['2021-01-01','15','18','15','18','13']):
        '''
        一次性插入一列
        '''
        #查询表的标题，列明
        show_title='select * from {}'.format(table_name)
        self.cursor.execute(show_title)
        title=[]
        for i in self.cursor.description:
            title.append(i[0])
        titles=','.join(title)
        if len(title)==len(value):
            insert_all='insert into {}({}) value{}'.format(table_name,titles,tuple(value))
            print(insert_all)
            self.cursor.execute(insert_all)
            self.db.commit()
            self.db.close()
            print('数据插入成功')
            #显示结果
            learn_mysql().show_table_context(table_name=table_name)
        else:
            print('输入数据长度不一样重新输入')
    #将excel表插入数据库
    def insert_data_excel_to_sql(self,table_name='excel',path=r'C:\Users\Administrator\Desktop\结果.xlsx'):
        '''
        将excel表插入数据库
        你需要在数据库建立一个表，列明顺序和excel表一样,自动建立
        因为建立表追该函数我在考虑
        table_name数据表名称
        path文件路径记得加r
        '''
        #删除表
        learn_mysql().drop_table(table_name=table_name)
        #建立表
        learn_mysql().create_table(table_name=table_name)
        df=pd.read_excel('{}'.format(path))
        #将数据全部转成字符串
        for i in df.columns.tolist():
            df[i]=df[i].astype(str)
        data=[]
        for i in range(1,len(df[df.columns.tolist()[0]].tolist())):
            data.append(df[i-1:i].values[0])
        #添加列
        for column in df.columns.tolist():
            learn_mysql().add_table_columns_name(table_name=table_name,columns=column,endcode='varchar(225)')
        #删除id
        learn_mysql().drop_table_columns_name(table_name=table_name,columns='id')
        #print(df)
        #插入方式和数据库不一样，数据库的一行一行的插入
        #这个是一列一列的插入
        columns=','.join(df.columns.tolist())
        try:
            for m in range(len(data)):
                values=','.join(data[m])
                insert_sql='insert into {} ({}) values ({})'.format(table_name,columns,values)
                self.cursor.execute(insert_sql)
                self.db.commit()
            print('插入成功')
            self.db.close()
            #产看结果
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('检查数据类型，默认字符串')
    #将csv表插入数据库
    def insert_data_csv_to_sql(self,table_name='csv',path=r'C:\Users\Administrator\Desktop\结果.csv'):
        '''
        将csv表插入数据库
        你需要在数据库建立一个表，列明顺序和excel表一样,自动建立
        因为建立表追该函数我在考虑
        table_name数据表名称
        path文件路径记得加r
        '''
        #删除表
        learn_mysql().drop_table(table_name=table_name)
        #建立表
        learn_mysql().create_table(table_name=table_name)
        df=pd.read_csv('{}'.format(path))
        #将数据全部转成字符串
        for i in df.columns.tolist():
            df[i]=df[i].astype(str)
        data=[]
        for i in range(1,len(df[df.columns.tolist()[0]].tolist())):
            data.append(df[i-1:i].values[0])
        #添加列
        for column in df.columns.tolist():
            learn_mysql().add_table_columns_name(table_name=table_name,columns=column,endcode='varchar(225)')
        #删除id
        learn_mysql().drop_table_columns_name(table_name=table_name,columns='id')
        #print(df)
        #插入方式和数据库不一样，数据库的一行一行的插入
        #这个是一列一列的插入
        columns=','.join(df.columns.tolist())
        try:
            for m in range(len(data)):
                values=','.join(data[m])
                insert_sql='insert into {} ({}) values ({})'.format(table_name,columns,values)
                self.cursor.execute(insert_sql)
                self.db.commit()
            print('插入成功')
            self.db.close()
            #产看结果
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('检查数据类型，默认字符串')
    #将股票日线数据插入数据库，常用
    def insert_data_stock_daily_to_sql(self,table_name='stock1',path=r'C:\Users\Administrator\Desktop\结果.excel',stock='sh600031',select='0'):
        '''
        将股票表插入数据库
        你需要在数据库建立一个表，列明顺序和股票表一样，标准的二维表,自动建立
        因为建立表追该函数我在考虑
        table_name数据表名称,默认stock
        path文件路径记得加r,文件要存在
        提供2个数据方式，一个本地的excel表，一个网络数据输入股票代码就可用了
        select选择方式0网络数据，1本地数据
        必须第一列是时间开头，一般都是，如果是其他的直接用上面的换上，因为时间这个特殊
        数据默认但是字符串
        '''
        if select=='1':
            df=pd.read_excel('{}'.format(path))
        elif select=='0':
            import akshare as ak
            df=ak.stock_zh_a_daily(symbol=stock,start_date='20210101')
        #删除表
        learn_mysql().drop_table(table_name=table_name)
        #建立表
        learn_mysql().create_table(table_name=table_name)
        #将数据全部转成字符串
        for i in df.columns.tolist():
            df[i]=df[i].astype(str)
        #按列进行数据转换
        data=[]
        for i in range(1,len(df[df.columns.tolist()[0]].tolist())):
            data.append(df[i-1:i].values[0])
        #添加列
        for column in df.columns.tolist():
            learn_mysql().add_table_columns_name(table_name=table_name,columns=column,endcode='varchar(225)')
        #删除id
        learn_mysql().drop_table_columns_name(table_name=table_name,columns='id')
        #print(df)
        #插入方式和数据库不一样，数据库的一行一行的插入
        #这个是一列一列的插入
        columns=','.join(df.columns.tolist())
        try:
            for m in range(len(data)):
                values=','.join(data[m])
                insert_sql='insert into {} ({}) values ({})'.format(table_name,columns,values)
                self.cursor.execute(insert_sql)
                self.db.commit()
            print('插入成功')
            self.db.close()
            #产看结果
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('检查数据类型，默认字符串')
    #将基金日线数据插入数据库
    def insert_data_fund_data_to_sql(self,table_name='招商白酒',fund_code='161725'):
        '''
        将基金日线数据插入数据库
        table_name数据表，自动建立
        fund_code基金代码
        '''
        import akshare as ak
        df=ak.fund_open_fund_info_em(fund=fund_code)
        #删除表
        learn_mysql().drop_table(table_name=table_name)
        #建立表
        learn_mysql().create_table(table_name=table_name)
        #将数据全部转成字符串
        for i in df.columns.tolist():
            df[i]=df[i].astype(str)
        #按列进行数据转换
        data=[]
        for i in range(1,len(df[df.columns.tolist()[0]].tolist())):
            data.append(df[i-1:i].values[0])
        #添加列
        for column in df.columns.tolist():
            learn_mysql().add_table_columns_name(table_name=table_name,columns=column,endcode='varchar(225)')
        #删除id
        learn_mysql().drop_table_columns_name(table_name=table_name,columns='id')
        #print(df)
        #插入方式和数据库不一样，数据库的一行一行的插入
        #这个是一列一列的插入
        columns=','.join(df.columns.tolist())
        try:
            for m in range(len(data)):
                values=','.join(data[m])
                insert_sql='insert into {} ({}) values ({})'.format(table_name,columns,values)
                self.cursor.execute(insert_sql)
                self.db.commit()
            print('插入成功')
            self.db.close()
            #产看结果
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('检查数据类型，默认字符串')
    #将股票历史资金流入插入数据库
    def insert_data_stock_cash_data_to_sql(self,table_name='北方稀土',stock='600111'):
        '''
        将股票历史资金流入插入数据库
        table_name保持的数据表名称
        stock股票代码
        '''
        import akshare as ak
        if stock[0]=='6':
            market='sh'
        else:
            market='sz'
        df=ak.stock_individual_fund_flow(stock=stock,market=market)
        #删除表
        learn_mysql().drop_table(table_name=table_name)
        #建立表
        learn_mysql().create_table(table_name=table_name)
        #将数据全部转成字符串
        for i in df.columns.tolist():
            df[i]=df[i].astype(str)
        #按列进行数据转换
        data=[]
        for i in range(1,len(df[df.columns.tolist()[0]].tolist())):
            data.append(df[i-1:i].values[0])
        #因为-在mysql特殊字符串
        column_data=[]
        for m in df.columns.tolist():
            column_data.append(m.replace('-',''))
        #添加列
        for column in column_data:
            learn_mysql().add_table_columns_name(table_name=table_name,columns=column,endcode='varchar(225)')
        #删除id
        learn_mysql().drop_table_columns_name(table_name=table_name,columns='id')
        #print(df)
        #插入方式和数据库不一样，数据库的一行一行的插入
        #这个是一列一列的插入
        columns=','.join(column_data)
        try:
            for m in range(len(data)):
                values=','.join(data[m])
                insert_sql='insert into {} ({}) values ({})'.format(table_name,columns,values)
                self.cursor.execute(insert_sql)
                self.db.commit()
            print('插入成功')
            self.db.close()
            #产看结果
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('检查数据类型，默认字符串')
    #通用数据pandas插入数据库
    def insert_data_pandas_to_sql(self,table_name='pandas',df=None):
        '''
        通用数据pandas插入数据库
        table_name数据表名称
        df数据标准二维表
        '''
        #删除表
        learn_mysql().drop_table(table_name=table_name)
        #建立表
        learn_mysql().create_table(table_name=table_name)
        #将数据全部转成字符串
        for i in df.columns.tolist():
            df[i]=df[i].astype(str)
        #按列进行数据转换
        data=[]
        for i in range(1,len(df[df.columns.tolist()[0]].tolist())):
            data.append(df[i-1:i].values[0])
        #因为-在mysql特殊字符串
        column_data=[]
        for m in df.columns.tolist():
            column_data.append(m.replace('-',''))
        #添加列
        for column in column_data:
            learn_mysql().add_table_columns_name(table_name=table_name,columns=column,endcode='varchar(225)')
        #删除id
        learn_mysql().drop_table_columns_name(table_name=table_name,columns='id')
        #print(df)
        #插入方式和数据库不一样，数据库的一行一行的插入
        #这个是一列一列的插入
        columns=','.join(column_data)
        try:
            for m in range(len(data)):
                values=','.join(data[m])
                insert_sql='insert into {} ({}) values ({})'.format(table_name,columns,values)
                self.cursor.execute(insert_sql)
                self.db.commit()
            print('插入成功')
            self.db.close()
            #产看结果
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('检查数据类型，默认字符串')
    #显示数据表结果
    def desc_table(self,table_name='students'):
        '''
        显示数据表结果
        '''
        try:
            desc_sql='desc {}'.format(table_name)
            self.cursor.execute(desc_sql)
            self.db.commit()
            #列
            columns=[]
            for i in self.cursor.description:
                columns.append(i[0])
            value=[]
            for m in self.cursor.fetchall():
                value.append(list(m))
            df=pd.DataFrame(value,columns=columns)
            print('查询成功')
            print(df)
            #查看数据
            learn_mysql().show_table_context(table_name=table_name)
            return df
        except:
            print('{}不存在'.format(table_name))
    #显示创建数据表的方法
    def show_create_table_moth(self,table_name='students'):
        '''
        显示创建数据表的方法
        '''
        try:
            desc_sql='show create table {}'.format(table_name)
            self.cursor.execute(desc_sql)
            self.db.commit()
            #列
            columns=[]
            for i in self.cursor.description:
                columns.append(i[0])
            value=[]
            for m in self.cursor.fetchall():
                value.append(list(m))
            df=pd.DataFrame(value,columns=columns)
            print('查询成功')
            print(df)
            #查看数据
            print('查看数据')
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('{}不存在'.format(table_name))
    #修改表名1
    def alter_table_name(self,table_name='students',to_table_name='student'):
        '''
        修改表名1
        '''
        try:
            alter_sql='alter table {} rename {}'.format(table_name,to_table_name)
            self.cursor.execute(alter_sql)
            self.db.commit()
            #显示结果
            learn_mysql().show_table()
        except:
            print('{}表不存在'.format(table_name))
            learn_mysql().show_table()
    #修改表名2
    def rename_table_name(self,table_name='students',to_table_name='student'):
        '''
        修改表名1
        '''
        try:
            alter_sql='rename table {} to {}'.format(table_name,to_table_name)
            self.cursor.execute(alter_sql)
            self.db.commit()
            #显示结果
            learn_mysql().show_table()
        except:
            print('{}表不存在'.format(table_name))
            learn_mysql().show_table()

    #添加列名
    def add_table_columns_name(self,table_name='student',columns='low',endcode='int(11)'):
        '''
        添加列名
        '''
        try:
            add_sql='alter table {} add {} {}'.format(table_name,columns,endcode)
            self.cursor.execute(add_sql)
            self.db.commit()
            print('添加成功')
            self.db.close()
            #查看结果
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('{}列名已经存在'.format(columns))
    #删除列明
    def drop_table_columns_name(self,table_name='student',columns='low'):
        '''
        添加列名
        '''
        try:
            add_sql='alter table {} drop {}'.format(table_name,columns)
            self.cursor.execute(add_sql)
            self.db.commit()
            print('删除成功')
            self.db.close()
            #查看结果
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('{}列名已经删除'.format(columns))
    #调整列的位置first after
    def adjiust_columns_postion(self,table_name='student',columns='open'):
        '''
         #调整列的位置first after
         table_name表名
        '''
        #先查看数据结果
        try:
            chart=learn_mysql().desc_table(table_name=table_name)
            endcode=chart[chart['Field']=='open']['Type'].tolist()[0]
            adjust_sql='alter table {} modify {} {} first'.format(table_name,columns,endcode)
            self.cursor.execute(adjust_sql)
            self.db.commit()
            self.db.close()
            #查看结果
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('{}已经的第一列'.format(columns))
    #删除数据表
    def drop_table(self,table_name='delete'):
        '''
        删除数据表
        table_name表名
        如果存在删除
        '''
        drop_sql='drop table if exists {}'.format(table_name)
        self.cursor.execute(drop_sql)
        self.db.commit()
        self.db.close()
        print('{}表删除成功'.format(table_name))
        learn_mysql().show_table()
    #清空数据表
    def delete_from_table(self,table_name='time'):
        '''
        清空数据表
        table_name数据名称
        清空全部数据
        '''
        try:
            delete_sql='delete from {}'.format(table_name)
            self.cursor.execute(delete_sql)
            self.db.commit()
            #关闭数据库
            self.db.close()
            print('清空完成{}'.format(table_name))
            #显示表
            learn_mysql().show_table_context(table_name=table_name)
        except:
            print('数据表{}不存在/已经清空'.format(table_name))