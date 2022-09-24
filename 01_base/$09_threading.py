import pymysql
import threading
import re
import time
from queue import Queue
from DBUtils.PooledDB import PooledDB

class ThreadInsert(object):
    "多线程并发MySQL插入数据"
    def __init__(self):
        start_time = time.time()
        self.pool = self.mysql_connection()
        self.data = self.getData()
        self.mysql_delete()
        self.task()
        print("========= 数据插入,共耗时:{}'s =========".format(round(time.time() - start_time, 3)))

    def mysql_connection(self):
        maxconnections = 15  # 最大连接数
        pool = PooledDB(
            pymysql,
            maxconnections,
            host='localhost',
            user='root',
            port=3306,
            passwd='123456',
            db='test_DB',
            use_unicode=True)
        return pool

    def getData(self):
        st = time.time()
        with open("10w.txt", "rb") as f:
            data = []
            for line in f:
                line = re.sub("\s", "", str(line, encoding="utf-8"))
                line = tuple(line[1:-1].split("\"\""))
                data.append(line)
        n = 100000    # 按每10万行数据为最小单位拆分成嵌套列表
        result = [data[i:i + n] for i in range(0, len(data), n)]
        print("共获取{}组数据,每组{}个元素.==>> 耗时:{}'s".format(len(result), n, round(time.time() - st, 3)))
        return result

    def mysql_delete(self):
        st = time.time()
        con = self.pool.connection()
        cur = con.cursor()
        sql = "TRUNCATE TABLE test"
        cur.execute(sql)
        con.commit()
        cur.close()
        con.close()
        print("清空原数据.==>> 耗时:{}'s".format(round(time.time() - st, 3)))

    def mysql_insert(self, *args):
        con = self.pool.connection()
        cur = con.cursor()
        sql = "INSERT INTO test(sku, fnsku, asin, shopid) VALUES(%s, %s, %s, %s)"
        try:
            cur.executemany(sql, *args)
            con.commit()
        except Exception as e:
            con.rollback()  # 事务回滚
            print('SQL执行有误,原因:', e)
        finally:
            cur.close()
            con.close()

    def task(self):
        q = Queue(maxsize=10)  # 设定最大队列数和线程数
        st = time.time()
        while self.data:
            content = self.data.pop()
            t = threading.Thread(target=self.mysql_insert, args=(content,))
            q.put(t)
            if (q.full() == True) or (len(self.data)) == 0:
                thread_list = []
                while q.empty() == False:
                    t = q.get()
                    thread_list.append(t)
                    t.start()
                for t in thread_list:
                    t.join()
        print("数据插入完成.==>> 耗时:{}'s".format(round(time.time() - st, 3)))


if __name__ == '__main__':
    ThreadInsert()
