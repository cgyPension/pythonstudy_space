import multiprocessing
import os
from multiprocessing import Process,Queue
import time,random

# 传递给apply_async()的函数如果有参数，需要以元组的形式传递 并在最后一个参数后面加上 , 号，如果没有加, 号，提交到进程池的任务也是不会执行的
# 原本想用apply_async()里传递1个函数，把函数执行过程中产生的中间结果放到队列(Manager.Queue()也是)里供其它进程消费的，结果放到进程池里的任务压根就不会执行，我觉得不执行的原因可能为同步队列有阻塞行为，所以直接导致了提交的任务被拒绝执行
# 使用pool管理子进程时，不能直接使用Queue，而是需要引入、使用multiprocessing.Manager.Queue(). 例如以下案例。
# apply_async()方法之后不能接get()，将返回结果直接赋值给一个变量即可，然后慢慢等待多进程程序执行完毕 除非用回调 但是回调又是一个一个结果写入
#  print("reader启动(%s),父进程为(%s)" % (os.getpid(), os.getppid()))
# 不能在join前get
# 使用配置文件运行 才能显示运行

from multiprocessing import  Pool
import  time ,os ,random

def worker(msg):
    t_start = time.time() #获取当前系统时间，长整型，常用来测试程序执行时间
    print("%s开始执行,进程号为%d" % (msg,os.getpid()))
    # random.random()随机生成0~1之间的浮点数
    # time.sleep(random.random()*2)
    t_stop = time.time()
    print(msg,"执行完毕，耗时%0.2f" % (t_stop-t_start))


# def main():
#     #Pool的默认大小是CPU的核数
#     po  = Pool(3)# 定义一个进程池，最大进程数3，大小可以自己设置，也可写成processes=3
#     for i in range(0,10):
#         # Pool().apply_async(要调用的目标,(传递给目标的参数元祖,))
#         # 每次循环将会用空闲出来的子进程去调用目标
#         po.apply_async(worker,args=(i,))
#
#     print("----start----")
#     po.close()  # 关闭进程池，关闭后po不再接收新的请求
#     po.join()  # 等待po中所有子进程执行完成，必须放在close语句之后
#     print("-----end-----")
if __name__ == '__main__':
    # main()
    #Pool的默认大小是CPU的核数
    po = Pool(processes=3)# 定义一个进程池，最大进程数3，大小可以自己设置，也可写成processes=3
    for i in range(0,10):
        # Pool().apply_async(要调用的目标,(传递给目标的参数元祖,))
        # 每次循环将会用空闲出来的子进程去调用目标
        po.apply_async(worker,args=(i,))

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接收新的请求
    po.join()  # 等待po中所有子进程执行完成，必须放在close语句之后
    print("-----end-----")
'''
----start----
0开始执行,进程号为5056
1开始执行,进程号为968
2开始执行,进程号为5448
2 执行完毕，耗时0.38
3开始执行,进程号为5448
1 执行完毕，耗时0.47
4开始执行,进程号为968
4 执行完毕，耗时0.02
5开始执行,进程号为968
3 执行完毕，耗时0.13
6开始执行,进程号为5448
5 执行完毕，耗时1.44
7开始执行,进程号为968
6 执行完毕，耗时1.45
8开始执行,进程号为5448
0 执行完毕，耗时1.99
9开始执行,进程号为5056
8 执行完毕，耗时0.18
7 执行完毕，耗时0.58
9 执行完毕，耗时1.75
-----end-----'''
