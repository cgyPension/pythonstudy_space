import os,multiprocessing,time

def create(x):
    for i in range(20):
        time.sleep(0.5)
        print('生产了+++pid{}{}'.format(os.getpid(),i))
        x.put('pid{}{}'.format(os.getpid(),i))

def consumer(x):
    for i in range(20):
        time.sleep(0.3)
        print('消费了---pid{}{}'.format(os.getpid(),x.get()))

if __name__ == "__main__":
    # q = queue.Queue()
    q = multiprocessing.Queue()  # 和线程的队列不一样
    p1 = multiprocessing.Process(target=create,args=(q,))  #
    p1.start()
    p2 = multiprocessing.Process(target=consumer,args=(q,))  #

    p2.start()