# TODO ======================================== 公共方法 ========================================
import os
import threading
import time

print('======================================== 公共方法 ========================================')
# +：可以用来拼接，用于字符串、元组、列表
print('hello' + 'world')
print(('good', 'yes') + ('hi', 'ok'))
print([1, 2, 3] + [4, 5, 6])

# -：只能用户集合，求差
print({1, 2, 3} - {3})

# *：可以用于字符串元组列表，表示重复多次
print('hello' * 3)
print([1, 2, 3] * 3)
print((1, 2, 3) * 3)

# in：成员运算符
print('a' in 'abc')
print(1 in [1, 2, 3])
print(4 in (6, 4, 5))

# in 用于字典是用来判断key是否存在
print('zhangsan' in {'name': 'zhangsan', 'age': 18, 'height': '180cm'})
print('name' in {'name': 'zhangsan', 'age': 18, 'height': '180cm'})
print(3 in {3, 4, 5})

nums = [19, 82, 39, 12, 34, 58]
# 带下标的遍历 enumerate 类的使用，一般用户列表和元组等有序的数据
en = enumerate(nums)
for i, e in en:
    print('第%d个数据是%d' % (i, e))

# TODO ======================================== 可迭代对象 ========================================
print('======================================== 可迭代对象 ========================================')


class Demo(object):
    def __init__(self, x):
        self.x = x

    def __iter__(self):  # 只要重写了 __iter__ 方法就是一个可迭代对象
        pass

    def __next__(self):
        # 每一次 for...in 都会调用一次 __next__ 方法，获取返回值
        self.count += 1
        if self.count <= self.x:
            return 'hello'
        else:
            raise StopIteration  # 让迭代器停止


d = Demo(100)

from collections import Iterable

print(isinstance(d, Iterable))
# for...in循环的本质就调用可迭代对象的 __iter__ 方法，获取到这个方法的返回值
# 这个返回值需要时一个对象，然后再调用这个对象 __next__ 方法

# TODO ======================================== 正则表达式 ========================================
print('======================================== 正则表达式 ========================================')
import re

x = 'hello\\nworld'
# python 正则里 想要及匹配一个 \ 需要使用 \\\\  还可以在前面加 r，\\ 就表示 \
# m = re.search('\\\\',x)
m = re.search(r'\\', x)
print(m)  # <re.Match object; span=(5, 6), match='\\'>

'''
match 和search;
共同点：1.只对字符串查询一次 2.返回值类型都是re.Match类型的对象
不同点: match是从头开始匹配一但匹配失败 就返回None: search是在整个字符串里匹配
'''
m1 = re.match(r'good', 'hello wrold good morining')
print(m1)  # None
m2 = re.search(r'good', 'hello wrold good morining')
print(m2)  # <re.Match object; span=(12, 16), match='good'>
# print(dir(m2))
print(m2.pos, m2.endpos)  # 开始位置结束位置
print(m2.span())

# . 表示除了换行符以外的任意字符
# 任意字符 * 出现任意次数 贪婪模式
m6 = re.search(r'w.*m', 'hello wrold good morining')
print(m6.group())  # 获取匹配到的字符结果
print(m6.group(0))  # group 可以传参 表示第 n 个分组 要是没有会抛IndexError异常

# 这里有四组
m7 = re.search(r' (9.*) (e.*) (5.*7)', 'da9fioriel5kfsda7ifsdaiferit')
print(m7.group(0))  # 第 0 组就是把整个正则表达式当做一个整体
print(m7.group())  # 默认就是拿第0组
print(m7.group(1))
print(m7.group(2))
print(m7.group(3))

print(m7.groups())  # 以元组显示所有分组

# (?P<name>表达式) 可以给分组起一个名字
# groupdict 获取分组组成的字典
m8 = re.search(r' (9.*) (?P<xxx>0.*) (5.*7)', 'da9fioriel5kfsda7ifsdaiferit')
print(m8.groupdict('xxx'))
print(m7.group(1))
print(m7.group('xxx'))  # 可以通过分组名或者分组的下标获取到分组里匹配到的字符串

# finditer 查找到所有的匹配对象，返回的结果是一个可迭代对象,是一个re.Match 类型的对象
# findall  把查找到的所有字符结果都放到一个列表里
# fullmatch 完整匹配，字符串里需要完全满足正则规则才会有结果，否则就是None
m3 = re.finditer(r'x', 'klixhblagkjxqakgsxsl')
print(isinstance(m3, Iterable))
for t in m3:
    print(t)

m4 = re.findall(r'x', 'klixhblagkjxqakgsxsl')
print(m4)

m5 = re.fullmatch(r'hello', 'hello wrold')
print(m5)

# 在re模块里，可以使用re. 方法调用函数，还可以调用re.compile得到一个对象
# 正则修饰符是对正则表达式进行修饰
# . 表示除了换行符以外的任意字符
# re.S 让点 . 匹配换行
# re.I 忽略大小写
# re.M 让$ 能够匹配到换行
q = re.search(r'm.*a', 'orgsadg\nslsa', re.S)
print(q)

r = re.compile(r'm.*a')
x = r.search('orgsadgnslsa')
y = r.search('asoflio\nbghapga')
print(x)

# \w:表示的是字母数字和_ +:出现一次以上 $:以指定得内容结尾
# \w 等价于[0-9a-zA-Z]
# \W 取反
z = re.findall(r'\w+$', 'tam boy\n you are girl\n he is man')

# 1. 很多字母前面添加 \ 会有特殊含义
# 2. 绝大多数标点符号都有特殊含义
# 3. 如果想要使用标点符号 可以加 \

# \s 表示任意的空白字符（非打印字符）
# \S 表示非空白字符
# \n:表示换行 \t:表示一个制表符
# \d:表示数字 等价于 [0-9]
# \D:表示非数字 等价于 [^0-9]
print(re.search(r'\s', 'hello word\n666\t'))
print(re.search(r'x\d+p', 'x534p'))

# () 表示一个分组
g = re.search(r'h(\d+)x', 'sh829xkflsa')
print(g.group(0))
print(g.group(1))

# 如果要表示括号，需要使用 \
g1 = re.search(r'\(.*\)', '(1+1)*3+5')
print(g1.group())

# [] 用来表示可选项 [x-y]从x到y区间，包含x和y
print(re.search(r'f[0-5]m', 'odsf0m'))
print(re.search(r'f[05d]m', 'odsf0m'))  # 0或5或或d
print(re.search(r'f[0-5]+m', 'odsf0487m'))
print(re.search(r'f[0-5a-dx]m', 'odsf0dm'))  # 0-5或a-d或x

# | 用来表示或  和 [] 有一定的区别 这里只是取值
# [] 里的值表示区间，而且是单个字符
# | 就是可选值，可以出现多个值
print(re.search(r'f(x|prz|t)m', 'odsfprzm'))

# {n} 用来限定前面元素出现的次数
# {n,} 表示前面的元素出现 n 次以上
# {,n} 表示前面的元素出现 n 次以下
# *:表示前面的元素出现任意次数(0次及以上) 等价于{0,}
# +:表示前面的元素至少出现一次,等价于{1,}
print(re.search(r'go{2}d', 'good'))
print(re.search(r'go{2,}d', 'gooooood'))
print(re.search(r'go{,2}d', 'god'))
print(re.search(r'go*d', 'gooooood'))
print(re.search(r'go+d', 'god'))

# ?:两种用法:
# 1.规定前面的元素最多只能出现一次,等价{,1}
# 2.将贪婪模式转换成为非贪婪模式  r'a.*?i'
print(re.search(r'go?d', 'god'))

# ^ 以指定得内容开头 $ 指定内容结尾
# ^ 在[] 里面还可以表示取反
print(re.search(r'^a.*i$', 'adsi sodihaofdi'))

'''
# 判断用户输入的内容是否是数字，如果是数字转换成为数字类型
num = input('请输入一段数字：')
if re.fullmatch(r'\d+(\.?\d+)?',num): # 3.12.84.35
    print('数字')
    print(float(num))
else:
    print('不是一个数字')
    
# 用户名匹配 1.用户名只能包含数字、字母和下划线 2.不能以数字开头 3.长度在6到6位范围
username = input('请输入用户名：')
x = re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{5,15}',username)
if x is None:
    print('输入的用户名不符合规范')
else:
    print(x)
'''

# 以非数字开头，后面有 字母数字_-组成的长度4到14位的字符串
# r'^\D[a-z0-9A-Z_\-]{3,13}'

# 匹配邮箱
# r'^([A-Za-z0-9_\-\.])+@([A-Za-z0-9_\-\.])+\.([A-Za-z]{2,4})$'

# 匹配手机
# r'^(13[0-9])|(14[5|7)|(15([0-3]|[5-9]))|(18[05-9]))\d{8}$'


# 替换 sub
print(re.sub(r'\d', 'x', 'gasfkg46456gijdag1369'))


def test():
    y = int(x.group(0))
    y *= 2
    return str(y)


p = 'gasfkg46456gijdag1p9'
# sub 内部在调用 test 方法时，会把每一个匹配到的数据以re.Match的格式
print(re.sub(r'\d+', test, p))  # test函数是自动调用

# TODO ======================================== 网络 ========================================
print('======================================== 网络 ========================================')
import socket

# 1.创建socket,并连接
# AF_INET:表示这个socket是用来进行网络连接
# SOCK_DGRAM:表示连接是一个 TODO udp连接 只管发 不安全 没有严格的客户端和服务器的区别
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# 2.发送数据
# data:要发送的数据，它是二进制的数据
# address:发送给谁，参数是一个元祖,元祖里有两个元素
# 第0个表示目标的ip地址，第1个表示程序的端口号
s.sendto('hello'.encode('utf8'), ('192.168.31.199', 9000))
data, addr = s.recvfrom(1024)  # recufrom是一个阻塞的方法,等待
print('接收到了{}地址{}端口号接收到了消息,内容是:{}'.format(addr[0], addr[1], data).decode('utf-8'))
s.close()  # 3.关闭socket

'''
# 接受数据 绑定端口号和ip
s.bind(('192.168.31.199',9000))
# revfrom 接受数据
data,addr = s.recvfrom(1024) #recufrom是一个阻塞的方法,等待
print('从{}地址{}端口号接收到了消息,内容是:{}'.format(addr[0],addr[1],data).decode('utf-8'))
s.close()
'''

#
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# TODO tcp连接 要先和服务器建立连接才能发送数据 更安全
s.connect(('192.168.31.199', 9000))
s.send('hello'.encode('utf8'))
s.close()

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(('192.168.31.199', 9000))
s.listen(128)  # 把socker监听
# x = s.accept()
# print(x)
client_socket, client_addr = s.accept()
client_socket.recv(1024)  # udp里接受数据，使用的recvfrom tcp里使用recv获取数据
print('接收到了{}客户端{}端口号发送的数据,内容是:'.format(client_addr[0], client_addr[1]))

s.close()

# TODO ======================================== 文件下载服务器 ========================================
print('======================================== 文件下载服务器 ========================================')
import socket

server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.bind(('192.168.31.199', 9090))
server_socket.listen(128)
client_socket, client_addr = server_socket.accept()
client_socket.recv(1024).decode('utf-8')
# print('接收到了{}客户端{}端口号发送的数据,内容是:'.format(client_addr[0], client_addr[1],data))
if os.path.isfile(data):
    with open(data, 'rb', encoding='utf-8') as file:
        content = file.read()
        client_socket.send(content.encode('utf-8'))
else:
    print('文件不存在')

# TODO ======================================== 文件下载客户端 ========================================
print('======================================== 文件下载客户端 ========================================')
import socket

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('192.168.31.199', 9090))
file_name = input('请输入您要下载的文件名：')
s.send(file_name.encode('utf8'))

content = s.recv(1024).decode('utf-8')
with open(file_name, 'wb', encoding='utf-8') as file:
    file.write(content)

s.close()

# TODO ======================================== 多线程 ========================================
print('======================================== 多线程 ========================================')


def dance():
    for i in range(50):
        time.sleep(0.2)
        print('跳舞')


def sing():
    for i in range(50):
        time.sleep(0.2)
        print('唱歌')


# target 需要的是一个函数，用来指定线程需要执行的任务
t1 = threading.Thread(target=dance)  # 创建了线程1
t2 = threading.Thread(target=sing)  # 创建了线程2

t1.start()
t2.start()

# TODO ====== 卖票
ticket = 50

lock = threading.Lock()  # 创建一把锁


def sell_ticket():
    global ticket
    while True:
        lock.acquire()  # 加同步锁
        if ticket > 0:
            time.sleep(1)  # 模拟繁忙下单支付 网络延迟
            ticket -= 1
            lock.release()  # 释放锁
            print('{}卖出一张票，还剩{}张'.format(threading.current_thread().name, ticket))
        else:
            print('票卖完了')


t1 = threading.Thread(sell_ticket, name='线程1')
t2 = threading.Thread(sell_ticket, name='线程1')
t1.start()
t2.start()

# TODO ====== 面包 生产者消费者
import queue


def produce():
    for i in range(10):
        time.sleep(0.5)
        print('生产了+++面包{}{}'.format(threading.current_thread().name, i))
        q.put('{}{}'.format(threading.current_thread().name, i))


def consumer():
    for i in range(100):
        time.sleep(0.3)
        print('买到了---面包{}'.format(q.get()))


q = queue.Queue()  # 创建一个队列

# 一条生产线
t1 = threading.Thread(produce, name='c1')
# 一条消费线
t2 = threading.Thread(consumer, name='p1')

t1.start()
t2.start()

# TODO ======================================== 多进程 ========================================
print('======================================== 多进程 ========================================')
import multiprocessing


# 进程是cpu控制的最小单位
def dance():
    for i in range(50):
        time.sleep(0.5)
        print('跳舞，pid={}'.format(os.getpid()))


def sing():
    for i in range(50):
        time.sleep(0.5)
        print('唱歌，pid={}'.format(os.getpid()))


if __name__ == "__main__":
    print('主进程的pid={}'.format(os.getpid()))
    # 创建了两个进程trarget用来表示执行的任务
    # args用来传参,类型是一个元组
    p1 = multiprocessing.Process(target=dance, args=(100,))
    p2 = multiprocessing.Process(target=sing, args=(100,))
    p1.start()
    p2.start()

# TODO ======================================== 进程共享全局变量 ========================================
print('======================================== 进程共享全局变量 ========================================')

# 同一进程间的不同线程可以共享全局变量，不同进程间不能共享全局变量
# 一个程序里至少有一个主进程，一个主进程里至少有一个主线程

n = 100


def test():
    global n
    n += 1
    print('{}里n的值是{}'.format(os.getpid(), n))


def demo():
    global n
    n += 1
    print('{}里n的值是{}'.format(os.getpid(), n))


# 同一个主进程里的两个子线程，线程之间可以共享同一进程的全局变量
# t1 = threading.Thread(test) # test() 101
# t2 = threading.Thread(demo) # demo() 102
# t1.start()
# t2.start()

# 不同进程各自保存一份全局变量
p1 = multiprocessing.Process(target=test)  # test() 101
p2 = multiprocessing.Process(target=demo)  # demo() 101
p1.start()
p2.start()

# TODO ======================================== queue队列 ========================================
print('======================================== queue队列 ========================================')

# q1 = queue.Queue()  # 进程间通信
# q2 = multiprocessing.Queue()  # 线程间通信

# 创建队列时，可以指定最大长度。默认是0，表示不限长
q = multiprocessing.Queue(5)

q.put('ho1w')
q.put('ho3w')
q.put('ho4w')
q.put('ho5w')
q.put('ho6w')

# print(q.full()) # True
# q.put('how') #无法放进去
# 往队列里方法了how
# block = True:表示是阻塞,如果队列已经满了,就等待
# timeout 超时 等待多久以后程序会出错单位是秒
q.put('how', block=True, timeout=1)
# q.put_nowait('how')  # 等价于 q.put('how',block=False)
print(q.get())

q.get(block=True, timeout=10)

# TODO ======================================== 线程、进程join ========================================
print('======================================== 线程、进程join ========================================')
x = 10

def test1(a, b):
    time.sleep(1)
    global x
    x = a + b

# test1(1,1)
# print(x)

t1 = threading.Thread(target=test1, args=(1, 1), name='c1')
t1.start()
t.join()  # 让主线程等待子线程
print(x)

# TODO ======================================== http服务器 ========================================
print('======================================== http服务器 ========================================')
# HTTP协议: HyperText Transfer Protocol 超文本传输协议
# 协议的作用就是用来传输超文本 HTML(HyperTextMarkupLanguage)
# HTML :超文本标记语言
# HTTP:用来传输超文本的一个协议
# HTTP 服务器都是基于TCP的socket 链接

class MyServer(object):
    def __init__(self,ip,port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((ip, port))
        self.socket.listen(128)

    def run_forever(self):
        while True:
            # 获取的数据是一个元组,元组里有两个元素
            # 第0个元素是客户端的socket链接
            # 第1个元素是客户端的ip地址和端口号
            client_socket, client_addr = self.socket.accept()

            # 从客户端的 socket 里获取数据
            data = client_socket.recv(1024).decode('utf-8')
            print('接受到{}的数据{}'.format(client_addr[0], data))

            path = '/'
            if data:  # 浏览器发送过来的数据有可能是空的
                path = data.splitlines()[0].split(' ')[1]
                print('请求的路径是{}'.format(path))

            response_body = 'hello world'
            response_header = 'HTTP/1.1 200 OK\n'  # 200 ok 成功了
            if path == '/login':
                response_body = '欢迎来到登录页面'
            elif path == '/register':
                response_body = '欢迎来到注册页面'
            elif path == '/':
                response_body = '欢迎来到首页'
            else:
                #  页面未找到 404 Page Not Found
                response_header = 'HTTP/1.1 404 Page Not Found\n'
                response_body = '对不起 你找的页面不存在!!!'

            # 返回内容之前，需要先设置HTTP响应头
            # 设置一个响应头就换一行
            response_header += 'content-type: text/html;charset=utf8\n' + '\n'
            # client_socket.send('HTTP/1.1 200 OK\n'.encode('utf-8'))
            # client_socket.send('content-type: text/html\n'.encode('utf-8'))
            # 所有的响应头设置完成以后，再换行
            # client_socket.send('\n'.encode('utf-8'))

            response = response_header + response_body
            # 发送内容
            client_socket.send(response.encode('utf8'))
            print(data)


server = MyServer('192.168.31.199', 9090)
server.run_forever()

# TODO ======================================== wsg服务器 ========================================
print('======================================== wsg服务器 ========================================')
