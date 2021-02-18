# TODO ======================================== 公共方法 ========================================
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
