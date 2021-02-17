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

