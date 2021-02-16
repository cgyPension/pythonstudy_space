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
    print('第%d个数据是%d'%(i, e))
