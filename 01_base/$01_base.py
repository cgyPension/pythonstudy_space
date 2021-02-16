# TODO ======================================== 注释 ========================================
# 单行注释
'''
多行注释①
'''

"""
多行注释②
"""

# TODO ======================================== 数据类型 ========================================
print('======================================== 数据类型 ========================================')
# 单引号 双引号 一对三引号、双引号都能表示字符串

print(45)  # int整数类型
print(3.1415)  # float类型
print((-1) ** 0.5)  # complex类型

# 字符串类型：python里的字符串要求使用一对单引号，或者双引号来包裹
print('今天天气好晴朗')
print('56')  # 不能像java +1 会报错不会变成字符串

# 对象是存储单个数据的容器，而列表（数组）就是存储多个数据的容器，任何数据类型都可以存放
lst = [1, '2', '三']
lst2 = list([1, '2', '三'])

# type 查看变量类型
a = 23
print(type(a))

# 将数字转换为布尔值
print(bool(100))  # True
print(bool(-1))  # True
print(bool(0))  # Flase 数字里只有0转换为Flase，其他数字转化为布尔值都是True

# 将字符串转换为布尔值
print(bool('hello'))  # True
print(bool('False'))  # True
print(bool(''))  # Flase 只有空字符串 '' / "" None 空的数据类型元组 列表等 转换为Flase

print(True + 1)  # 2
print(False + 1)  # 1

# 隐式转换
if 3:
    print('隐式转换打印')

# 可迭代对象: str list tuple dict set range 都可以便利
# str list tuple 可以通过下标来获取或者操作数据
# 列表中 正向索引从0到N-1 逆向索引从-N到-1

# TODO ======================================== 标识符与关键字 ========================================
'''
标识符：变量，模块名，函数名，类名
标识符的命名规则与规范:
1.由数字、字母和组成，不能以数字开头
2.严格区分大小写
3.不能使用在Python有特殊含义的关键字

变量命令规范
1 小驼峰   变量名
2 大驼峰   类名
3 下划线连接 变量、函数和模块名使用下划线连接

python语言里,使用强制缩进来表示语句之间的结构
'''

# TODO ======================================== 算数运算符 ========================================
# + - * /除 **幂运算 //整除（只取整数） %取余
# 5 ** 2 === 5的2次方
# // 表示做除运算后取整

# python3 和 python2 得到的结果会有点区别
# TODO ======================================== 比较运算符 ========================================
'''
> < >= <= = !=
ord(chr) 返回该chr的ASCII码(只能为单个字符)
chr(num) 返回该ASCII对应的字符
'''

# TODO ======================================== 逻辑运算 ========================================
print('======================================== 逻辑运算 ========================================')
print(2 > 1 and 5 > 3 and 10 > 2)  # True
print(3 > 2 and 5 < 4 and 6 > 1)  # False

print(3 > 9 or 4 < 7 or 10 < 3)  # True
print(3 > 5 or 4 < 2 or 8 < 7)  # False

# 逻辑非
print(not (5 > 2))  # False

# TODO ======================================== if分支 ========================================
print('======================================== if分支 ========================================')

# 双分支 注意缩进 bool表达式
if 2 > 1:
    print('执行语句1')
else:
    print('执行语句2')

'''
# 分支嵌套 注意缩进
if bool表达式:  
    if bool表达式2:
        print('执行嵌套语句')
    else:
        print('执行语句1')  
else:
    print('执行语句2')
'''

'''
# 多分支 注意缩进
if bool表达式1:  
    print('执行语句1')  
elif bool表达式2:
    print('执行语句2')
elif bool表达式...:
    print('执行语句...')
else: 
    print('如上都不满足，则执行这条语句')
'''

'''
if numl > num2:
    x = numl
else:
    x = num2

x = num1 if num1 > num2 else num2  # 三元表达式
print('两个数里的较大数是', x)
'''

# TODO ======================================== 循环 ========================================
print('======================================== 循环 ========================================')
'''
# 注意判断条件
while bool表达式:
    print('执行语句')
 
# in后面必须是一个可迭代的对象   
# range(start, end, rule) 从start开始，到end-1，间隔为rule,rule的正负表示正序或倒序
# range(1, 11) 输出1~10，不包括最大值11
# range(1, 11, 2) 输出1~10，不包括最大值11, 第三个参数表示为每次间隔2
# range(11, 1, -2) 输出11~2，不包括最小值值1, 第三个参数为负数，则表示为倒序且每次间隔2
# range(5) 简写形式，0~5的范围
for i in range(1,11):
    print(i)    

for y in 'hello':
    print(y)   
    
break # 跳出本循环
continue # 结束本次循环，执行下一次循环 


'''

# python打印三角形
i = 0
while i < 5:
    i += 1
    print(i * '*')

# python打印矩形

# python打印9*9乘法表
# 外循环控制函数，内循环控制每一行的列数
j = 0
while j < 9:
    j += 1
    i = 0
    while i < j:
        i += 1
        print(i, '*', j, "=", (i * j), sep="", end="\t")
    print()

for i in range(1, 10):
    for j in range(1, i + 1):
        print(j, '*', i, '=', i * j, end='\t', sep='')
    print()

# TODO ======================================== list列表 ========================================
print('======================================== list列表 ========================================')

# 找不到元素则抛出异常， 可预先使用in方法判断是否存在
# print(list.index('元素'))

lst3 = [10, 20, 30]
lst3.append(100)  # 在原有的列表末尾添加一个元素
print('append：', lst3)  # [10, 20, 30, 100]

lst3.extend([99, 100])  # 在原有的列表末尾至少添加一个元素，添加元素必须是列表（添加多个）
print('extend：', lst3)

lst3.insert(1, 99)  # 在原有的列表指定位置添加一个元素
print('insert：', lst3)

lst3[1:] = [66]  # 切片添加多个元素(指定位置添加多个元素) 使用切片把选择到的元素替换掉
print('替换：', lst3)

lst3.remove(10)
print('删除10：', lst3)  # 在原有的列表删除一个元素，如果有重复元素只删除一个，如果元素不存在则抛出异常

lst3.pop(0)
print('pop：', lst3)  # 删除指定索引位置的元素，如果指定索引不存在则抛出异常 不传index，则删除末尾元素

lst.clear()
print('clear：', lst)  # 清空列表

lst4 = [10, 20, 30]
lst4[1] = 28  # 直接重新赋值
print('修改：', lst4)

lst4[1:3] = [40, 50, 60]
print('批量修改：', lst4)

# sotr()
lst5 = [9, 3, 2, 8]
lst5.sort()
print('不传默认升序：', lst5)  # [2, 3, 8, 9]

strname = ['你好', '大家好', '大佬']
print(strname.reverse())  # reverse() 反转

# sotr(reverse=True)
lst5 = [9, 3, 2, 8]
lst5.sort(reverse=True)
print('传入reverse=True降序排序', lst5)  # [9, 8, 3, 2]

# sorted(lst)   将产生一个 新列表 对象 原有还是乱序
# sorted(lst, reverse=False)
# sorted(lst, reverse=True)
lst6 = [22, 7, 12, 8]
new_lst6 = sorted(lst6)
print(lst6, new_lst6)

# 列表生成的表达式 列表推导式
lst7 = [i * i for i in range(1, 6)]
print(lst)  # [1, 4, 9, 16, 25]

points = [(x, y) for x in range(5, 9) for y in range(10, 20)]
print(points)

# for...in循环的本质就是不断的调用迭代器的next方法查找下一个数据
for k in lst6:
    print(k)

'''
列表常用函数
len(list)：列表元素个数
max(list)：返回列表元素最大值
min(list)：返回列表元素最小值
'''

# 列表的嵌套


# TODO ======================================== 字典 ========================================
print('======================================== 字典 ========================================')
'''
字典的特点：
①字典中的所有元素都是一个key-value对，key不允许重复（要是重复后一个key的值会覆盖前一个），value可以重复
②字典中的元素是 无序的
③字典中的key必须是不可变对象，（即key不能为变量）
④字典也可以根据需要动态的伸缩
⑤字典会浪费较大的内存，是一种使用空间换时间的数据结构
'''

info = {'name': 'cgy', 'age': 18}
print(info['name'])  # cgy []如果字典中不存在指定的key，抛出 KeyError 异常
print(info.get('age'))  # get()方法取值 如果字典中不存在指定的 key，并不会抛出 KeyError 而是返回 None
print(info.get('sex', '男'))  # 如果sex字段为None的话，则打印 '男' 默认值

# key 的判断
print('name' in info)  # True
print('sex' not in info)  # True

# key 的删除
del info['name']
print(info)
info.clear()
print(info)

# 新增key 如果key存在则是修改 不在新增
info = {'name': 'cgy', 'age': 18}
info['sex'] = '男'
print(info)  # {'name':'cgy', 'age':18, 'sex':'男'}

# 修改key
info['age'] = 20
print(info)

# 获取字典的视图方法
print(info.keys())  # 返回字典的所有key组成的列表
print(info.values())  # 返回字典的所有value组成的列表
print(info.items())  # 返回字典的所有key:value组成的列表

# 字典的遍历
for item in info:
    print(item, info[item])  # name cgy...

# 字典的生成式 拉链
key1 = ['Fruits', 'Books', 'Others']
value1 = [98, 97, 95]
d = {k1: v1 for k1, v1 in zip(key1, value1)}
print(d)  # {'Others': 95, 'Books': 97, 'Fruits': 98}

info.update(d)  # 字典合并
print(info)

# 字典 wordcount
chars = ['a', 'g', 'x', 'a', 'p', 'p', 'a', 'g']
char_count = {}
for char in chars:
    # if char in char_count:
    #     char_count += 1
    # else:
    #     char_count[char] = 1
    if char not in char_count:
        char_count[char] = chars.count(char)

print(char_count)

vs = char_count.values()
# 可以使用内置函数 max 取最大值
max_count = max(vs)  # 3

for k, v in char_count.items():
    if v == max_count:
        print(k)

# Python        Json
#   字典          对象
#   列表、元组     数组
# jumps 将字典、列表、集合、元组等转换成为JSON字符串
import json

m = json.dumps(info)
print(m)
n = '{"name": "cgy", "age": 20,"gender":"male"}'
s = json.loads(n)  # 把json字符串转换为Python里的数据类型
print(type(s))

'''
练习_01
x = input('请输入您的姓名：')
for persion in persons:
    # if name in persion: # in 运算符，如果直接用在字典上，是用来判断key是否存在，而不是value
    if persion['name'] == x:
        print('您输入的名字存在')
        break
else:
    # print('您输入的名字不存在')
    # 创建一个新的字典 new_persion
    new_persion = {'name':x}

    y = int(input('请输入您的年龄：'))
    new_persion['age'] = y

    # 把这个新的数据存储到 persons 列表里
    persons.append(new_persion)
    print('用户添加成功')
    
练习_02
dict1 = {"a": 100, "b": 200, "c": 300}

# dict2 = {}
# for k, v in dict1.items():
#     dict2[v] = k
# 
# dict1 = dict2

dict1 = {v: k for k, v in dict1.items()}  # 字典推导式
print(dict1)

练习_03
students = [
    {...}
]
count = 0
teenager_count = 0
max_score = students[0]['score']  # 假设第0个学生的成绩是最高的
max_indes = 0  # 假设最高分的学生下标是0
for student in students:
    if student['score'] < 60:
        count += 1
        print('%s不及格，分数是%d' % (student['name'], student['score']))
    if student['age'] < 18:
        teenager_count += 1
    # if student['tel'].endswith('8'):  # int类型
    if student['tel'][-1] == '8'
        print('%s的手机号以8结尾' % student['name'])

    if student['score'] > max_score:  # 遍历时，发现一个学生的成绩大于假设的最大数
        max_score = student['score']
        # max_indes = i  # 修改最高分的同时，把最高分的下标也修改

print('不及格的学生有%d' % count)
print('未成年的学生有%d' % teenager_count)
print('最高成绩是%d' % max_score)

for student in students:
    if student['score'] == max_score:
        print('最高分是%s' % student['name'])
'''

# 字典排序
students = [
    {'name': 'xiaojing', 'age': 20},
    {'name': 'xiaoming', 'age': 18},
    {'name': 'linyang', 'age': 19}
]


def foo(ele):
    return ele['age']  # 通过返回值告诉sort方法，按照元素的那个属性进行排序


# students.sort(key=foo)
students.sort(key=lambda ele: ele['age'])
print(students)


def bar(x, y):
    # x = 0
    # y = {'name': 'xiaoming', 'age': 18},
    return x + y['age']


# print(reduce(bar, students, 0))
# print(reduce(lambda x, y: x + y['age'], students, 0))

# TODO ======================================== 元组 ========================================
print('======================================== 元组 ========================================')
# 列表是可变的，而元组是不可变数据类型
t = ('python', 'world', '20')
t1 = tuple(('python', 'world', '20'))
t2 = ('python',)  # 如果元组只有一个元素，则在元素后面加上 ，
print(t2[0])  # 获取元组的元素 python

print(t1.index('python'))
print(t1.count('world'))

m, n = 3, 5  # 拆包 要是变量和值的个数不一致，会报错
print(m, n)

o, *p, q = 1, 2, 3, 4, 5, 6
print(o, p, q)

x = 'hello', 'good', 'yes'  # 括号可以省略
print(x)  # ('hello', 'good', 'yes')

# 元组的遍历
for yuanzu in t:
    print(yuanzu)

# 注：
# 如果元组中对象本身是不可变对象，则不能再引用其他对象
# 如果元组中的对象是可变对象，则可变对象的引用不允许改变，但数据可以改变

print(tuple(lst6))  # 列表转元组
print(list(t))  # 元组转列表

'''
练习_01
sing = ('李白', '白居易', '李清照', '杜甫', '王昌齡', '王維', '孟浩然', '王安石')
dance = ('李商隐', '杜甫', '李白', '白居易', '岑参', '王昌齡')
rap = ('李清照', '刘禹锡', '岑參', '王昌齡', '苏轼', '王維', '李白')
# 去重
total = set(sing+dance+rap)

# 求只选了一个学科的人的数量和对应的名字
sing_only = []
for p in sing:
    if p not in dance and p not in rap:
        sing_only.append(p)
print('只选择了第一个学科的有{}人，是{}'.format(len(sing_only),sing_only))

'''

# TODO ======================================== 集合 ========================================
print('======================================== 集合 ========================================')
# 集合的存储是无序的 value不能重复
s = {2, 3, 4, 5, 2, 4}
print(s)  # {2,3,4,5}

# set()函数
s1 = set(range(6))
print(s1)  # {0, 1, 2, 3, 4, 5}

s2 = set([1, 2, 3, 5, 6, 4, 3])
print(s2)  # {1, 2, 3, 4, 5, 6}

s3 = set((1, 23, 5, 32, 1))
print(s3)  # {32, 1, 5, 23}

s4 = set('python')
print(s4)  # {'p', 't', 'h', 'n', 'o', 'y'}

# 空集合定义  空字典定义是{}
s5 = set()

s6 = {10, 20, 30, 40, 50}
# 判断是否存在
print(10 in s6)  # True
s6.pop()  # 删除一个
s6.add(80)  # 新增
# union 将多个集合合并成一个新的集合
s6.update({200, 400})  # 新增多个
s6.remove(10)  # 删除特定元素
s6.discard(900)  # 如果有则删除900， 没有也不会报错
s6.pop()  # 随机删除一个， 不能传参
s6.clear()  # 清空集合

s7 = {10, 20, 30, 40}
s8 = {20, 40}
s9 = {80, 60}
print(s7 == s8)  # False
print(s8.issubset(s7))  # True 假如集合s7的元素里包括集合s8的全部元素，则s7是s8的子集
print(s7.issuperset(s8))  # True 与上相反,假如集合s7的元素里包括集合s8的全部元素，则s7是s8的超集
print(s7.isdisjoint(s9))  # False 无交集  判断两个集合是否有交集

print(s7 - s8)  # A - B 求A和B的差集
print(s7 & s8)  # 求A和B的交集
print(s7 | s8)  # 求A和B的并集
print(s7 ^ s8)  # 求A和B的差集的并集

# TODO ======================================== 字符串常用操作 ========================================
print('======================================== 字符串常用操作 ========================================')
m = 'my said:"I am th"'  # 字符串里面有双引号，外面就使用单引号
n = "I\'m th"  # \ 表示的是转义字符，作用是对 \ 后面的字符进行转义

# 切片语法 不改变原有字符串 左闭右开包头不包尾
print(m[2:9])
print(m[2:])  # 会截取到最后
print(m[3:15:1])  # 步长为1 注:步长不能为0
print(m[3:15:2])  # 步长为2
print(m[3:15:-1])  # 步长为1
print(m[::])  # 保持不变
print(m[::-1])  # 倒序

print(len(m))  # 获取字符串长度

# 字符串的查询操作方法
str = 'abcdebfg'
print(str.index('b'))  # 查找子串第一次出现的位置，如果查找的子串不存在时，则抛出ValueError
print(str.rindex('b'))  # 查找子串最后一次出现的位置，如果查找的子串不存在时，则抛出ValueError
print(str.find('b'))  # 查找子串第一次出现的位置，如果查找的子串不存在时，则返回-1
print(str.rfind('b'))  # 查找子串最后一次出现的位置，如果查找的子串不存在时，则返回-1

# 字符串的大小写转换操作的方法
str1 = 'ABCD'
str2 = 'abCD'
print(str.upper())  # 转成大写字母
print(str1.lower())  # 转成小写字母
print(str2.swapcase())  # 把字符串中所有大写字母转成小写字母，把所有小写字母转成大写字母
print(str2.capitalize())  # 把第一个字符转换为大写，其余字符小写
print(str.title())  # 把每个单词的第一个字符转换为大写，把每个单词的剩余字符转换为小写

# 字符串内容对齐操作的方法
'''
center()	居中对齐，第一个参数指定宽度，第二个参数指定填充符，第二个参数是可选的，默认是空格，如果设置宽度小于实际宽度，则返回原字符串
ljust()	左对齐，第一个参数指定宽度，第二个参数指定填充符，第二个参数是可选的，默认是空格，如果设置宽度小于实际宽度，则返回原字符串
rjust()	右对齐，第一个参数指定宽度，第二个参数指定填充符，第二个参数是可选的，默认是空格，如果设置宽度小于实际宽度，则返回原字符串
zfill()	右对齐，左边用0填充，该方法只接收一个参数，用于指定字符串的宽度，如果设置宽度小于实际宽度，则返回原字符串
'''

# 字符串分割操作的方法
'''
split(cha, max)	从字符串的左边开始分割，默认分割符是空格，返回值是一个列表，第一个参数为分割符，第二个参数为最大分割次数
split(cha, max)	从字符串的右边开始分割，默认分割符是空格，返回值是一个列表，第一个参数为分割符，第二个参数为最大分割次数
'''

# 判断字符串操作的方法
'''
startswith() 判断开头是否包含
endswith() 判断结尾是否包含
isidentifier()	判断指定字符串是不是合法的标识符
isspace()	判断指定字符串是否由空白字符组成（回车、换行、水平制表符tab）
isalpha()	判断指定字符串是否全部由字母组成
isdecimal()	判断指定字符串是否全部由十进制的数字组成
isnumeric()	判断指定字符串全部由数字组成
isalnum()	判断指定字符串是否全部由字母和数字组成
'''

# 字符串操作的其他方法
'''
replace()	第1个参数指定被替换的子串
第2个参数指定替换子串的字符串
该方法返回替换后的字符串，替换前的字符串不会发生变化
该方法的第3个参数可以指定最大替换次数

cha.join(lst)	用cha将列表后元组的字符串合并成一个字符串。使用方法为cha.join(lst)
eval()  执行字符串里的代码
'''
print('{0:.3}'.format(3.1415926))  # 0表示顺序(第一个数) .3表示一共是3位数
print('{:.3f}'.format(3.1415926))  # .3f表示一共是3位小数
print('{:10.3f}'.format(3.1415926))  # 同时设置宽度和精度.一共10位，3位是小数

print('abcdefxghli'.partition('x'))  # ('abcdef', 'x', 'ghli') partition指定一个字符串作为分隔符，分为三部分前面 分隔符 后面
print('2020.2.14你懂的.mp4'.rpartition('.'))  # ('2020.2.14你懂的', '.', 'mp4')

# 字符串格式化操作
name = '大佬'
age = 18

# %s 的s表示数据类型  s:字符串  i:整数
print('我叫%s，我今年%i岁' % ('cgy', 18))  # 我叫cgy，我今年18岁'

# {}里的数字表示后面变量的索引，可重复使用 不写索引和写索引不能混合使用
print('我叫{}，我今年{}岁'.format('靓仔', 18))  # 我叫靓仔，我今年18岁
print('我叫{0}，我今年{1}岁，我真的叫{0}'.format('cgy', 18))  # 我叫cgy，我今年18岁，我真的叫cgy'

# 前面加f  可在{}中直接填写变量
print(f'我叫{name}，我今年{age}岁')  # 我叫cgy，我今年18岁'

# 字符串编码和解码操作
bm = '好好学习，天天向上'
# 使用GBK格式进行编码, 此格式中，一个中文占两个字节
print(bm.encode(encoding='GBK'))  # b'\xba\xc3\xba\xc3\xd1\xa7\xcf\xb0\xa3\xac\xcc\xec\xcc\xec\xcf\xf2\xc9\xcf'

# 使用UTF-8格式进行编码 此格式中，一个中文占三个字节
print(bm.encode(
    encoding='UTF-8'))  # b'\xe5\xa5\xbd\xe5\xa5\xbd\xe5\xad\xa6\xe4\xb9\xa0\xef\xbc\x8c\xe5\xa4\xa9\xe5\xa4\xa9\xe5\x90\x91\xe4\xb8\x8a'
# 解码  （使用什么格式编码，就必须使用什么格式解码，否则会报错）
# 使用GBK格式进行编码
print(bm.encode(encoding='GBK').decode(encoding='GBK'))  # 好好学习，天天向上
# 使用UTF-8格式进行编码
print(bm.encode(encoding='UTF-8').decode(encoding='UTF-8'))  # 好好学习，天天向上

# TODO ======================================== 函数 ========================================
print('======================================== 函数 ========================================')
'''
# 定义
def 函数名(a, b): # 参数a和b 可以填默认值
    函数体  
    return  # 返回值  没有返回值时可以不写 函数的返回值，如果是1个，直接返回原值 函数的返回值，如果是多个，返回的结果为元组
    
函数的三要素：函数名、参数和返回值井在有一些编程语言里，允许函数重名
在Python里不允许函数的重名 如果函数重名了，后一个函数会覆盖前一个函数
Python 里函数名也可以理解为变量 所以变量一定不能和内置函数重名在

缺省参数就是设置形参默认值，如果有缺省参数要放在最后
如果有位置参数和关键字参数，关键字参数一定要放在位置参数后面
'''


def cals(a1, b1=100):
    c1 = a1 + b1
    return c1


print(cals(20, 30))
print(cals(20))


def fun(lst):
    '''
    :param lst: 放入一个数字列表
    :return: 返回奇偶数
    '''
    odd = []  # 存放奇数
    even = []  # 存放偶数
    for i in lst:
        if (i % 2):
            odd.append(i)
        else:
            even.append(i)
    return odd, even


lst11 = [10, 23, 65, 78, 32, 77]
print(fun(lst11))  # ([23, 65, 77], [10, 78, 32])
help(fun)  # 查看函数注释文档


# 位置参数 使用*定义可变形参，结果为一个元组
def fun2(*args):
    print(args)


fun2(1)  # (1,)
fun2(1, 2, 3)  # (1, 2, 3)


# 无法确定传递的关键字实参个数，这个时候就要使用可变的关键字形参。使用**定义，结果为一个字典
def fun3(**args):
    print(args)


fun3(a=10)  # {'a':10}
fun3(a=10, b=20, c=30)  # {'a':10, 'b':20, 'c':30}
'''
以上参数在函数定义时，一种方式只能定义一次。
当两种参数定义方式同时存在时，位置形参必须在关键字形参前面
'''


def fun4(a, b, c):  # a,b,c在函数的定义处,所以是形式参数
    print('a=', a)
    print('b=', b)
    print('c=', c)


# 函数的调用
fun4(10, 20, 30)  # 位置传参
lst = [10, 20, 30]
fun4(*lst)  # 在函数调用时，将列表中的每个元素都转换为位置实参传入

fun4(a=100, b=200, c=300)  # 关键字传参
dic = {'a': 111, 'b': 222, 'c': 333}
fun4(**dic)  # 在函数的调用时,将字典中的键值对都转换为关键字实参传入

# 如果局部变量的名和全局变量同名，会在函数内部又定义一个新的局部变量
# 而不是修改全局变量
# 函数内部如果想要修改全局变量？
# 使用global对变量进行声明,可以通过函数修改全局变量的值
# 内置函数globals()可以查看全局变量  Locals()可以查看局部变量

# 匿名函数 用来表达一个简单的函数，函数调用的次数很少,基本上就是调用一次
# 调用匿名函数两种方式：
# 1.给它定义一个名字(很少这样使用)
# 2.把这个函数当做参数传给另一个函数使用(使用场景比较多)
mul = lambda a, b: a * b
print(mul(4, 5))


# 回调函数 把一个函数当做另一个函数的参数
def calc(a, b, cals):
    c = cals(a, b)
    return c


x9 = calc(5, 3, lambda x, y: x + y)
print(x9)

'''
# 数学相关
abs()  # 取绝对值
divmod()  # 求两个数相除的商和余数
max()   # 获取最大值
min()   # 获取最小值

# 可迭代对象相关
all()  # 如果所有得元素转换成为布尔值都是True，结果是True，否则是Flase
any()  # 只要有一个元素转换成为布尔值是True，结果就是True
len() # 获取长度
iter # 获取可迭代对象的迭代器
next() # 本质是调用迭代器的next方法

# 转换相关
bin()  # 将数字转换成为二进制
chr()  # 将字符编码转换成为对应的字符串
ord()  # 将字符串转换成为对应的编码
eval()  # 执行字符串里的Python代码

# 变量相关
globals()  # 用来查看所有的全局变量
locals()  # 用来查看所有的局部变量
help() # 打印函数的帮助文档

# 判断对象相关的方法
isinstance() # 判断一个对象是否是由其中一个类创建的
issubclass() # 判断一个类是否是另一个类的子类

dir()  # 列对象所有的属性和方法
exit()  # 以指定的退出代码结束程序
'''


def foo():
    print('我是foo,我被调用了')
    return 'foo'


def bar():
    print('我是bar,我被调用了')
    return foo


# x =  bar()
# print('x的值是{}'.format(x))
# x()

y = bar()()
print(y)

'''
# 闭包 闭包里默认不能修改外部变量
def outer():
    x = 10  # 在外部函数里定义了一个变量x,是一个局部变量
    def inner():
        # x = 20
        # nonlocal x # 此时x不再是新增的变量，而是外部的局部变量x
        y = x + 1
    return inner
'''

# 时间戳是从1970-01-01 00:00:00 UTC 到现在的秒数
import time


def cal_time(fn):
    print('我是外部函数，我被调用了')
    print('fn = {}'.format(fn))

    def inner(x, *args, **kwargs):
        start = time.time()
        s = fn(x)
        end = time.time()
        print('代码运行耗时{}秒'.format(end - start))
        return s

    return inner


# 装饰器的使用 开闭原则
# 首先调用cal_time;把被装饰的函数传递给fn;
@cal_time
def demo(n):
    x = 0
    for i in range(1, n):
        x += i
    print(x)


# 当再次调用demo函数时，才是demo函数已经不再是上面的demo
print('装饰后的demo = {}'.format(demo))
demo(1000)

# TODO ======================================== python异常处理机制 ========================================
print('======================================== python异常处理机制 ========================================')
# 最终 try…except…else…finally结构
# try:
#     a = int(input('请输入第一个整数'))
#     b = int(input('请输入另一个整数'))
#     res = a / b
# except BaseException as e:
#     print('出错了', e)
# else:
#     print('结果为:', res)
# finally:
#     print('无论是否产生异常，总会被执行的代码')

'''
使用traceback模块打印异常信息

import traceback
try:
    print('1.-------------')
    print(1/0)
except:
    traceback.print_exc()
'''

'''
python常见的异常类型：
ZeroDivisionError	除零or向零取余
IndexError	序列中没有此索引
KeyError	映射中没有这个键
NameError	未声明or未定义
SyntaxError	语法错误
ValueError	传入无效的参数
'''

# TODO ======================================== 类与对象 ========================================
print('======================================== 类与对象 ========================================')
'''
类属性: 类中方法外的变量称为类属性，被该类的所有实例对象共享
类方法: 使用@classmethod修饰的方法，可使用类名直接访问
静态方法: 使用@staticmethod修饰的方法，可使用类名直接访问

面向对象的三大特征
封装 （提高程序的安全性）
将数据（属性）和行为（方法）封装到类对象中，在方法内部对属性进行操作，在类对象的外部调用方法。
这样无需关心方法内部的具体实现细节，从而隔离了复杂度。
在python中没有专门的修饰符用户属性的私有化，如果该属性不想被类对象访问，可以在属性名前面加两个 ‘_’

继承

多态
'''


# 类的创建
# Stubent为类的名称，由一个或多个单词组成。（建议类名书写方式为每个单词首字母大写）
class Student:
    native_pace = '河南'  # 直接写在类里的变量，称为属性

    # 这个属性直接定义在类里，是一个元组，用来规定对象可以存在的属性
    # __slots__ = ('name','age')

    # 初始化函数( 构造函数 )
    def __init__(self, name, age):
        # 创建实例的时候 对name和age属性进行赋值
        self.name = name
        self.age = age

    # 定义在类里面的方法为实例方法
    def eat(self):
        print('吃饭')

    # 静态方法 （静态方法里不允许写self）
    @staticmethod
    def method():
        print('使用staticmethod进行修饰，所以我是静态方法')

    # 类方法
    @classmethod
    def cm(cls):
        print('使用calssmethod进行修饰，所以我是类方法')


# 类实例创建语法  stu = Student()
# 假设已存在上面的 Student 类
# stu1就是Student类的实例对象，该实例对象中可以访问到类中的属性和方法
# 1. 调用 __new__ 方法，用来申请内存空间
# 2. 调用 __init__ 方法，并让self指向申请好的那段内存空间
# 3. 让stu1也指向创建好的内存空间
stu1 = Student('cgy', 18)  # 创建实例
print(stu1.name)  # cgy
print(stu1.age)  # 18
stu1.eat()  # 吃饭

# 动态绑定属性和方法
# 绑定 gender 属性
stu1.gender = '男'
print(stu1.gender)  # 男


# 绑定 show 方法
def show():
    print('show方法执行')


stu1.show = show
stu1.show()  # show方法执行


# 继承
class Person(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age
        self.__money = 1000  # 以两个下划线开始的变量时私有变量

    def info(self):
        print(self.name, self.age)


class Employee(Person):
    def __init__(self, name, age, stu_no):
        super().__init__(name, age)
        self.stu_no = stu_no


class Teacher(Person):
    def __init__(self, name, age, teachofyear):
        super().__init__(name, age)
        self.teachofyear = teachofyear

    @staticmethod  # 静态方法 如果一个方法没有用到实例对象的任何属性，可以将这个方法成static
    def demo_01():
        print('hello')

    @classmethod  # 如果这个函数只用到了类属性，我们可以把定义成为一个类方法
    def demo_02(cls):
        # 类方法会有一个参数 cls，也不需要手动的传参，会自动传参
        # cls 指的是类对象  cls is Teacher
        print(cls.type)
        print('yes')

p = Person('大佬', 12)
stu = Employee('张三', 20, 10001)
teacher = Teacher('李四', 35, 10)
print(p._Person__money)  # 通过这种方式也能获取到私有属性
# 也可以定义 set get方法获取
# 使用property来获取

stu.info()  # 张三 20
teacher.info()  # 李四 35
Employee.info(stu)  # 类对象调用

# python 支持多继承
'''
pass语句
语句什么都不做，只是一个占位符，用在语法上需要语句的地方
什么时候使用：
先搭建语法结构，还没想好代码怎么写的时候和哪些语句一起使用:
if 语句的条件执行体
for-in 语句的循环体
定义函数时的函数体

class A(class1, class2):
    pass
'''

# 方法重写
# 方法重写就是在子类里定义与父类重名的方法，这样就会优先调用子类的定义的方法

'''
特殊方法和特殊属性
开头和结尾都一样，方法名都是系统规定好的，在合适的实际自己调用

特殊属性
__dict__ 获得类对象或实例对象所绑定的所有属性和方法的字典
__len__()  通过重写__len__()方法,让内置函数len ()的参数可以是自定义类型
特殊方法
__add__() 通过重写__add__()方法,可使用自定义对象具有“+”功能
__new__() 用于创建对象
__init__() 1对创建的对象进行初始化

当打印一个对象的时候,会调用这个对象的__ str_ 或者_repr_ 方法 相当于java的toString
如果两个方法都写了，选择__str__
print(p)
'''

# is身份运算符可以用来判断两个对象是否是同一个对象
# is比较两个对象的内存地址
# == 会调用__eq__方法，获取这个方法的比较结果;如果不重写 默认比较的是内存地址 比较值


# 单例设计模式



# TODO ======================================== 模块和包 ========================================
print('======================================== 模块和包 ========================================')
# 每一个.py文件就是一个模块
# 不是所有的py文件都能作为一个模块来导入
# 如果想要让一个py文件能够被导入，模块名字必须要遵守命名规则
# 导入模块
# import 模块名称 [as 别名]

# 导入模块中的指定函数(or 变量 or 类)
# from 模块名称 import 函数/变量/类
# from 模块名称 import *  # 不再需要使用模块名
# 本质是读取模块里的__all__属性，看这个属性里定义了哪些变量和函数
# 如果模块里没用定义__all__才会导入所有不以_开头的变量和函数

# 以一个下划线开始变量，建议只在本模块里使用，别的类模块不要导入
_age = 19  # 使用from 模块名 import * 这种方式无法导入

del _age  # 可以在后面自己删除

# __name__：当直接运行这个py文件的时候，值是__main__
# 如果这个py文件作为一个模块导入的时候，值是文件名
print('demo里的name是：', __name__)

# 包是一个分层次的目录结构，它将一组功能相近的模块组织在一个目录下
# import 包名.模块名

# 第三方模块的安装和使用
# 安装：打开命令行程序 输入 pip install 模块名

'''
python内置模块
使用import关键字引入
random	随机数生成
traceback	处理异常
sys	与python解释器及环境操作相关的标准库
time	提供与时间相关的各种函数的标准库
os	提供了访问操作系统服务功能的标准库
calendar	提供与日期相关的各种函数的标准库
urllib	用于读取来自网上（服务器）数据的标准库
json	用于使用JSON序列化和反序列化对象
re	用于在字符串中执行正则表达式匹配和替换
math	提供标准算术运算函数的标准库
decimal	用于进行精准控制运算精度、有效数位和四舍五入操作的十进制运算
logging	提供了灵活的记录事件、错误、警告和调试信息等日志信息的功能
'''

import os

print(os.name)  # 操作系统的名字 windows系列=nt 非windows=posix
print(os.sep)  # 路径的分隔符 windows  \  非windows /

import sys

print(sys.path)  # 结果是一个列表，表示查找模块的路径
sys.stdin  # 接受用户的输入，和input 相关
# sys.stdout # 改变默认输出位置

import math

print(math.floor(12.98))  # 向下取整
print(math.ceil(15.001))  # 向上取整

import random

print(random.randint(2, 9))  # 用来生成[a,b]的随机整数
print(random.randrange(2, 9))  # 用来生成[a,b)的随机整数
print(random.random())  # 用来生成[0,1)的随机浮点数
print(random.choice(['zhangsan', 'luxi', 'jane']))  # choice 用来在可迭代对象里最随机抽取一个数据
print(random.sample(['zhangsan', 'luxi', 'jane'], 2))  # sample 用来在可迭代对象里最随机抽取 n 个数据

import datetime

print(datetime.datetime.now())  # 获取当前的日期时间
print(datetime.date(2020, 1, 1))  # 创建一个日期
print(datetime.time(18, 23, 45))  # 创建一个时间
print(datetime.datetime.now() + datetime.timedelta(3))  # 计算三天以后的日期时间

import time

print(time.time())  # 获取1970-01-01 00:00:00 YTC 到现在的时间秒数
print(time.strftime("%Y-%m-%d %H:%M:%S"))  # 按照指定格式输出时间
print(time.asctime())  # Mon Apr 15 20:03:23 m2019
print(time.ctime())  # Mon Apr 15 20:03:23 m2019
print(time.sleep(10))  # 让线程暂停10秒钟

import calendar

print(calendar.calendar(2021))  # 生成2020的日历，并且以周日为其实日期码

# 这两个模块都是用来进行数据机密
# hashlib模块里主要支持两个算法 md5 和 sha 加密
# 加密方式：单向加密:只有加密的过程，不能解密nd5/sha 对称加密 非对称加密rsa
import hashlib
import hmac

# 需要将要加密的内容转换成为二进制
x = hashlib.md5()  # 生成一个md5对象
x.update('abc'.encode('utf8'))
print(x.hexdigest())

h1 = hashlib.sha1('123456'.encode())
print(h1.hexdigest())
h2 = hashlib.sha224('123456'.encode())
print(h2.hexdigest())

# hmac 加密可以指定密钥
# h = hmac.new('h'.encode(), '你好'.encode())  # ’h‘ 是密钥
# result = h.hexdigest()
# print(result)  # 获取加密后的结果

import uuid  # 生成一个全局唯一的id

print(uuid.uuid1())  # 32个长度  每一个字符有16个选择 16**32
print(uuid.uuid3(uuid.NAMESPACE_DNS, 'zhangsan'))  # 生成固定的uuid
print(uuid.uuid5(uuid.NAMESPACE_DNS, 'zhangsan'))  # 生成固定的uuid
print(uuid.uuid4())  # 通过伪随机数得到uuid 可能重复 使用得最多

'''
pip install <package_name> 下载第三方模块
pip uninstall <package_name> 删除第三方模块
pip list 用来列出当前环境安装的模块名和版本号
pip freeze 用来列出当前环境安装的模块名和版本号

一般比较常用
pip freeze > requirements.txt  将安装的模块名和版本号重定向输出到指定的文件
pip install -r requirements.txt 读取文件里模块名和版本号并安装

要是用阿里云服务器 默认从阿里云镜像下载
临时修改 只修改这一个文件的下载路径
pip install <package_name> -i <url> 从指定的地址下载包
'''

'''

'''

# TODO ======================================== 文件的读写 ========================================
print('======================================== 文件的读写 ========================================')
# file = open( filename, [mode, encoding] )
file = open('hello1.txt', 'r')
print(file.readlines())  # ['hello world']
file.close()

'''
常见的字符串编码格式
Python的解释器使用的是 Unicode（内存）
.py 文件在磁盘上使用 UTF-8 存储（外存）
'''

'''
常用的文件打开模式
打开模式	描述
r	以只读模式打开文件，文件的指针将会放在文件的开头
w	以只写模式打开文件，如果文件不存在则创建，如果文件存在，则覆盖原有内容，文件指针在文件的开头
a	以追加模式打开文件，如果文件不存在则创建，文件指针在文件开头，如果文件存在，则在文件末尾追加内容
b	以二进制方式打开文件，不能单独使用，需要与其他模式一起使用，rb,后者wb
+	以读写方式打开文件，不能单独使用，需要与其他模式一起使用，如a+

文件对象的常用方法
方法名	描述
read([size])	从文件中读取size个字节或字符的内容返回，若省略size，则读取到文件末尾，即一次读取文件所有内容
readline()	从文本文件中读取一行内容
readlines()	把文本文件中每一行都作为独立的字符串对象，并将这些对象放入列表返回
write(str)	将字符串str内容写入文件
writelines(s_list)	将字符串列表s_list写入文本文件，不添加换行符
seek(offset[, whence])	把文件指针移动到新的位置，offset表示相对与whence的位置：
offset：为正则往结束方向移动，为负则往开始方向移动
whence不同的值代表不同含义：
0：从文件头开始计算（默认值）
1：从当前位置开始计算
2：从文件末尾开始计算
tell()	返回文件指针的当前位置
flush()	把缓冲区的内容写入文件，但不关闭文件
close()	把缓冲区的内容写入文件，同时关闭文件，释放文件对象相关资源
'''

'''
with语句（上下文管理器）
with语句可以自动管理上下文资源，不论什么原因跳出with语句都能确保文件正确的关闭，以此来达到释放资源的目的

with open(filename, mode) as file:
    pass 
'''

'''
目录操作 （os 和 os.path）   
os模块是python内置的与操作系统功能和文件系统相关的模块，该模块中的语句的执行结果通常与操作系统有关，在不同的操作系统上运行，得到的结果可能不一样
os模块与os.path模块用于对目录或文件进行操作

# os模块操作目录相关函数
getcwd()	返回当前的工作目录
listdir(path)	返回指定路径下的文件和目录信息
mkdir(path[, mode])	创建目录
makedirs(path1/path2…[, mode])	创建多级目录
rmdir(path)	删除目录
removedirs(path1/path2…)	删除多级目录
chdir(path)	将path设置为当前工作目录

# os.path模块操作目录相关函数
abspath(path)	用于获取文件或目录的绝对路径
exists(path)	用于判断文件或目录是否存在，如果存在返回True,否则返回False
join(path, name)	将目录与目录，或者目录与文件名连接起来
split(path)	分离目录和文件名
splittext(name)	分离文件名和扩展名
basename(path)	从一个目录中提取文件名
dirname(path)	从一个路径中提取文件路径，不包括文件名
isdir(path)	判断是否为路径
'''
import os

os.system('calc.exe')  # 打开计算器
os.system('notepad.exe')  # 打开记事本
os.startfile('F:\\6400 作品')  # 打开可执行文件 路径

# TODO ======================================== 其它 ========================================
print('======================================== 其它 ========================================')

'''
变量的作用域
程序代码能访问该变量的区域
根据变量的有效范围可分为
局部变量
在函数内定义并使用的变量，只在函数内部有效，局部变量使用 global 声明，这个变量就会变成全局变量
全局变量
函数体外定义的变量，可以用于函数内外
'''

'''
# 字符串驻留机制(仅了解):
仅保存一份相同且不可变字符串的方法，不同的值被存放在字符串的驻留池中，Python的驻留机制对相同字符串只保留一份拷贝，
后续创建相同字符串时，不会开辟新空间，而是把该字符串的地址赋给新创建的变量。
'''
a = 'python'
b = "python"
c = '''python'''
print(id(a))  # 1722934638960
print(id(b))  # 1722934638960
print(id(c))  # 1722934638960
# 字符串只在编译时进行驻留，而非运行时
str = 'anonymous'
str1 = 'anony' + 'mous'
print(id(str))  # 1419843712688
print(id(str1))  # 1419843712688 与str地址相同
str2 = ''.join(['anony', 'mous'])
print(id(str2))  # 1419843713264 #与str地址不相同
'''
PyCharm 对字符串进行了优化处理
字符串驻留机制的优缺点
当需要值相同的字符串时，可以直接从字符串池里拿来使用，避免频繁的创建和销毁，提升效率和节约内存，因此拼接字符串和修改字符串是会比较影响性能的。
在需要进行字符串拼接是建议使用 str 类型的的 join 方法，而非 + ，因为 join() 方法是先计算出所有字符中的长度，然后再拷贝，只是 new 一次对象，效率要比 + 效率高。
'''

'''
类的浅拷贝与深拷贝
变量的赋值操作
只是形成两个变量，实际上还是指向同一个对象
浅拷贝
Python拷贝一般都是钱拷贝，拷贝时，对象包含的子对象内容不拷贝。因此，源对象与拷贝对象会引用同一个子对象
深拷贝
使用 copy 模块的 deepcopy 函数，递归拷贝对象中包含的子对象，源对象和拷贝对象所有的子对象也不相同
'''


class CPU:
    pass


class Disk:
    pass


class Computer:
    def __init__(self, cpu, disk):
        self.cpu = cpu
        self.disk = disk


# 变量的赋值
cpu = CPU()  # 等号 是内存地址的赋值 指向了同一个内存空间
cpu1 = cpu
print(cpu, id(cpu))  # <__main__.CPU object at 0x000002D562B1CFD0> 3115507109840
print(cpu1, id(cpu1))  # <__main__.CPU object at 0x000002D562B1CFD0> 3115507109840

disk = Disk()
print(disk)  # <__main__.Disk object at 0x000001E192E8DF70>
computer = Computer(cpu, disk)

# 浅拷贝
import copy

computer1 = copy.copy(computer)  # 等于 computer1 = computer.copy()
print(computer, computer.cpu,
      computer.disk)  # <__main__.Computer object at 0x000001E192E8DE50> <__main__.CPU object at 0x000001E192E8DFD0> <__main__.Disk object at 0x000001E192E8DF70>
print(computer1, computer1.cpu,
      computer1.disk)  # <__main__.Computer object at 0x000001E192E8DD60> <__main__.CPU object at 0x000001E192E8DFD0> <__main__.Disk object at 0x000001E192E8DF70>

# 深拷贝
computer1 = copy.deepcopy(computer)
print(computer, computer.cpu,
      computer.disk)  # <__main__.Computer object at 0x000002302CF9DE50> <__main__.CPU object at 0x000002302CF9DFD0> <__main__.Disk object at 0x000002302CF9DF70>
print(computer1, computer1.cpu,
      computer1.disk)  # <__main__.Computer object at 0x000002302CF9DC70> <__main__.CPU object at 0x000002302CF9D460> <__main__.Disk object at 0x000002302CF9D490>


# 计算阶乘的递归算法
def fac(n):
    if n == 1:
        return 1
    else:
        return n * fac(n - 1)


print('计算阶乘的递归算法:', fac(6))


# 斐波那契数列的递归算法
def fib(n):
    if n == 1: return 1
    if n == 2:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


print('斐波那契数列的递归算法:', fib(6))


# 输出斐波那契数列的前n个值
def fibn(n):
    for i in range(1, n + 1):
        print(fib(i))


print('输出斐波那契数列的前n个值:', fibn(8))

# filter 对可迭代对象进行过滤，得到的是一个filter对象
# Python2的时候是内置函数，Python3修改成了一个内置类
# filter可以给定两个参数，第一个参数第函数，第二个参数是可迭代对象，结果是一个filter 类型的对象
ages = [12, 23, 38, 28, 16, 22]
x = filter(lambda ele: ele > 8, ages)
print(x)

m = map(lambda ele: ele * 2, ages)
print(m)

from functools import reduce

print(reduce(lambda ele1, ele2: ele1 + ele2, ages))

# TODO ======================================== 进制 ========================================
print('======================================== 进制 ========================================')

# 整型就是整数。计算机其实只能保存二进制 0 和 1，为了方便数据的表示，同时计算机也支持八进制和十六进制
# 二进制几进制 十六进制 十进制 在Python里都能够表示
'''
整数类型(int)
十进制(默认)
二进制 -> 以0b开头
八进制 -> 以0o开头
十六进制 -> 以0x开头
'''

a = 98  # 默认数字都是十进制的数字。98 就是十进制的九十八

b = 0b101101101  # 以0b开头的数字是二进制 二进制里最大的个位数是1，不能出现2
print(b)  # 当使用print语句打印一个数字的时候，默认也是使用十进制打印输出的  365

c = 0o34  # 以0o开头的数字是八进制的数字 八进制里最大的个位数是7
print(c)  # 28

d = 0x2a  # 以0x开头的数字是十六进制 0~9 a~f
print(d)  # 42

# 手算 十进制 转 二进制 不断除取余数 从末尾开始排
print(bin(a))  # 转为二进制
print(oct(a))  # 转为八进制
print(hex(a))  # 转为十六进制

x = '1a2c'
y = int(x, 16)  # 把字符1a2c 当做+六进制转换成为整数'
print(y)  # 6700 打印—个数字, 默认使用十进制输出

# TODO ======================================== 输入、输出 ========================================
print('======================================== 输入、输出 ========================================')

# inp = input('请输入：')  # 输入结果为str数据类型，如须做其他操作可进行数据转换 强转
