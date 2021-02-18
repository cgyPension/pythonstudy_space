import bean

import file_manager

name = ''


def show_manager():
    content = file_manager.read_file('files/students_page.txt') % name
    print(content)

    while True:
        operator = input('请选择（1~5）：')
        if operator == '1':
            add_student()
        elif operator == '2':
            show_student()
        elif operator == '3':
            modify_student()
        elif operator == '4':
            delte_student()
        elif operator == '5':
            break
        else:
            print('输入有误')


def add_student():
    x = file_manager.read_json('files/' + name + '.json')
    if not x:  # 如果文件不存在
        students = []
        num = 0
    else:
        students = x['all_student']
        num = int(x['num'])

    while True:
        s_name = input('请输入学生姓名：')
        s_age = input('请输入学生年龄：')
        s_gender = input('请输入学生性别：')
        s_tel = input('请输入学生电话：')

        num = int(x['num'])
        num += 1
        s_id = 'stu_' + str(num).zfill(4)  # zfill方法，不够n位 在字符串前面补0

        s = bean.Student(s_id, s_name, s_age, s_gender, s_tel)
        students.append(s.__dict__)  # 转为字典
        data = {'all_student': students, 'num': len(students)}
        file_manager.write_json('files/' + name + '.json', data)
        chioce = input('添加成功！\n1.继续\n2.返回\n请选择（1-2）：')
        if chioce == '2':
            break


def show_student():
    x = input('1.查看所有学生\n2.根据姓名查找\n3.根据学号查找\n其他：返回\n请选择：')
    y = file_manager.read_json('files/' + name + '.json')
    # if not y:
    #     students = []
    #     num = 0
    # else:
    #     students = x['all_student']
    #     num = int(x['num'])

    students = y.get('all_student', [])
    if not students:
        print('该老师还没有学员，请添加学员')
        return

    num = y.get('num', 0)

    if x == '1':
        pass
    elif x == '2':
        s_name = input('请输入学生姓名：')
        # same_name_students = []
        # for student in students:
        #     if student['name'] == s_name:
        #         same_name_students.append(student)

        # filter结果是一个filter类，它是一个可迭代对象
        students = filter(lambda s: s['name'] == s_name, students)
    elif x == '3':
        s_id = input('请输入学生id：')
        students = filter(lambda s: s['s_id'] == s_id, students)
    else:
        print('输入有误')
        return

    if not students:
        print('该老师还没有学员，请添加学员')
        return
    for student in students:
        print('学号:{s_id}，姓名:{name}，性别:{gender}，年龄:{age}，电话:{tell}'.format(**student))


def modify_student():
    pass


def delte_student():
    x = input('1.按姓名删\n2.按学号删\n其他：返回\n请选择：')
    y = file_manager.read_json('files/' + name + '.json')

    all_students = y.get('all_student', [])
    if not all_students:
        print('该老师还没有学员，请添加学员')
        return

    num = y.get('num', 0)

    if x == '1':
        key = 'name'
        value = input('请输入要删除学生的姓名：')
    elif x == '2':
        key = 's_id'
        value = input('请输入要删除学生的id：')
    else:
        print('输入有误')
        return

    students = list(filter(lambda s: s.get(key, '') == value, all_students))
    if not students:
        print('没有找到对应的学生')
        return
    for i, student in enumerate(students):
        print('{x} 学号:{s_id}，姓名:{name}，性别:{gender}，年龄:{age}，电话:{tell}'.format(x=i, **student))

    n = input('请输入需要删除的学生的标号（0~{}）,q-返回：'.format(i))  # 使用i 可能有潜在风险

    if not n.isdigit() or not 0 <= int(n) <= i:
        print('输入的内容不合法')
        return

    # 将学生从all_students里删除
    all_students.remove(students[int(n)])
    y['all_student'] = all_students

    file_manager.write_json('files/' + name + '.json',y)