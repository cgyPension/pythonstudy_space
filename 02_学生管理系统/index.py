import password as password

import file_manager
import bean

import student_manager

def start():
    content = file_manager.read_file('files/welcome.txt')
    while True:
        operator = input(content + '\n请选择(1-3)：')
        if operator == '1':
            print('登陆')
        elif operator == '2':
            print('注册')
            register()
        elif operator == '3':
            print('退出')
            exit(0)
        else:
            print('输入有误')


if __name__ == "__main__":
    start()


# teacher = {}


def register():
    # 读取teacher.json
    data = file_manager.read_file('files/teacher.json')
    while True:
        teacher_name = input('请输入账号（3~6位）：')
        if not 3 <= len(teacher_name) <= 6:
            print('账号不符合，请重新输入！')
        else:
            break

    if teacher_name in data:
        print('注册失败！该账号已经注册过！')
        return

    while True:
        password = input('请输入密码（6~12位）：')
        if not 6 <= len(password) <= 12:
            print('密码不符合，请重新输入！')
        else:
            break

    # teacher[teacher_name] = password
    t = bean.Teacher(teacher_name, password)
    data[t.name] = t.password
    file_manager.write_json('files/teacher.json', data)


def login():
    # 读取teacher.json
    data = file_manager.read_file('files/teacher.json')
    teacher_name = input('请输入老师账号：')
    if teacher_name not in data:
        print('登陆失败！该账号没有注册过！')
        return

    password = input('请输入密码：')


    import tools
    if data[teacher_name] == tools.encrypt_password(password):
        student_manager.name = teacher_name
        student_manager.show_manager()
    else:
        print('密码错误')