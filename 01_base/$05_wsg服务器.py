import json
from wsgiref import simple_server
from wsgiref.simple_server import make_server


def load_html(file_name,**kwargs):
    try:
        with open(file_name,'r',encoding='utf-8') as file:
            content = file.read()
            if kwargs: # kwargs = {'username ': 'zhangsan ', 'age':19, 'gender': 'male'}
                content = content.format(**kwargs)
                # {username}, 迎回来,你今年{age}岁,你的性别是(gender}.format(**kwargs)
            return content
    except FileExistsError:
        print('文件未找到')


# 第1个参数，表示环境（电脑的环境；请求路径相关的环境）
# 第2个参数，是一个函数，用来返回响应头
# 这个函数需要一个返回值，返回值是一个列表
# 列表里有一个元素，是一个二进制，表示返回给浏览器的数据
def demo_app(environ,start_response):
    print(environ)  # environ是一个字典，保存了很多数据
    # 其中重要的一个是 PATH_INFO 能够获取到用户的访问路径
    path = environ['PATH_INFO']
    # print(environ. get('QUERY-STRING')) # QUERY_STRING ==> 获取到客户端GET请求方式传递的参数
    # POST 请求数据的方式

    # 状态码: RESTFUL ==> 前后端分离
    # 2xx：请求响应成功
    # 3Xx:重定向
    # 4xx：客户端的错误。404客户端访问了一个不存在的地址 405:请求方式不被允许
    # 5xx：服务器的错误。
    status_code = '200 OK'  # 默认状态码
    if path == '/':
        response = '欢迎来到我的首页'
    elif path == '/test':
        response = json.dumps({'name':'zhagnsna','age':18})
    elif path == '/demo':
        with open('hello1.txt','r',encoding='utf-8') as file:
            response = file.read()
    elif path == '/info':
        # html 文件查询数据库，获取用户名
        name = 'jack'
        with open('info.html','r',encoding='utf-8') as file:
            # '{username}，欢迎回来'.format(username=name)
            # flask django 模块，渲染引擎
            response = file.read().format(username=name,age=18,gender='男')
    else:
        status_code = '404 Not Found'
        response = '页面走丢了'

    print('path={}'.format(path))
    start_response(status_code, [('Content-Type', 'text/plain; charset=utf-8'),('Connection', 'keep-alive')])
    return [response.encode("utf-8")]  # 浏览器显示的内容


if __name__ == '__main__':
    # demo_app 是一个函数，用来处理用户的请求
    httpd = make_server('', 8000, demo_app)
    sa = httpd.socket.getsockname()
    print("Serving HTTP on", sa[0], "port", sa[1], "...")
    # 代码的作用是打开电脑的浏览器，并在浏览器里输入 http://localhost:8000/xyz?abc
    # import webbrowser
    # webbrowser.open('http://localhost:8000/xyz?abc')
    # httpd.handle_request()  # 只处理一次请求
    httpd.serve_forever()  # 服务器在后台一直运行