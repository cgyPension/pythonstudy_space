import file_manager

name = ''

def show_manager():
    content = file_manager.read_file('files/students_page.txt') % name
    print(content)

    while True:
        input('请选择（1~5）：')
