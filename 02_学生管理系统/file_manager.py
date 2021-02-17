# 读取文件
def read_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            return content
    except FileNotFoundError:
        print('文件没找到')


# 写入json
def write_json(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as file:
        import json
        json.dump(data, file)


# 读取json
def read_json(file_path, default_data={}):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            import json
            return json.load(file)
    except FileNotFoundError:
        # print('文件没找到')
        return default_data
