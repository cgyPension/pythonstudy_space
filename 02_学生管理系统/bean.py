import tools


class Teacher(object):
    def __init__(self, name, password):
        self.name = name
        self.password = tools.encrypt_password(password)
