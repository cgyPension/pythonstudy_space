import hashlib


def encrypt_password(password, x='assoijweg'):
    h = hashlib.sha256()
    h.update(password.encode('utf-8'))
    return h.hexdigest()
