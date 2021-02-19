import socket
import threading

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(('192.168.31.199', 8080))


# data的数据类型是一个元组
# 元组里第0个元素是接收到的数据
# 元组里第1个元素是发送方的ip地址和端口号
def send_msg():
    while True:
        msg = input('请输入您要发送的内容')
        s.sendto(msg.encode('utf-8'), ('192.168.31.199', 9090))
        if msg == 'exit':
            break


def recv_msg():
    while True:
        data, addr = s.recvfrom(1024)
        x = open('消息记录.txt', 'a', encoding='utf-8')
        print('接收到了{}客户端{}端口号发送的数据,内容是:'.format(addr[0], addr[1], data.decode('utf-8')), file=x)
        # file.write('接收到了{}客户端{}端口号发送的数据,内容是:'.format(addr[0], addr[1], data.decode('utf-8')))


if __name__ == "__main__":
    t1 = threading.Thread(target=send_msg)
    t2 = threading.Thread(target=recv_msg)

    t1.start()
    t2.start()

    s.close()
