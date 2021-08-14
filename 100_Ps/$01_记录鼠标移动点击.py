# 记录鼠标移动点击操作，并写入至本地 txt 文件中
import pyautogui
import time
from pynput import mouse
from pynput.mouse import Button
import threading

MymouseX = []
MymouseY = []

time.sleep(4)

def on_move(x, y):
    print('Pointer moved to {o}'.format((x,y)))

def on_click(x, y , button, pressed):

    if pressed:
        #print('{0} Pressed at {1} at {2}'.format(button_name, x, y))

        MymouseX.append(10086)
        MymouseY.append(10086)

        with open("MouseX.txt", "w") as f:
             f.write(str(MymouseX))

        with open("MouseY.txt", "w") as f:
             f.write(str(MymouseY))

def myfun():
     mouse.Listener()
    with mouse.Listener( no_move = on_move,on_click = on_click,suppress = False) as listener:
        #print("0")
        listener.join()


def muisc(func):
     while True:
          # 加上time就可以输出速度不那么快了
          time.sleep(0.1)
          mouseX, mouseY = pyautogui.position()
          MymouseX.append(mouseX)
          MymouseY.append(mouseY)
          # MymouseY = MymouseY = list(mouseY)
          print(MymouseX, MymouseY)

          with open("MouseX.txt", "w") as f:
               f.write(str(MymouseX))

          with open("MouseY.txt", "w") as f:
               f.write(str(MymouseY))

def move(func):
    while True:
        #print('Start playing： move')
        myfun()


def player(name):
    r = name.split('.')[1]
    if r == 'mp3':
        muisc(name)
    elif r == 'mp4':
        move(name)
    else:
        print('error: The format is not recognized!')

list = ['爱情买卖.mp3', '阿凡达.mp4']

threads = []
files = range(len(list))

# 创建线程
for i in files:
    t = threading.Thread(target=player, args=(list[i],))
    threads.append(t)

if __name__ == '__main__':
    # 启动线程
    for i in files:
        threads[i].start()
    for i in files:
        threads[i].join()

    # 主线程
    print("好了，线程开始了")