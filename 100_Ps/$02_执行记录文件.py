# 读取本地 txt 的文件内容，鼠标重复执行所记录的操作
import pyautogui
import time
pyautogui.PAUSE = 0.05
MymouseX = []
MymouseY = []

time.sleep(3)

with open("MouseX.txt", "r") as f:  # 打开文件
    MymouseX = f.read()  # 读取文件
    #print(MymouseX)

with open("MouseY.txt", "r") as f:  # 打开文件
    MymouseY = f.read()  # 读取文件
    #print(MymouseY)
MymouseX = MymouseX.strip("[")
MymouseX = MymouseX.strip("]")
MymouseX = MymouseX.split(", ")
MymouseY = MymouseY.strip("[")
MymouseY = MymouseY.strip("]")
MymouseY = MymouseY.split(", ")
#MymouseX = list(MymouseX)

#print(MymouseX)
#MymouseY = list(MymouseY)

#time.sleep(0.1)

mys = 0

while True:
    for s in MymouseX:
        #print(MymouseX[mys])
        #print(type(int(MymouseX[mys])))
        #print(mys)
        if int(MymouseX[mys]) == 10086:
            print("this is Click")
            pyautogui.mouseDown()
            pyautogui.mouseUp()
            print(pyautogui.position())
            mys += 1
        else:
            pyautogui.moveTo(int(MymouseX[mys]), int(MymouseY[mys]))
            mys += 1
    #重复执行
    mys = 0
