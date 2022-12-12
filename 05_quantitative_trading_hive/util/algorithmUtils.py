import os
import sys
import pandas as pd
import numpy as np
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)


def max_drawdown(df):
    '''计算最大回撤
        df[name] 传入收盘价
    '''
    md=((df.cummax()-df)/df.cummax()).max()
    return round(md*100,2)

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

if __name__ == '__main__':
     test_xxx()