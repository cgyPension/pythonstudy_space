import numpy as np


def test_corrcoef():
    '''
    相关系数是用以反映变量之间相关关系密切程度的统计指标。相关系数也可以看成协方差：一种剔除了两个变量量纲影响、
    标准化后的特殊协方差,它消除了两个变量变化幅度的影响，而只是单纯反应两个变量每单位变化时的相似程度
    结果矩阵的行数*结果矩阵的列数==矩阵1的行数*矩阵2的行数
    :return:
    '''
    Array1 = [[1, 2, 3], [4, 5, 6]]
    Array2 = [[11, 25, 346], [734, 48, 49]]
    Mat1 = np.array(Array1)
    Mat2 = np.array(Array2)
    correlation = np.corrcoef(Mat1, Mat2)
    print("矩阵1=\n", Mat1)
    print("矩阵2=\n", Mat2)
    print("相关系数矩阵=\n", correlation)

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
    test_corrcoef()
