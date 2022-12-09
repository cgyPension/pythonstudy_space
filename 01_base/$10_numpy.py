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

def test_log():
    '''
    如果a的x次方等于N（a>0，且a≠1），那么数x叫做以a为底N的对数（logarithm），记作x=loga N。其中，a叫做对数的底数，N叫做真数。 [1]
    numpy.log()是一个数学函数, 用于计算x(x属于所有输入数组元素)的自然对数。它是指数函数的倒数, 也是元素自然对数。
    自然对数对数是指数函数的逆函数, 因此log(exp(x))= x。以e为底的对数是自然对数。
    '''
    print(np.log2(8))
    # 不写底数时默认以e为底
    print(np.log([1, np.e, np.e**2, 0]))
    print(np.log([1,2,3,4,7.8,7,8]))
    print(np.log2([1,2,3,8]))
    print(np.log10([1,2,3,8]))

def test_exp():
    # exp：高等数学里以自然常数e为底的指数函数
    # numpy.exp()：返回e的幂次方，e是一个常数为2.71828
    print(np.exp(1))
    print(np.exp(2))


def test_column_stack():
    '''列合并为矩阵：np.column_stack()'''
    a = np.array((1, 2, 3))
    b = np.array((2, 3, 4))
    ab = np.column_stack((a, b))
    # 行合并为矩阵 np.row_stack
    c = np.row_stack((a, b))
    print(ab,type(ab))
    print(c,type(c))





def test_clip():
    '''
    一般用来做极值处理 给定一个区间，区间外的值被裁剪到区间边缘
    numpy.clip(a, a_min, a_max, out=None)[source]
    '''
    # 将数组中的元素限制在a_min, a_max之间，大于a_max的就使得它等于 a_max，小于a_min,的就使得它等于a_min
    x = np.array([1, 2, 3, 5, 6, 7, 8, 9])
    print(np.clip(x, 3, 8))
    z = np.array([[1, 2, 3, 5, 6, 7, 8, 9], [1, 2, 3, 5, 6, 7, 8, 9]])
    print(np.clip(z, 3, 8))

def test_xxx():
    pass

def test_xxx():
    pass

def test_xxx():
    pass

if __name__ == '__main__':
    test_clip()
