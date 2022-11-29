import os
import sys
from pyspark.sql.functions import udf
from pyspark.sql.types import *
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)


def registerUDF(spark):
    '''批量注册udf'''
    udf = spark.udf
    udf.register('pow1', pow1, returnType=DoubleType())


def pow1(m, n):
    return float(m) ** float(n)

