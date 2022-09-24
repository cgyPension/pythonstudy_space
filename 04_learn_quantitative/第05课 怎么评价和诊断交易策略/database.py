#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""


from pymongo import MongoClient

# 指定数据库的连接，quant_01是数据库名
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_01']
