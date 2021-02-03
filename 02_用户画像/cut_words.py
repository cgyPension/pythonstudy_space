#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import jieba
import jidba.analyse  # 导入提取关键词的库


# 对训练集 测试集文本都进行切词处理，对测试集数据打上主题标签

# 保存至文件
def save_file(save_path, content):
    with open(save_path, "a", encoding='utf-8', errors='ignore') as fp:
        fp.write(content)


# 读取文件
def read_file(file_path):
    with open(file_path, "r", encoding='utf-8', errors='ignore') as fp:
        content = fp.readline()
        # print(content)
    return str(content)


# 抽取测试集的主题关键词
def extract_theme(content):
    themes = []
    tags = jieba.analyse.extract_tags(content,topK=3,withWeight=True,allowPOS=['n','ns','v','vn'],withFlag=True)
    for i in tags:
        themes.append(i[0].word)
    return str(themes)

