#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import jieba
import jidba.analyse  # 导入提取关键词的库

# 对训练集 测试集文本都进行切词处理，对测试集数据打上主题标签
