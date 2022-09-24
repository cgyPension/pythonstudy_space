#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""

import schedule
from daily_crawler import DailyCrawler
import time
from datetime import datetime

"""
每天下午15:30执行抓取，只有周一到周五才真正执行抓取任务
"""


def crawl_daily():
    """
    每日的定时抓取
    """

    # 初始化抓取日线数据类
    dc = DailyCrawler()
    # 获取当前时间
    now_date = datetime.now()
    # 获取今天星期几，周日-周六：0-6
    weekday = int(now_date.strftime('%w'))
    # 只有周一到周五执行
    if 0 < weekday < 6:
        # 当前日期
        now = now_date.strftime('%Y-%m-%d')
        # 抓取当日指数
        dc.crawl_index(begin_date=now, end_date=now)
        # 抓取当日K线
        dc.crawl(begin_date=now, end_date=now)


# 定时任务的启动入口
if __name__ == '__main__':
    # 设定每天下午15:30执行抓取任务
    schedule.every().day.at("15:30").do(crawl_daily)
    # 通过无限循环，执行任务检测
    while True:
        # 每10秒检测一次
        schedule.run_pending()
        time.sleep(10)
