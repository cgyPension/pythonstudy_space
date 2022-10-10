#-*- coding:utf-8 -*-
import os
import sys
import time
from datetime import date
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from ods import ods_dc_stock_quotes_di, ods_163_stock_quotes_di, ods_dc_stock_tfp_di
from util.CommonUtils import get_code_list_v2, get_process_num
from util.DBUtils import sqlalchemyUtil


from apscheduler.schedulers.blocking import BlockingScheduler

def task_update_daily():
    """
    :return: None
    """
    code_list = get_code_list_v2()
    period = 'daily'

    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        run_type = sys.argv[1]
        if run_type == 'all':
            start_date = '20210101'
            end_date = date.today().strftime('%Y%m%d')
        else:
            start_date = date.today().strftime('%Y%m%d')
            end_date = start_date
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]

    adjust = "hfq"
    engine = sqlalchemyUtil().engine
    process_num = get_process_num()


    # 程序开始运行时间
    start_time = time.time()

    ods_dc_stock_quotes_di.multiprocess_run(code_list, period, start_date, end_date, adjust, engine, process_num)
    ods_163_stock_quotes_di.multiprocess_run(code_list, start_date, end_date, engine, process_num)
    ods_dc_stock_tfp_di.get_data(start_date, end_date, engine)

    # 程序结束运行时间
    end_time = time.time()
    print('程序运行时间：{}s'.format(end_time - start_time))



# BlockingScheduler: 调用start函数后会阻塞当前线程。当调度器是你应用中唯一要运行的东西时（如上例）使用。
# BackgroundScheduler: 调用start后主线程不会阻塞。当你不运行任何其他框架时使用，并希望调度器在你应用的后台执行。
# sched = BlockingScheduler()
# 时间： 周一到周五每天早上9点25, 执行run_today
# sched.add_job(task_update_daily, 'cron', day_of_week='mon-fri', hour=9, minute=25)
# sched.start()

# nohup python app.py update 20221010 20221010>> my.log 2>&1 &
if __name__ == '__main__':
    task_update_daily()