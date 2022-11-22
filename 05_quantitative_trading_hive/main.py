#-*- coding:utf-8 -*-
import os
import sys
import time
from datetime import date
# 在linux会识别不了包 所以要加临时搜索目录
from ads import ads_stock_suggest_di, AdsSendMail

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from dim import dim_dc_stock_plate_df
from dwd import dwd_stock_quotes_di
from ods import ods_dc_stock_quotes_di, ods_dc_stock_tfp_di, ods_lg_indicator_di, \
    ods_dc_stock_concept_plate_name_di, ods_dc_stock_industry_plate_name_di, ods_dc_stock_industry_plate_df, \
    ods_dc_stock_concept_plate_df, ods_stock_lrb_em_di, ods_financial_analysis_indicator_di, ods_stock_lhb_detail_em_di
from util.CommonUtils import get_code_list, get_process_num
from util.DBUtils import sqlalchemyUtil, hiveUtil


def task_update_daily():
    """
    :return: None
    """
    code_list = get_code_list()
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
    hive_engine = hiveUtil().engine
    process_num = get_process_num()


    # 程序开始运行时间
    start_time = time.time()

    ods_dc_stock_quotes_di.multiprocess_run(code_list, period, start_date, end_date, adjust,hive_engine,process_num)
    # 财务
    ods_lg_indicator_di.multiprocess_run(code_list, start_date, hive_engine, process_num)
    ods_stock_lrb_em_di.get_data(start_date, end_date)
    # 这个全量很慢 平时不能全量跑
    # ods_financial_analysis_indicator_di.multiprocess_run(code_list, start_date, hive_engine)

    # 其他
    ods_stock_lhb_detail_em_di.get_data(start_date, end_date)
    ods_dc_stock_tfp_di.get_data(start_date, end_date)

    # 板块
    ods_dc_stock_industry_plate_name_di.get_data()
    ods_dc_stock_concept_plate_name_di.get_data()
    ods_dc_stock_industry_plate_df.multiprocess_run(process_num)
    ods_dc_stock_concept_plate_df.multiprocess_run(process_num)
    dim_dc_stock_plate_df.get_data()

    dwd_stock_quotes_di.get_data(start_date, end_date)
    ads_stock_suggest_di.get_data(start_date, end_date)
    # AdsSendMail.get_data()


    # 程序结束运行时间
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))


# BlockingScheduler: 调用start函数后会阻塞当前线程。当调度器是你应用中唯一要运行的东西时（如上例）使用。
# BackgroundScheduler: 调用start后主线程不会阻塞。当你不运行任何其他框架时使用，并希望调度器在你应用的后台执行。
# sched = BlockingScheduler()
# 时间： 周一到周五每天早上9点25, 执行run_today
# sched.add_job(task_update_daily, 'cron', day_of_week='mon-fri', hour=9, minute=25)
# sched.start()

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/main.py update
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/main.py all
# nohup /opt/code/05_quantitative_trading_hive/main.py update 20221010 20221010 >> my.log 2>&1 &
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/main.py update 20221122 20221122
if __name__ == '__main__':
    task_update_daily()