import os
import sys
import time
from datetime import date
# 在linux会识别不了包 所以要加临时搜索目录
from dim import dim_plate_df

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from ads import ads_stock_suggest_di, export_clickhouse
from dwd import dwd_stock_quotes_di, dwd_stock_zt_di, dwd_stock_strong_di, dwd_stock_technical_indicators_df, \
    dwd_stock_quotes_stand_di
from ods import ods_lg_indicator_di
from util.CommonUtils import get_code_list, get_process_num
from util.DBUtils import hiveUtil
from util.stockImportExportUtils import export_stock_zt_hive

def task_update_daily():
    start_date = date.today().strftime('%Y%m%d')
    end_date = start_date
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

    code_list = get_code_list()
    period = 'daily'
    # adjust = "hfq"
    adjust = "qfq"
    hive_engine = hiveUtil().engine
    process_num = get_process_num()

    # 有单独用hive的 不能用spark-submit提交
    # 程序开始运行时间
    start_time = time.time()

    # 财务 这个也跑得慢全部代码遍历对比日期 要跑20分钟可以另外单独跑 有时候会漏数据 总市值不能为空
    ods_lg_indicator_di.multiprocess_run(code_list, start_date, end_date,hive_engine, process_num)

    # 这个dwd有先后顺序
    dwd_stock_technical_indicators_df.get_data()
    dwd_stock_quotes_di.get_data(start_date, end_date)
    # 这玩意全量跑不动 要一年一年跑
    factors = ['volume','volume_ratio_1d','volume_ratio_5d','turnover','turnover_rate','turnover_rate_5d','turnover_rate_10d','total_market_value','pe','pe_ttm']
    dwd_stock_quotes_stand_di.get_data(factors,start_date, end_date)
    dwd_stock_zt_di.get_data(start_date, end_date)
    dwd_stock_strong_di.get_data(start_date, end_date)

    # ads_stock_suggest_di.get_data(start_date, end_date)
    # ads_strategy_yield_di.get_data()
    export_clickhouse.get_data(start_date, end_date)
    # export_stock_zt_hive(start_date)
    # nd_codes = ['002689', '002094', '002651', '002264', '002808', '002888', '003040', '002762', '002238', '002766', '003028']
    # export_stock_ndzt_hive(start_date,nd_codes)
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
#
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/main_lg.py update
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/main_lg.py update 20210101 20221230
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/main_lg.py update 20221230 20221230
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/main_lg.py all
# nohup /opt/code/05_quantitative_trading_hive/main_lg.py update 20221010 20221010 >> my.log 2>&1 &
if __name__ == '__main__':
    task_update_daily()