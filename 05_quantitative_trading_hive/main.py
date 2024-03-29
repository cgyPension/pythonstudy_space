import os
import sys
import time
from datetime import date
# 在linux会识别不了包 所以要加临时搜索目录
from ads import export_clickhouse

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from dim import dim_dc_stock_plate_di, dim_plate_df
from dwd import dwd_stock_technical_indicators_df, dwd_stock_quotes_stand_di, dwd_stock_quotes_di, dwd_stock_zt_di, \
    dwd_stock_strong_di
from ods import ods_dc_stock_quotes_di, ods_dc_stock_tfp_di, ods_dc_stock_concept_plate_rt_di, \
    ods_dc_stock_industry_plate_rt_di, ods_dc_stock_industry_plate_cons_di, \
    ods_dc_stock_concept_plate_cons_di, ods_stock_lrb_em_di, ods_stock_lhb_detail_em_di, \
    ods_dc_stock_industry_plate_hist_di, ods_dc_stock_concept_plate_hist_di, \
    ods_trade_date_hist_sina_df, ods_stock_zt_pool_di, ods_stock_strong_pool_di, ods_stock_hot_rank_wc_di, \
    ods_dc_index_di, ods_lg_indicator_di, ods_stock_hsgt_stock_di, ods_stock_fund_holder_di
from util.CommonUtils import get_code_list, get_process_num, get_fund_list
from util.DBUtils import hiveUtil


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
    fund_list = get_fund_list()
    period = 'daily'
    # adjust = "hfq"
    adjust = "qfq"
    hive_engine = hiveUtil().engine
    process_num = get_process_num()

    # 有单独用hive的 不能用spark-submit提交
    # 程序开始运行时间
    start_time = time.time()

    # 前复权 一般季度时间后一周内会修改旧数据 这时候要全量重跑这个ods
    # 如果进行价值投资，建议采用后复权 适合回测
    # 如果进行技术分析，最好用前复权 适合指导实盘 会引入了未来函数？
    ods_dc_stock_quotes_di.multiprocess_run(code_list, period, start_date, end_date, adjust,hive_engine,process_num)
    # 有时候会到了公告日期没有发布
    ods_stock_lrb_em_di.get_data(start_date, end_date)
    # 这个年报全量很慢 平时不能全量跑  20200331", "20200630", "20200930", "20201231" 报告日期才跑
    # ods_financial_analysis_indicator_di.multiprocess_run(code_list, start_date, hive_engine)
    # ods_stock_fund_holder_di.multiprocess_run(start_date, end_date,code_list, hive_engine,process_num)

    # 其他
    ods_stock_lhb_detail_em_di.get_data(start_date, end_date)
    # 停复牌8点左右跑 不然有些数据会后补
    ods_dc_stock_tfp_di.get_data(start_date, end_date)
    ods_trade_date_hist_sina_df.get_data()
    ods_stock_zt_pool_di.get_data(start_date, end_date)
    ods_stock_strong_pool_di.get_data(start_date, end_date)
    ods_stock_hot_rank_wc_di.get_data(start_date, end_date)
    ods_dc_index_di.get_data(start_date, end_date)
    ods_stock_hsgt_stock_di.get_data(start_date, end_date)

    # 板块
    ods_dc_stock_industry_plate_cons_di.multiprocess_run(start_date, process_num)
    ods_dc_stock_concept_plate_cons_di.multiprocess_run(start_date, process_num)
    ods_dc_stock_industry_plate_rt_di.get_data(start_date)
    ods_dc_stock_concept_plate_rt_di.get_data(start_date)
    ods_dc_stock_industry_plate_hist_di.multiprocess_run(start_date, end_date,process_num)
    ods_dc_stock_concept_plate_hist_di.multiprocess_run(start_date, end_date,process_num)
    dim_plate_df.get_data()
    dim_dc_stock_plate_di.get_data(start_date, end_date)

    # 这个接口间接性有问题 所有放在这里方便后面重跑 要是重跑这里 直接跑main_lg
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
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/main.py update
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/main.py update 20210101 20221230
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/main.py update 20221230 20221230
# spark-submit /opt/code/pythonstudy_space/05_quantitative_trading_hive/ods/main.py all
# nohup /opt/code/05_quantitative_trading_hive/main.py update 20221010 20221010 >> my.log 2>&1 &
if __name__ == '__main__':
    task_update_daily()