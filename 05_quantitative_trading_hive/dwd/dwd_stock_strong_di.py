import os
import sys
import time
import warnings
from datetime import date
import akshare as ak
import pandas as pd

# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows = None
pd.options.display.max_columns = None
pd.options.display.expand_frame_repr = False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
from util.CommonUtils import get_process_num, get_spark


def get_data(start_date, end_date):
    try:
        appName = os.path.basename(__file__)
        spark = get_spark(appName)
        start_date, end_date = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
        s_date = '20210101'
        # 增量 因为持股5日收益 要提前6个交易日 这里是下一交易日开盘买入 持股两天 在第二天的收盘卖出
        td_df = ak.tool_trade_date_hist_sina()
        daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(s_date).date()) & (td_df.trade_date < start_date)]
        daterange_df = daterange_df.iloc[-7:, 0].reset_index(drop=True)
        if daterange_df.empty:
            start_date = pd.to_datetime(start_date).date()
        else:
            start_date = pd.to_datetime(daterange_df[0]).date()

        spark_df = spark.sql("""
select t1.trade_date,
       t1.stock_code,
       t1.stock_name,
       t2.open_price,
       t2.close_price,
       t2.high_price,
       t2.low_price,
       t2.volume,
       t2.volume_ratio_1d,
       t2.volume_ratio_5d,
       t2.change_percent,
       t2.turnover_rate,
       t2.turnover_rate_5d,
       t2.turnover_rate_10d,
       t2.total_market_value,
       t2.industry_plate,
       t2.concept_plates,
       t2.rps_5d,
       t2.rps_10d,
       t2.rps_20d,
       t2.rps_50d,
       t2.rps_120d,
       t2.rps_250d,
       t2.rs,
       t2.rsi_6d,
       t2.rsi_12d,
       t2.ma_5d,
       t2.ma_10d,
       t2.ma_20d,
       t2.ma_50d,
       t2.high_price_250d,
       t2.low_price_250d,
       t2.stock_label_names,
       t2.stock_label_num,
       t2.sub_factor_names,
       t2.sub_factor_score,
       t2.holding_yield_2d,
       t2.holding_yield_5d,
       t2.f_volume,
       t2.f_volume_ratio_1d,
       t2.f_volume_ratio_5d,
       t2.f_turnover,
       t2.f_turnover_rate,
       t2.f_turnover_rate_5d,
       t2.f_turnover_rate_10d,
       t2.f_total_market_value,
       t2.f_pe,
       t2.f_pe_ttm,
       t1.is_new_g,
       t1.selection_reason,
       
       t3.sealing_amount,
       t3.first_sealing_time,
       t3.last_sealing_time,
       t3.bomb_sealing_nums,
       t3.zt_tj,
       t3.lx_sealing_nums,
       
       t2.hot_rank,
       t2.interprets,
       t2.reason_for_lhbs,
       t2.lhb_num_60d,
       current_timestamp() as update_time,
       t1.trade_date as td
from stock.ods_stock_strong_pool_di t1
left join stock.dwd_stock_quotes_stand_di t2
      on t1.trade_date = t2.trade_date
            and t1.stock_code = t2.stock_code
            and t2.td between '%s' and '%s'
left join stock.ods_stock_zt_pool_di t3
      on t1.trade_date = t3.trade_date
            and t1.stock_code = t3.stock_code
            and t3.td between '%s' and '%s'
where t1.td between '%s' and '%s'
            """ % (start_date, end_date, start_date, end_date, start_date, end_date))
        # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
        spark_df.repartition(1).write.insertInto('stock.dwd_stock_strong_di', overwrite=True)  # 如果执行不存在这个表，会报错
        spark.stop
    except Exception as e:
        print(e)
    print('{}：执行完毕！！！'.format(appName))


# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_strong_di.py all
# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/dwd/dwd_stock_strong_di.py update 20210101 20211231
if __name__ == '__main__':
    # between and 两边都包含
    process_num = get_process_num()
    start_date = date.today().strftime('%Y%m%d')
    end_date = start_date
    if len(sys.argv) == 1:
        print("请携带一个参数 all update 更新要输入开启日期 结束日期 不输入则默认当天")
    elif len(sys.argv) == 2:
        run_type = sys.argv[1]
        if run_type == 'all':
            start_date = '20210101'
            end_date
        else:
            start_date
            end_date
    elif len(sys.argv) == 4:
        run_type = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]

    start_time = time.time()
    get_data(start_date, end_date)
    end_time = time.time()
    print('程序运行时间：耗时{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))

