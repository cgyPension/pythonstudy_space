import os
import sys
import time
import warnings
from datetime import date, datetime
import akshare as ak
import numpy as np
import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from util.CommonUtils import str_pre, get_spark
warnings.filterwarnings("ignore")
# 输出显示设置
pd.set_option('max_rows', None)
pd.set_option('max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)



def get_data(start_date, end_date):
    appName = os.path.basename(__file__)
    # 本地模式
    spark = get_spark(appName)
    pd_df = pd.DataFrame()
    # 新浪财经的股票交易日历数据
    td_df = ak.tool_trade_date_hist_sina()
    daterange_df = td_df[(td_df.trade_date >= pd.to_datetime(start_date).date()) & (td_df.trade_date<=pd.to_datetime(end_date).date())]
    for single_date in daterange_df.trade_date:
        try:
            df = ak.stock_lhb_detail_em(single_date.strftime("%Y%m%d"), single_date.strftime("%Y%m%d"))
            # print('ods_stock_lhb_detail_em_di：正在处理{}...'.format(single_date))
            if df.empty:
                continue
            # 去重、保留最后一次出现的
            df.drop_duplicates(subset=['序号'], keep='last', inplace=True)

            df['stock_code'] = df['代码'].apply(str_pre)
            df['trade_date'] = pd.to_datetime(single_date)

            df['td'] = df['trade_date']
            df['update_time'] = datetime.now()

            df.rename(columns={'名称':'stock_name','序号':'ranking','解读':'interpret','收盘价':'close_price','涨跌幅':'change_percent','龙虎榜净买额':'lhb_net_buy','龙虎榜买入额':'lhb_buy_amount','龙虎榜卖出额':'lhb_sell_amount','龙虎榜成交额':'lhb_turnover','市场总成交额':'total_turnover','净买额占总成交比':'nbtt','成交额占总成交比':'ttt','换手率':'turnover_rate','流通市值':'circulating_market_value','上榜原因':'reason_for_lhb'}, inplace=True)
            df = df[['trade_date','ranking','stock_code','stock_name','close_price','change_percent','circulating_market_value','turnover_rate','interpret','reason_for_lhb','lhb_net_buy','lhb_buy_amount','lhb_sell_amount','lhb_turnover','total_turnover','nbtt','ttt','update_time','td']]
            # MySQL无法处理nan
            # df = df.replace({np.nan: None})
            # 数据中存在None，进行替换才能创基
            df = df.replace(pd.NA, '')
            pd_df = pd_df.append(df)

        except Exception as e:
            print(e)

    spark_df = spark.createDataFrame(pd_df)
    # # 默认的方式将会在hive分区表中保存大量的小文件，在保存之前对 DataFrame 用 .repartition() 重新分区，这样就能控制保存的文件数量。这样一个分区只会保存 5 个数据文件。
    spark_df.repartition(1).write.insertInto('stock.ods_stock_lhb_detail_em_di',overwrite=True)  # 如果执行不存在这个表，会报错
    print('{}：执行完毕！！！'.format(appName))


# spark-submit /opt/code/05_quantitative_trading_hive/ods/ods_stock_lhb_detail_em_di.py all
# nohup python ods_stock_lhb_detail_em_di.py update 20220930 >> my.log 2>&1 &
# python ods_stock_lhb_detail_em_di.py all
# python ods_stock_lhb_detail_em_di.py update 20221101  20221110
if __name__ == '__main__':
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

    start_time = time.time()
    get_data(start_date, end_date)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))
