# 使用AKSHARE + mysql 实现动态抓取个股的交易历史数据
# 同理外面再包一层循环就可以把所有的交易历史数据下载每个股票一个表。
# 后续下载历史数据并且定制下每天更新脚本这样历史交易数据就解决了。
#
# 后续就是弄个回测框架
#
# 添加宏观因素 再添加个股微观因素   再历史回测因素相关性
import time
from datetime import datetime

import pandas as pd
import warnings
from sqlalchemy import create_engine
import akshare as ak
warnings.filterwarnings("ignore")
# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide',True)
pd.set_option('display.unicode.east_asian_width',True)

# myslq url
engine = create_engine('mysql+pymysql://root:123456@hadoop102:3306/stock?charset=utf8')

# 参数设置
# period="daily" # 周期 'daily', 'weekly', 'monthly'
# start_date="20220822" # 数据获取开始日期
# end_date="20220822" # 数据获取结束日期
# adjust="hfq" # 复权类型 qfq": "前复权", "hfq": "后复权", "": "不复权"

def get_stock(period="daily",start_date=None,end_date=None,adjust="hfq",engine=engine):
    """
    获取指定日期的A股数据写入mysql

    :param period: 周期 'daily', 'weekly', 'monthly'
    :param start_date: 数据获取开始日期
    :param end_date: 数据获取结束日期
    :param adjust: 复权类型 qfq": "前复权", "hfq": "后复权", "": "不复权"
    :param engine: myslq url
    :return: None
    """

    if start_date or end_date is None:
        # 今天 要下午3点后运行
        start_date = datetime.date.today().strftime('%Y%m%d')
        end_date = start_date

    # 利用东财实时行情数据接口获取所有股票代码接口 实时接口获取数据的时候排除北京
    df = ak.stock_zh_a_spot_em()
    code_list = df[['代码', '名称']].values

    # 获取所有历史上股票的历史数据
    for i in range(len(code_list)):
        ak_code = code_list[i][0]
        ak_name = code_list[i][1]
        # print(f"已下载到{i+1}支股票，股票代码为：{ak_code}，股票名称为：{ak_name}")

        try:
            # 东财接口  没有市值 要从网易接口join 补充
            stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date, adjust=adjust)
        except Exception as e:
            print(e)

        try:
            # 网易接口 获取 总市值 流通市值
            stock_zh_a_hist_163_df = ak.stock_zh_a_hist_163(symbol=ak_code, start_date=start_date, end_date=end_date)
        except Exception as e:
            print(e)

        try:
            # 东方财富网-数据中心-特色数据-两市停复牌
            stock_tfp_em_df = ak.stock_tfp_em(date="20220523")
        except Exception as e:
            print(e)

        if stock_zh_a_hist_df.empty==False:
            # # 在股票代码前加上交易所简称
            if ak_code.startswith('6') == True:
                stock_zh_a_hist_df['股票代码'] = ak_code + '.SH'
            elif ak_code.startswith('8') or ak_code.startswith('4') == True:
                stock_zh_a_hist_df['股票代码'] = ak_code + '.BJ'
            else:
                stock_zh_a_hist_df['股票代码'] = ak_code + '.SZ'

            stock_zh_a_hist_df['股票名称'] = ak_name
            stock_zh_a_hist_df.rename(columns={'日期': '交易日期', '开盘': '开盘价', '收盘': '收盘价', '最高': '最高价', '最低': '最低价'},
                                      inplace=True)
            stock_zh_a_hist_df = stock_zh_a_hist_df[
                ['交易日期', '股票代码', '股票名称', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]

            # 排序、去重
            stock_zh_a_hist_df.sort_values(by=['交易日期'], ascending=True, inplace=True)
            # stock_zh_a_hist_df.drop_duplicates(subset=['股票代码','交易日期'],keep='first',inplace=True)
            stock_zh_a_hist_df.reset_index(drop=True, inplace=True)

            # 写入mysql 会自动建表
            stock_zh_a_hist_df.to_sql('ods_dc_stock_quotes_di', engine, chunksize=100000, index=None)
            # print('存入成功！')
            continue



