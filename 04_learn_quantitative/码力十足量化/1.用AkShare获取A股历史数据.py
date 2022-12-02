import os
import time
import pandas as pd
import akshare as ak
import warnings
warnings.filterwarnings("ignore")

# 输出显示设置
pd.options.display.max_rows=None
pd.options.display.max_columns=None
pd.options.display.expand_frame_repr=False
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

# 参数设置
start_date = '20170101'  # 数据获取开始日期
end_date = '20220820'  # 数据获取结束日期
adj = 'hfq'  # 复权类型：None未复权 qfq前复权 hfq后复权
period = "daily"  # 周期可选：'daily', 'weekly', 'monthly'

# 利用东财实时行情数据接口获取所有股票代码接口
df = ak.stock_zh_a_spot_em()
code_list = df[['代码', '名称']].values


# 创建文件存储路径
def create_path(ak_code):
    global path
    date_str = str(pd.to_datetime(start_date).date())  # 日期转换成字符串

    path = os.path.join(".", "all_stock_candle", "stock", date_str)
    # 保存数据
    if not os.path.exists(path):
        # os.mkdir(path)  # 可以建一级文件夹
        os.makedirs(path)  # 可以建多级文件夹
    file_name = ak_code + ".csv"
    return os.path.join(path, file_name)


# 获取所有历史上股票的历史数据
for i in range(len(code_list)):
    ak_code = code_list[i][0]
    ak_name = code_list[i][1]
    print(f"已下载到{i+1}支股票数据，股票代码为:{ak_code}，股票名称为：{ak_name}")

    try:
        # 利用东财历史行情数据接口获取股票数据
        df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date, adjust=adj)

    except Exception as e:
        print(e)
    df['股票名称'] = ak_name
    df['股票代码'] = ak_code
    df.rename(columns={'日期': '交易日期', '开盘': '开盘价', '收盘': '收盘价', '最高': '最高价', '最低': '最低价'}, inplace=True)
    df = df[['交易日期', '股票代码', '股票名称', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额', '涨跌幅', '换手率']]

    # 在股票代码前加上交易所简称
    df['股票代码'] = df["股票代码"].astype(str)
    df.loc[df["股票代码"].str.startswith('6'), '股票代码'] = "sh" + df["股票代码"]
    df.loc[df["股票代码"].str.startswith('4') | df['股票代码'].str.startswith('8'), '股票代码'] = "bj" + df["股票代码"]
    df.loc[df["股票代码"].str.startswith('3') | df['股票代码'].str.startswith('0'), '股票代码'] = "sz" + df["股票代码"]

    # 排序、去重
    df.sort_values(by=['交易日期'], ascending=True, inplace=True)
    df.drop_duplicates(subset=['股票代码', '交易日期'], keep='first', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 存储文件
    path = create_path(ak_code)
    pd.DataFrame(columns=['数据由码力十足学量化收集']).to_csv(path, index=False, encoding='gbk')
    df.to_csv(path, index=False, mode='a', encoding="gbk")

