import tushare as ts
import os
import sys
# import time
# from datetime import date
# import pandas as pd
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
# 输出显示设置
# pd.set_option('max_rows', None)
# pd.set_option('max_columns', None)
# pd.set_option('expand_frame_repr', False)
# pd.set_option('display.unicode.ambiguous_as_wide', True)
# pd.set_option('display.unicode.east_asian_width', True)
def run():
    ts.set_token('65686e5c83bc9b3db3752a5d94c7c471b0bc58695a2fb66cced015a6')
    pro = ts.pro_api()
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    print(data)
    print(type(data))

if __name__ == '__main__':
    run()