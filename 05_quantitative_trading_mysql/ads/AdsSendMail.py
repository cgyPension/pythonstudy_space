import os
import sys
from datetime import time, date

import pandas as pd

# 在linux会识别不了包 所以要加临时搜索目录
from util.DBUtils import sqlalchemyUtil
from util.df_SendMail import send_mail

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

def get_data(engine):
    try:
        start_date = date.today().strftime('%Y-%m-%d')

        sql = """
                select trade_date,
                       stock_code,
                       stock_name,
                       open_price,
                       close_price,
                       high_price,
                       low_price,
                       change_percent,
                       turnover_rate,
                       turnover_rate_10d,
                       total_market_value,
                       pe_ttm,
                       industry_plate,
                       concept_plates,
                       stock_label_names,
                       factor_names,
                       stock_strategy_name,
                       stock_strategy_ranking,
                       create_time
                from ads_stock_suggest_di
                where trade_date = '%s';     
        """% (start_date)

        df = pd.read_sql(sql, engine)

        df.rename(columns={
            'trade_date': '交易日期',
            'stock_code': '股票代码',
            'stock_name': '股票名称',
            'open_price': '开盘价',
            'close_price': '收盘价',
            'high_price': '最高价',
            'low_price': '最低价',
            'change_percent': '涨跌幅%',
            'turnover_rate': '换手率%',
            'turnover_rate_10d': '10日平均换手率%',
            'total_market_value': '总市值',
            'pe_ttm': '市盈率TTM',
            'industry_plate': '行业板块',
            'concept_plates': '概念板块 ,拼接',
            'stock_label_names': '股票标签名称 ,拼接',
            'factor_names': '因子标签名称 ,拼接',
            'stock_strategy_name': '股票策略名称 股票标签名称 +拼接',
            'stock_strategy_ranking': '策略内排行',
            'create_time': '创建时间'
        }, inplace=True)
        # 发送邮件
        send_mail(df)
    except Exception as e:
        print(e)
    print('AdsSendMail：执行完毕！！！')


# nohup AdsSendMail.py >> my.log 2>&1 &
# python AdsSendMail.py
if __name__ == '__main__':
    engine = sqlalchemyUtil().engine
    start_time = time.time()
    get_data(engine)
    engine.dispose()
    end_time = time.time()
    print('程序运行时间：{}s，{}分钟'.format(end_time - start_time, (end_time - start_time) / 60))
