import requests
import json
import pandas as pd
def get_stock_daily_em(stock='600031',start_date='20210101',end_date='20221114'):
    '''
    获取股票日线数据
    '''
    if stock[0]=='6':
        stock='1.'+stock
    else:
        stock='0.'+stock
    url='http://push2his.eastmoney.com/api/qt/stock/kline/get?'
    params={
        'fields1':'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13',
        'fields2':'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'beg':start_date,
        'end':'20500101',
        'ut':'fa5fd1943c7b386f172d6893dbfba10b',
        'rtntype':end_date,
        'secid':stock,
        'klt':'101',
        'fqt':'1',
        'cb':'jsonp1668432946680'
    }
    res=requests.get(url=url,params=params)
    text=res.text[19:len(res.text)-2]
    json_text=json.loads(text)
    df=pd.DataFrame(json_text['data']['klines'])
    df.columns=['数据']
    data_list=[]
    for i in df['数据']:
        data_list.append(i.split(','))
    data=pd.DataFrame(data_list)
    columns=['date','open','close','high','low','成交量','成交额','振幅','涨跌幅','涨跌额','换手率']
    data.columns=columns
    print(data)
    return data
if __name__=='__main__':
    df=get_stock_daily_em(stock='600031',start_date='20200101',end_date='20221114')