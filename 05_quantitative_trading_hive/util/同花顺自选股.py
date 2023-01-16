import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import execjs
import time
from datetime import datetime
import pandas as pd
import requests
import json
import akshare as ak

def get_var():
    '''
    获取js
    :return:
    '''
    js = '/opt/code/pythonstudy_space/05_quantitative_trading_hive/util/ths.js'
    with open(js) as f:
        comm = f.read()
    comms = execjs.compile(comm)
    result = comms.call('v')
    return result

def get_headers(cookie='Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1672230413; historystock=688255%7C*%7C003816%7C*%7C002933%7C*%7C600706%7C*%7C688687; Hm_lvt_da7579fd91e2c6fa5aeb9d1620a9b333=1673161546; log=; user=MDq080MxOjpOb25lOjUwMDo1MTMxNDQ1NjI6NywxMTExMTExMTExMSw0MDs0NCwxMSw0MDs2LDEsNDA7NSwxLDQwOzEsMTAxLDQwOzIsMSw0MDszLDEsNDA7NSwxLDQwOzgsMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDEsNDA7MTAyLDEsNDA6MjQ6Ojo1MDMxNDQ1NjI6MTY3MzY1OTkxNTo6OjE1NzQ1NTQ5ODA6MjY3ODQwMDowOjFkZjgzN2I5YThiZTRiNzBhZTIyZTE2MzViYWFiYjlhODpkZWZhdWx0XzQ6MQ%3D%3D; userid=503144562; u_name=%B4%F3C1; escapename=%25u5927C1; ticket=90f706428300af2c9ad5b9bc8faf3498; user_status=0; utk=bd0610c31e8fad6a9f67c1c47f83cb90; Hm_lpvt_da7579fd91e2c6fa5aeb9d1620a9b333=1673661956; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1673661957; v=A5hM9kejT7kEnWM9jZ0-eQlTac0vgfoIXu7QjdKD5lmEWjbzepHMm671oDEh'):
    '''
    获取请求头 设置自己的请求头在get_header配置
    :param cookie: 
    :return: 
    '''
    v = get_var()
    cookie = cookie.split('v=')
    cookie = cookie[0] + 'v=' + v
    headers={
        'Cookie':cookie ,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    }
    return headers

def get_all_stock():
    '''
    获取全部自选股
    :return:
    '''
    headers=get_headers()
    url='https://t.10jqka.com.cn/newcircle/group/getSelfStockWithMarket/?'
    params={
        'callback': 'selfStock',
        '_': '1673617915586'
    }
    res=requests.get(url=url,params=params,headers=headers)
    text=res.text[10:len(res.text)-2]
    json_text=json.loads(text)
    df = pd.DataFrame(json_text['result'])
    return df

def add_stock_to_account(stock='600111'):
    '''
    添加股票到自选股
    :param stock:
    :return:
    '''
    url='https://t.10jqka.com.cn/newcircle/group/modifySelfStock/?'
    headers=get_headers()
    params={
        'callback':'modifyStock',
        'op': 'add',
        'stockcode': stock,
        '_': '1673620068115',
    }
    res = requests.get(url=url, params=params, headers=headers)
    text = res.text[12:len(res.text) - 2]
    json_text = json.loads(text)
    err=json_text['errorMsg']
    if err=='修改成功':
        print('{}加入自选股成功'.format(stock))
    else:
        print('{}{}'.format(stock,err))

def del_stock_from_account(stock='600111'):
    '''
    删除股票从自选股
    :param stock:
    :return:
    '''
    url = 'https://t.10jqka.com.cn/newcircle/group/modifySelfStock/?'
    headers = get_headers()
    df=get_all_stock()
    try:
        marker=df[df['code']==stock]['marketid'].tolist()[0]
        stockcode='{}_{}'.format(stock,marker)
        params={
            'op':'del',
            'stockcode':stock
        }
        res = requests.get(url=url, params=params, headers=headers)
        text = res.text
        json_text = json.loads(text)
        err = json_text['errorMsg']
        if err == '修改成功':
            print('{}删除自选股成功'.format(stock))
        else:
            print('{}{}'.format(stock, err))
    except:
        print('{}没有在自选股'.format(stock))

def all_zt_stock_add_account(date='20230113'):
    '''
    将涨停的股票全部加入自选股
    :return:
    '''
    df=ak.stock_zt_pool_em(date=date)
    for stock in df['代码'].tolist():
        add_stock_to_account(stock=stock)


def all_del_add_stocks(codes):
    '''
    将所有自选股删除并加入新股票
    :return:
    '''
    del_df = get_all_stock()
    d_n = 0
    a_n = 0

    url = 'https://t.10jqka.com.cn/newcircle/group/modifySelfStock/?'
    headers = get_headers()
    for stock in del_df['code'].tolist():
        try:
            marker = del_df[del_df['code'] == stock]['marketid'].tolist()[0]
            stockcode = '{}_{}'.format(stock, marker)
            params = {
                'op': 'del',
                'stockcode': stock
            }
            res = requests.get(url=url, params=params, headers=headers)
            text = res.text
            json_text = json.loads(text)
            err = json_text['errorMsg']
            if err == '修改成功':
                d_n = d_n+1
            else:
                print('{}{}'.format(stock, err))
        except:
            print('{}没有在自选股'.format(stock))

    for stock in codes:
        params = {
            'callback': 'modifyStock',
            'op': 'add',
            'stockcode': stock,
            '_': '1673620068115',
        }
        res = requests.get(url=url, params=params, headers=headers)
        text = res.text[12:len(res.text) - 2]
        json_text = json.loads(text)
        err = json_text['errorMsg']
        if err == '修改成功':
            a_n = a_n+1
        else:
            print('{}{}'.format(stock, err))

    print('删除自选股成功，删除了{}个；加入自选股成功，加入了{}个'.format(d_n,a_n))

# python /opt/code/pythonstudy_space/05_quantitative_trading_hive/util/同花顺自选股.py
if __name__=='__main__':
    start_time = time.time()
    codes = ['002689','002094','002651','002264','002808','002888','003040','002762','002238','002766','003028']
    all_del_add_stocks(codes)
    end_time = time.time()
    print('{}：程序运行时间：{}s，{}分钟'.format(os.path.basename(__file__),end_time - start_time, (end_time - start_time) / 60))

