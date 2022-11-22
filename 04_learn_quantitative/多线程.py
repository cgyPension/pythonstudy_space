
import requests
import json
import pandas as pd
import threading
import akshare as ak
from datetime import datetime
from tqdm import tqdm
import pymysql
from sqlalchemy import create_engine
#链接数据库
db=pymysql.connect(host='localhost',user='root2',passwd='密码',port=3306,db='root2',charset='utf8')
#建立游标
cursor=db.cursor()
now_date_str=str(datetime.now())[:10]
now_date=''.join(str(datetime.now())[:10].split('-'))
def get_all_stock_code():
    '''
    获取全部股票代码
    '''
    df=ak.stock_individual_fund_flow_rank(indicator='今日')
    df1=df[['代码','名称']]
    #st
    st=ak.stock_zh_a_st_em()[['代码','名称']]
    #退市
    stop=ak.stock_zh_a_stop_em()[['代码','名称']]
    #合并st和退市
    st_stop=pd.concat([st,stop],ignore_index=True)
    code=df1['代码'].tolist()
    name=df1['名称'].tolist()
    #删除st,stop
    for m,n in zip(st_stop['代码'].tolist(),st_stop['名称'].tolist()):
        try:
            m_index=code.index(m)
            n_index=name.index(n)
            del code[m_index]
            del name[n_index]
        except:
            pass
    data=pd.DataFrame({'代码':code,'名称':name})
    #删除北京交易所
    def select_data(x):
        if x[:3]=='688':
            return '是'
        else:
            return '无'
    data['北京交易所']=data['代码'].apply(select_data)
    #选择数据
    df2=data[data['北京交易所']=='无']
    del df2['北京交易所']
    return df2
all_code=get_all_stock_code()
code_dict=dict(zip(all_code['代码'].tolist(),all_code['名称'].tolist()))
code_list=all_code['代码'].tolist()
def get_stock_daily_em_to_sql(stock='600031',start_date='20220101',end_date=now_date,
host = 'localhost',db = 'root2',user = 'root2',password = '密码'):
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
    engine = create_engine(str(r"mysql+pymysql://%s:" + '%s' + "@%s/%s") % (user, password, host, db))
    try:
        data.to_sql('{}'.format(code_dict[stock[2:]]),con=engine,if_exists='replace',index=False)
        print(code_dict[stock[2:]],'下载完成')
    except:
        print('{}有问题'.format(code_dict[stock[2:]]))
    return data
def down_all_data_to_sql():
    threading_list=[]
    for stock in code_list:
        threading_list.append(threading.Thread(target=get_stock_daily_em_to_sql,args=(stock,)))
    print('建立多线程完成')
    print('开始启动多线程')
    for down in threading_list:
        down.start()
    for down in threading_list:
        down.join()
    print('开始下载')
def get_read_sql_data(table_name='爱玛科技'):
    '''
    读取数据库表的数据
    '''
    sql='select * from {}'.format(table_name)
    try:
        #执行
        cursor.execute(sql)
        #提交
        db.commit()
        #将返回的数据解析成pandas
        columns=[]
        #列
        for i in cursor.description:
            columns.append(i[0])
        #内容
        data=[]
        for m in cursor.fetchall():
            data.append(list(m))
        result=pd.DataFrame(data,columns=columns)
        db.close()
        return result
    except:
        print('{}不存在'.format(table_name))
        db.close()
if __name__ == "__main__":
    #读取数据，可以多线程
    df=get_read_sql_data(table_name='爱旭股份')
    print(df)
    #下载到数据库
    down_all_data_to_sql()