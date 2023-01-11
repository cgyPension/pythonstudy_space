import os
#如果没有安装自动安装
try:
    from snownlp import SnowNLP
except:
    os.system('py -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple some-package snownlp')
import requests
import json
from bs4 import BeautifulSoup
from lxml import etree
from tqdm import tqdm
import pandas as pd
import numpy as np
import plotly.express as px
import akshare as ak
import  warnings
#os.system('py -m pip install zipline')
warnings.filterwarnings('ignore')
'''
函数名称  函数
东方财富股吧,日线分析  analysis_em_stock_ba_daily(stock='600031',page_size=2)
东方财富股吧,分钟分析  analysis_em_stock_ba_mi(stock='600031',page_size=2)
计算函数  alalysis_text_sentiments_list(df)
获取股吧情绪分析结果,日线  analysis_em_stock_ba_sentiments_all_daily(stock='600031',page_size=10)
分析东方财富股吧实时股票评论数据获取情绪分析结果，分钟  analysis_em_stock_ba_sentiments_all_mi(stock='600031',page_size=10)
获取东方财富指数评论，日线数据  get_A_stock_ba_em_comment_daily(stock='上证指数',page_size=2)
获取东方财富指数评论，分钟数据，实时数据  get_A_stock_ba_em_comment_mi(stock='上证指数',page_size=2)
获取情绪分析结果,指数，日线  get_analysis_A_stock_index_sentiments_daily(stock='上证指数',page_size=2)
获取情绪分析结果,指数，实时数据   get_analysis_A_stock_index_sentiments_mi(stock='上证指数',page_size=2)
板块评论数据日线，具体名称   get_stock_board_industry_daily(name='旅游酒店',page_size=2)
板块评论数据分钟数据，具体名称，实时数据  get_stock_board_industry_mi(name='旅游酒店',page_size=2)
获取情绪分析结果,板块日线  get_stock_board_industry_sentiments_daily(name='旅游酒店',page_size=2)
获取情绪分析结果,板块分钟实时数据  get_stock_board_industry_sentiments_mi(name='旅游酒店',page_size=2)
股票板块概念日线评论数据  get_stock_board_concept_daily(name='昨日连板',page_size=2)
股票板块概念评论数据,分钟数据，实时数据  get_stock_board_concept_mi(name='昨日连板',page_size=2)
获取情绪分析结果,日线数据  get_stock_board_concept_sentiments_daily(name='昨日连板',page_size=2)
获取情绪分析结果,分钟实时数据  get_stock_board_concept_sentiments_mi(name='昨日连板',page_size=2)
开放式基金评论,日线数据  get_open_fund_comment_daliy(fund='161725',page_size=2)
开放式基金评论,分钟数据，实时数据  get_open_fund_comment_mi(fund='161725',page_size=2)
基金日线情绪分析  get_open_fund_comment_sentiments_daliy(fund='161725',page_size=2)
基金日线情绪分析  get_open_fund_comment_sentiments_mi(fund='161725',page_size=2)
获取股票热门讨论数据,日线  get_stock_top_comment_daily(page_size=2)
获取股票热门讨论数据,分钟数据，实时数据  get_stock_top_comment_mi(page_size=2)
获取股票热门讨论数据,日线数据  get_stock_top_sentiments_daily(page_size=2)
获取股票热门讨论数据,分钟数据，实时数据  get_stock_top_sentiments_mi(page_size=2)
雪球讨论热贴  get_stock_top_comment_xq()
雪球情绪分析  get_stock_top_sentiments_xq()
东方财富7*24小时数据，日线   get_stock_7_24_data_daily(page_size=5)
东方财富7*24小时数据，实时数据  get_stock_7_24_data_mi(page_size=5)
东方财富7*24小时数据，日线，情绪分析  get_stock_7_24_sentiments_daily(page_size=5)
东方财富7*24小时，数据情绪分析，分钟数据  get_stock_7_24_sentiments_mi(page_size=5)
全球财经导读，日线  get_all_gold_finance_conment_daily(page_size=2)
全球财经导读，分钟，实时数据  get_all_gold_finance_conment_mi(page_size=2)
全球财经导读日线，情绪分析，日线  get_all_gold_finance_sentiments_daily(page_size=2)
全球财经导读日线，情绪分析，分钟，实时数据  get_all_gold_finance_sentiments_mi(page_size=2)
外汇实时数据，数据很多不进行日线分析  get_all_gold_foregn_exchange_conment(page_size=10)
外汇实时数据，数据很多不进行日线分析，情绪分析，实时数据  get_all_gold_foregn_exchange_sentiments(page_size=10)
实时期货新闻，数据很多不进行日线分析   get_all_gold_futures_conment(page_size=10)
实时期货新闻，情绪分析，数据很多不进行日线分析  get_all_gold_futures_sentiments(page_size=10)
'''
'''
def alalysis_text_sentiments(text= '利好'):
    text1=SnowNLP(text)
    result=text1.sentiments
    return result
'''
def analysis_em_stock_ba_daily(stock='600031',page_size=2):
    '''
    东方财富股吧,日线分析
    100页
    :param stock:
    :return:
    '''
    titles=[]
    times=[]
    for i in tqdm(range(1,page_size)):
        url='http://guba.eastmoney.com/list,{}_{}.html'.format(stock,i)
        res=requests.get(url=url)
        soup=etree.HTML(res.text)
        for j in range(2,81):
            title=soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time=soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0][:5])
    data=pd.DataFrame({'时间':times,'内容':titles})
    return data
def analysis_em_stock_ba_mi(stock='600031',page_size=2):
    '''
    东方财富股吧,分钟分析
    100页
    :param stock:
    :return:
    '''
    titles=[]
    times=[]
    for i in tqdm(range(1,page_size)):
        url='http://guba.eastmoney.com/list,{}_{}.html'.format(stock,i)
        res=requests.get(url=url)
        soup=etree.HTML(res.text)
        for j in range(2,81):
            title=soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time=soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0])
    data=pd.DataFrame({'时间':times,'内容':titles})
    return data
def alalysis_text_sentiments_list(df):
    '''
    计算函数
    分析情绪
    技术安每天情绪的均值
    进行百分比转变
    :param df: 
    :return: 
    '''
    df=df
    data_len=len(df['内容'].tolist())
    result_list=[]
    for title in df['内容'].tolist():
        text=SnowNLP(title)
        result=text.sentiments
        result_list.append(result)
    data=np.mean(result_list)*100
    return data
def analysis_em_stock_ba_sentiments_all_daily(stock='600031',page_size=10):
    '''
    获取情绪分析结果,日线
    :param stock:
    :param page_size:
    :return:
    '''
    df=analysis_em_stock_ba_daily(stock=stock,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result=alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def analysis_em_stock_ba_sentiments_all_mi(stock='600031',page_size=10):
    '''
    分析东方财富股吧实时股票评论数据获取情绪分析结果，分钟
    :param stock:
    :param page_size:
    :return:
    '''
    df=analysis_em_stock_ba_mi(stock=stock,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result=alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_A_stock_ba_em_comment_daily(stock='上证指数',page_size=2):
    '''
    获取东方财富指数评论，日线数据
    :return:
    '''
    index = {'上证指数': 'zssh000001','科创板':'bk0869','创业板':'zssz399006','恒生指数':'zsgj110000'}
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url = 'http://guba.eastmoney.com/list,{}_{}.html'.format(index[stock],i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(2, 81):
            title = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0][:5])
    data = pd.DataFrame({'时间': times, '内容': titles})
    return data
def get_A_stock_ba_em_comment_mi(stock='上证指数',page_size=2):
    '''
    获取东方财富指数评论，分钟数据，实时数据
    :return:
    '''
    index = {'上证指数':'zssh000001', '科创板': 'bk0869', '创业板': 'zssz399006', '恒生指数': 'zsgj110000'}
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url = 'http://guba.eastmoney.com/list,{}_{}.html'.format(index[stock],i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(2, 81):
            title = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0])
    data = pd.DataFrame({'时间': times, '内容': titles})
    return data
def get_analysis_A_stock_index_sentiments_daily(stock='上证指数',page_size=2):
    '''
    获取情绪分析结果,指数，日线
    :param stock:
    :param page_size:
    :return:
    '''
    df=get_A_stock_ba_em_comment_daily(stock=stock,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result=alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_analysis_A_stock_index_sentiments_mi(stock='上证指数',page_size=2):
    '''
    获取情绪分析结果,指数，实时数据
    :param stock:
    :param page_size:
    :return:
    '''
    df=get_A_stock_ba_em_comment_mi(stock=stock,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result=alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_stock_board_industry_daily(name='旅游酒店',page_size=2):
    '''
    板块评论数据日线，具体名称
    ak.stock_board_industry_name_em()
    :param name:
    :param page_size:
    :return:
    '''
    df=ak.stock_board_industry_name_em()
    board_dict=dict(zip(df['板块名称'].tolist(),df['板块代码'].tolist()))
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url = 'http://guba.eastmoney.com/list,{}_{}.html'.format(board_dict[name],i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(2, 81):
            title = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0][:5])
    data = pd.DataFrame({'时间': times, '内容': titles})
    return data
def get_stock_board_industry_mi(name='旅游酒店',page_size=2):
    '''
    板块评论数据分钟数据，具体名称，实时数据
    ak.stock_board_industry_name_em()
    :param name:
    :param page_size:
    :return:
    '''
    df=ak.stock_board_industry_name_em()
    board_dict=dict(zip(df['板块名称'].tolist(),df['板块代码'].tolist()))
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url = 'http://guba.eastmoney.com/list,{}_{}.html'.format(board_dict[name],i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(2, 81):
            title = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0])
    data = pd.DataFrame({'时间': times, '内容': titles})
    return data
def get_stock_board_industry_sentiments_daily(name='旅游酒店',page_size=2):
    '''
    获取情绪分析结果,板块日线
    :param stock:
    :param page_size:
    :return:
    '''
    df=get_stock_board_industry_daily(name=name,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result=alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_stock_board_industry_sentiments_mi(name='旅游酒店',page_size=2):
    '''
    获取情绪分析结果,板块分钟实时数据
    :param stock:
    :param page_size:
    :return:
    '''
    df=get_stock_board_industry_mi(name=name,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result=alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_stock_board_concept_daily(name='昨日连板',page_size=2):
    '''
    股票板块概念日线评论数据
    :param name:
    :param page_size:
    :return:
    '''
    df=ak.stock_board_concept_name_em()
    concept_dict = dict(zip(df['板块名称'].tolist(), df['板块代码'].tolist()))
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url = 'http://guba.eastmoney.com/list,{}_{}.html'.format(concept_dict[name], i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(2, 81):
            title = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0][:5])
    data = pd.DataFrame({'时间': times, '内容': titles})
    return data
def get_stock_board_concept_mi(name='昨日连板',page_size=2):
    '''
    股票板块概念评论数据,分钟数据，实时数据
    df=ak.stock_board_concept_name_em()
    :param name:
    :param page_size:
    :return:
    '''
    df=ak.stock_board_concept_name_em()
    concept_dict = dict(zip(df['板块名称'].tolist(), df['板块代码'].tolist()))
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url = 'http://guba.eastmoney.com/list,{}_{}.html'.format(concept_dict[name], i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(2, 81):
            title = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0])
    data = pd.DataFrame({'时间': times, '内容': titles})
    return data
def get_stock_board_concept_sentiments_daily(name='昨日连板',page_size=2):
    '''
    获取情绪分析结果,日线数据
    :param stock:
    :param page_size:
    :return:
    '''
    df=get_stock_board_concept_daily(name=name,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result=alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_stock_board_concept_sentiments_mi(name='昨日连板',page_size=2):
    '''
    获取情绪分析结果,分钟实时数据
    :param stock:
    :param page_size:
    :return:
    '''
    df=get_stock_board_concept_mi(name=name,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result=alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_open_fund_comment_daliy(fund='161725',page_size=2):
    '''
    开放式基金评论,日线数据
    :param fund:
    :param page_size:
    :return:
    '''
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url ='http://guba.eastmoney.com/list,of{}_{}.html'.format(fund,i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(2, 81):
            title = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0][:5])
    data = pd.DataFrame({'时间': times, '内容': titles})
    return data
def get_open_fund_comment_mi(fund='161725',page_size=2):
    '''
    开放式基金评论,分钟数据，实时数据
    :param fund:
    :param page_size:
    :return:
    '''
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url ='http://guba.eastmoney.com/list,of{}_{}.html'.format(fund,i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(2, 81):
            title = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[3]/a/text()'.format(j)).strip())
            time = soup.xpath(''.join(r'//*[@id="articlelistnew"]/div[{}]/span[5]/text()'.format(j)).strip())
            titles.append(title[0])
            times.append(time[0])
    data = pd.DataFrame({'时间': times, '内容': titles})
    return data
def get_open_fund_comment_sentiments_daliy(fund='161725',page_size=2):
    '''
    基金日线情绪分析
    :param fund:
    :param page_size:
    :return:
    '''
    df = get_open_fund_comment_daliy(fund=fund,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result = alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_open_fund_comment_sentiments_mi(fund='161725',page_size=2):
    '''
    基金日线情绪分析
    :param fund:
    :param page_size:
    :return:
    '''
    df = get_open_fund_comment_mi(fund=fund,page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result = alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_stock_top_comment_daily(page_size=2):
    '''
    获取股票热门讨论数据,日线
    :return:
    '''
    names=[]
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url = 'http://guba.eastmoney.com/default,99_{}.html'.format(i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(1,88):
            name=''.join(soup.xpath('//*[@id="main-body"]/div[4]/div[1]/div[3]/div/div[2]/div[1]/ul/li[{}]/span/a[1]/text()'.format(j)))[:-1]
            title=''.join(soup.xpath('//*[@id="main-body"]/div[4]/div[1]/div[3]/div/div[2]/div[1]/ul/li[{}]/span/a[2]/text()'.format(j)))
            time=''.join(soup.xpath('//*[@id="main-body"]/div[4]/div[1]/div[3]/div/div[2]/div[1]/ul/li[{}]/cite[5]/text()'.format(j)))
            names.append(name)
            titles.append(title)
            times.append(time[:5])
    data=pd.DataFrame({'时间':times,'内容':titles,'公司':names})
    return data
def get_stock_top_comment_mi(page_size=2):
    '''
    获取股票热门讨论数据,分钟数据，实时数据
    :return:
    '''
    names=[]
    titles = []
    times = []
    for i in tqdm(range(1, page_size)):
        url = 'http://guba.eastmoney.com/default,99_{}.html'.format(i)
        res = requests.get(url=url)
        soup = etree.HTML(res.text)
        for j in range(1,88):
            name=''.join(soup.xpath('//*[@id="main-body"]/div[4]/div[1]/div[3]/div/div[2]/div[1]/ul/li[{}]/span/a[1]/text()'.format(j)))[:-1]
            title=''.join(soup.xpath('//*[@id="main-body"]/div[4]/div[1]/div[3]/div/div[2]/div[1]/ul/li[{}]/span/a[2]/text()'.format(j)))
            time=''.join(soup.xpath('//*[@id="main-body"]/div[4]/div[1]/div[3]/div/div[2]/div[1]/ul/li[{}]/cite[5]/text()'.format(j)))
            names.append(name)
            titles.append(title)
            times.append(time)
    data=pd.DataFrame({'时间':times,'内容':titles,'公司':names})
    return data
def get_stock_top_sentiments_daily(page_size=2):
    '''
    获取股票热门讨论数据,日线数据
    :return:
    '''
    df = get_stock_top_comment_daily(page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    name_list=[]
    data_len=[]
    for i in time_list:
        df1 = df[df['时间'] == i]
        comple_name_list=list(set(df1['公司'].tolist()))
        for j in comple_name_list:
            df2=df1[df1['公司']==j]
            name_list.append(j)
            result = alalysis_text_sentiments_list(df=df2)
            result_list.append(result)
            data_len.append(len(df1['公司'].tolist()))
    #生产时间
    result_time=[]
    for time, m in zip(time_list,data_len):
        for n in range(m):
            result_time.append(time)
    data = pd.DataFrame({'时间':result_time[:len(result_list)],'公司':name_list, '情绪值%': result_list})
    return data
def get_stock_top_sentiments_mi(page_size=2):
    '''
    获取股票热门讨论数据,分钟数据，实时数据
    :return:
    '''
    df = get_stock_top_comment_mi(page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    name_list=[]
    for i in time_list:
        df1 = df[df['时间'] == i]
        name=df1['公司'].tolist()[0]
        name_list.append(name)
        result = alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list,'公司':name_list, '情绪值%': result_list})
    return data
def get_stock_top_comment_xq():
    '''
    雪球讨论热贴
    :return:
    '''
    url='https://xueqiu.com/statuses/hot/listV2.json?'
    params={
        'since_id':'-1',
        'max_id':'1111111111111111',
        'size':15
    }
    headers = {
        'Cookie': 'device_id=8507c57c6ef4efc894b4a4359df27ded; s=c112d21ugo; __utmz=1.1662694809.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); bid=f1b5e01be977a7023f9ec859cdf24ad4_l93j3rgg; __utma=1.1061226918.1662694809.1666187281.1668504618.4; acw_tc=2760827c16696136047044647eab1325f7c8b4614ced3de2fe613618b490c6; Hm_lvt_39f36dNMc5CmshUCPAH1VsCv4A84pGdSyu=1667114671,1667178266,1668504488,1669613608; acw_sc__v2=63844835045dfef6f5d8f905339098afc3291a24; xq_a_token=df4b782b118f7f9cabab6989b39a24cb04685f95; xqat=df4b782b118f7f9cabab6989b39a24cb04685f95; xq_r_token=3ae1ada2a33de0f698daa53fb4e1b61edf335952; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTY3MjE4Njc1MSwiY3RtIjoxNjY5NjEzNzQxNzQyLCJjaWQiOiJkOWQwbjRBWnVwIn0.eOUcaJK15nCSZjigvWMHKLxct9Zx9CtpwMU973uEP5ag67DIyPQrg2dQnMUOqoC1hhquAZJvn2ynY3lanq0xlGicB0Lsr3ScyL949NjKAaT5gDbCnQNJvyObO_x_c3MsYkrJZ_sPE91uMhM3qq9TNn_7tpPG_MEDsQJkPkttX-ciXwaGL22WJrvgnGFE5MuFAJ2UEFwu_rtmMEzeQ0AdT7D9EksR7R2BTB1z8bdbk3X2wdF9I5Zq678Fg91L9NNSvtOIhda-jgZEbRKfsY9MKmC9315GJYz9j8TEMqgqt6JGcP-loVSPBcsbfFJLjHd5ghreKvikUIavCZjLo72FPA; u=431669613768060; Hm_lpvt_39f36dNMc5CmshUCPAH1VsCv4A84pGdSyu=1669613768',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
    }
    res=requests.get(url=url,params=params,headers=headers)
    text=res.json()
    df=pd.json_normalize(text['items'])
    df.rename(columns={'original_status.timeBefore':'时间','original_status.description':'内容','original_status.created_at':'时间1'},inplace=True)
    df1=df[['时间','内容']]
    return df1
def get_stock_top_sentiments_xq():
    '''
    雪球情绪分析
    :return:
    '''
    df=get_stock_top_comment_xq()
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result = alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_stock_7_24_data_daily(page_size=5):
    '''
    东方财富7*24小时数据
    page_size页面数据量，一个页面100个数据
    日线
    :return:
    '''
    data=pd.DataFrame()
    for i in tqdm(range(1,page_size)):
        url='https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_100_{}_.html?'.format(i)
        params={
            'r':'0.6674255877904876',
            '_':'1669624350945'
        }
        res=requests.get(url=url,params=params)
        text=res.text[15:len(res.text)]
        json_text=json.loads(text)
        df=pd.DataFrame(json_text['LivesList'])
        #如果数据太多可以digest将换城simtitle
        df.rename(columns={'showtime':'时间','digest':'内容'},inplace=True)
        df1=df[['时间','内容']]
        df2=df1['时间'].str[:10].tolist()
        df1['时间']=df2
        data=pd.concat([data,df1],ignore_index=True)
    return data
def get_stock_7_24_data_mi(page_size=5):
    '''
    东方财富7*24小时数据，实时数据
    page_size页面数据量，一个页面100个数据
    分钟数据
    :return:
    '''
    data=pd.DataFrame()
    for i in tqdm(range(1,page_size)):
        url='https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_100_{}_.html?'.format(i)
        params={
            'r':'0.6674255877904876',
            '_':'1669624350945'
        }
        res=requests.get(url=url,params=params)
        text=res.text[15:len(res.text)]
        json_text=json.loads(text)
        df=pd.DataFrame(json_text['LivesList'])
        #如果数据太多可以digest将换城simtitle
        df.rename(columns={'showtime':'时间','digest':'内容'},inplace=True)
        df1=df[['时间','内容']]
        data=pd.concat([data,df1],ignore_index=True)
    return data
def get_stock_7_24_sentiments_daily(page_size=5):
    '''
    东方财富7*24小时数据
    情绪分析
    日线
    :param page_size:
    :return:
    '''
    df = get_stock_7_24_data_daily(page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result = alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_stock_7_24_sentiments_mi(page_size=5):
    '''
    东方财富7*24小时数据
    情绪分析
    分钟数据
    :param page_size:
    :return:
    '''
    df = get_stock_7_24_data_mi(page_size=page_size)
    result_list=[]
    for i in df['内容'].tolist():
        text=SnowNLP(i)
        result=text.sentiments
        result_list.append(result*100)
    data=pd.DataFrame({'时间':df['时间'].tolist(),'情绪值%':result_list,'内容':df['内容'].tolist()})
    return data
def get_all_gold_finance_conment_daily(page_size=2):
    '''
    全球财经导读
    日线
    :param page_size:
    :return:
    '''
    data=pd.DataFrame()
    for i in tqdm(range(1,page_size)):
        url='https://np-listapi.eastmoney.com/comm/web/getNewsByColumns?'
        params = {
            'client': 'web',
            'biz': 'web_news_col',
            'column': '790',
            'order': '1',
            'needInteractData': '0',
            'page_index':str(i),
            'page_size': 200,
            'req_trace': '1669631639274',
            'fields': 'code,showTime,title,mediaName,summary,image,url,uniqueUrl',
            'callback': 'jQuery18305095819878601981_1669631639181',
            '_': '1669631639274',
        }
        res=requests.get(url=url,params=params)
        text=res.text[41:len(res.text)-1]
        json_text=json.loads(text)
        df=pd.DataFrame(json_text['data']['list'])
        df.rename(columns={'showTime':'时间','summary':'内容'},inplace=True)
        df1=df[['时间','内容']]
        df2=df1['时间'].str[:10].tolist()
        df1['时间']=df2
        data=pd.concat([data,df1],ignore_index=True)
    return data
def get_all_gold_finance_conment_mi(page_size=2):
    '''
    全球财经导读
    分钟数据
    :param page_size:
    :return:
    '''
    data=pd.DataFrame()
    for i in tqdm(range(1,page_size)):
        url='https://np-listapi.eastmoney.com/comm/web/getNewsByColumns?'
        params = {
            'client': 'web',
            'biz': 'web_news_col',
            'column': '790',
            'order': '1',
            'needInteractData': '0',
            'page_index':str(i),
            'page_size': 200,
            'req_trace': '1669631639274',
            'fields': 'code,showTime,title,mediaName,summary,image,url,uniqueUrl',
            'callback': 'jQuery18305095819878601981_1669631639181',
            '_': '1669631639274',
        }
        res=requests.get(url=url,params=params)
        text=res.text[41:len(res.text)-1]
        json_text=json.loads(text)
        df=pd.DataFrame(json_text['data']['list'])
        df.rename(columns={'showTime':'时间','summary':'内容'},inplace=True)
        df1=df[['时间','内容']]
        data=pd.concat([data,df1],ignore_index=True)
    return data
def get_all_gold_finance_sentiments_daily(page_size=2):
    '''
    全球财经导读日线
    情绪分析
    :param page_size:
    :return:
    '''
    df = get_all_gold_finance_conment_daily(page_size=page_size)
    time_list = sorted(list(set(df['时间'].tolist())))
    result_list = []
    for i in time_list:
        df1 = df[df['时间'] == i]
        result = alalysis_text_sentiments_list(df=df1)
        result_list.append(result)
    data = pd.DataFrame({'时间': time_list, '情绪值%': result_list})
    return data
def get_all_gold_finance_sentiments_mi(page_size=2):
    '''
    全球财经导读分钟数据，实时数据
    情绪分析
    :param page_size:
    :return:
    '''
    df = get_all_gold_finance_conment_mi(page_size=page_size)
    result_list = []
    for i in df['内容'].tolist():
        text = SnowNLP(i)
        result = text.sentiments
        result_list.append(result * 100)
    data = pd.DataFrame({'时间': df['时间'].tolist(), '情绪值%': result_list, '内容': df['内容'].tolist()})
    return data
def get_all_gold_foregn_exchange_conment(page_size=10):
    '''
    外汇实时数据
    数据很多不进行日线分析
    :return:
    '''
    data=pd.DataFrame()
    for i in tqdm(range(1,page_size)):
        url='https://classify-ws.jin10.com:5142/flash?'
        params={
            'channel':'-8200',
            'vip': str(i),
            'classify':'[12]',
            't':'1669634068427',
        }
        res=requests.get(url=url,params=params)
        text=res.json()
        df=pd.DataFrame(text['data'])
        df.rename(columns={'time':'时间','data':'内容'},inplace=True)
        df1=df[['时间','内容']]
        comment_list=[]
        for i in df1['内容'].tolist():
            comment=str(i).split("': '")[1]
            comment_list.append(comment)
        df1['内容']=comment_list
        data=pd.concat([data,df1],ignore_index=True)
    return data
def get_all_gold_foregn_exchange_sentiments(page_size=10):
    '''
    外汇实时数据
    数据很多不进行日线分析
    情绪分析
    :return:
    '''
    df = get_all_gold_foregn_exchange_conment(page_size=page_size)
    result_list = []
    for i in df['内容'].tolist():
        text = SnowNLP(i)
        result = text.sentiments
        result_list.append(result * 100)
    data = pd.DataFrame({'时间': df['时间'].tolist(), '情绪值%': result_list, '内容': df['内容'].tolist()})
    return data
def get_all_gold_futures_conment(page_size=10):
    '''
    实时期货新闻
    数据很多不进行日线分析
    :return:
    '''
    data=pd.DataFrame()
    for i in tqdm(range(1,page_size)):
        url='https://classify-ws.jin10.com:5142/flash?'
        params={
            'channel':'-8200',
            'vip':'1',
            'classify':'[36]',
            't':'1669635998851',
        }
        res=requests.get(url=url,params=params)
        text=res.json()
        df=pd.DataFrame(text['data'])
        df.rename(columns={'time':'时间','data':'内容'},inplace=True)
        df1=df[['时间','内容']]
        comment_list=[]
        for i in df1['内容'].tolist():
            comment=str(i).split("': '")[1]
            comment_list.append(comment)
        df1['内容']=comment_list
        data=pd.concat([data,df1],ignore_index=True)
    return data
def get_all_gold_futures_sentiments(page_size=10):
    '''
    实时期货新闻
    情绪分析
    数据很多不进行日线分析
    :return:
    '''
    df = get_all_gold_futures_conment(page_size=page_size)
    result_list = []
    for i in df['内容'].tolist():
        text = SnowNLP(i)
        result = text.sentiments
        result_list.append(result * 100)
    data = pd.DataFrame({'时间': df['时间'].tolist(), '情绪值%': result_list, '内容': df['内容'].tolist()})
    return data