# 导入函数库
import jqdata
import numpy as np
import pandas as pd
import math
from statsmodels import regression
import statsmodels.api as sm
import matplotlib.pyplot as plt


def winsorize(factor, std=3, have_negative=True):
    '''
    去极值函数
    factor:以股票code为index，因子值为value的Series
    std为几倍的标准差，have_negative 为布尔值，是否包括负值
    输出Series
    '''
    r = factor.dropna().copy()
    if have_negative == False:
        r = r[r >= 0]
    else:
        pass
    # 取极值
    edge_up = r.mean() + std * r.std()
    edge_low = r.mean() - std * r.std()
    r[r > edge_up] = edge_up
    r[r < edge_low] = edge_low
    return r


# 标准化函数：
def standardize(s, ty=2):
    '''
    s为Series数据
    ty为标准化类型:1 MinMax,2 Standard,3 maxabs
    '''
    data = s.dropna().copy()
    if int(ty) == 1:
        re = (data - data.min()) / (data.max() - data.min())
    elif ty == 2:
        re = (data - data.mean()) / data.std()
    elif ty == 3:
        re = data / 10 ** np.ceil(np.log10(data.abs().max()))
    return re


# 中性化函数
# 传入：mkt_cap：以股票为index，市值为value的Series,
# factor：以股票code为index，因子值为value的Series,
# 输出：中性化后的因子值series
def neutralization(factor, mkt_cap=False, industry=True):
    y = factor
    if type(mkt_cap) == pd.Series:
        LnMktCap = mkt_cap.apply(lambda x: math.log(x))
        if industry:  # 行业、市值
            dummy_industry = get_industry_exposure(factor.index)
            x = pd.concat([LnMktCap, dummy_industry.T], axis=1)
        else:  # 仅市值
            x = LnMktCap
    elif industry:  # 仅行业
        dummy_industry = get_industry_exposure(factor.index)
        x = dummy_industry.T
    result = sm.OLS(y.astype(float), x.astype(float)).fit()
    return result.resid


# 为股票池添加行业标记,return df格式 ,为中性化函数的子函数
def get_industry_exposure(stock_list):
    df = pd.DataFrame(index=jqdata.get_industries(name='sw_l1').index, columns=stock_list)
    for stock in stock_list:
        try:
            df[stock][get_industry_code_from_security(stock)] = 1
        except:
            continue
    return df.fillna(0)  # 将NaN赋为0


# 查询个股所在行业函数代码（申万一级） ,为中性化函数的子函数
def get_industry_code_from_security(security, date=None):
    industry_index = jqdata.get_industries(name='sw_l1').index
    for i in range(0, len(industry_index)):
        try:
            index = get_industry_stocks(industry_index[i], date=date).index(security)
            return industry_index[i]
        except:
            continue
    return u'未找到'


# a=get_industry_code_from_security('600519.XSHG', date=pd.datetime.today())
# print a

# stocks_industry=get_industry_exposure(stocks)
# print stocks_industry

def get_win_stand_neutra(stocks):
    h = get_fundamentals(query(valuation.pb_ratio, valuation.code, valuation.market_cap) \
                         .filter(valuation.code.in_(stocks)))
    stocks_pb_se = pd.Series(list(h.pb_ratio), index=list(h.code))
    stocks_pb_win_standse = standardize(winsorize(stocks_pb_se))
    stocks_mktcap_se = pd.Series(list(h.market_cap), index=list(h.code))
    stocks_neutra_se = neutralization(stocks_pb_win_standse, stocks_mktcap_se)
    return stocks_neutra_se


# 对沪深300成分股完成
stocks = get_index_stocks('000300.XSHG')
print
get_win_stand_neutra(stocks)