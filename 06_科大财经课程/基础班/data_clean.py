import pandas as pd 
import numpy as np
from scipy import stats
from sklearn import linear_model
import statsmodels.api as sm
from rqdatac import *
from rqfactor import *
from rqfactor.notebook import *
from rqfactor.extension import *
init()
import rqdatac


# 再次定义函数：计算最大回撤
def maxdrawdown(arr):
    '''
    输入：净值序列
    输出：最大回撤
    '''
    # 最大回撤结束点
    i = np.argmax((np.maximum.accumulate(arr) - arr)/np.maximum.accumulate(arr))
    # 开始点
    j = np.argmax(arr[:i]) # start of period
    # 输出回撤值
    return (1-arr[i]/arr[j])

# 设置函数：计算净值曲线的绩效指标
def get_Performance_analysis(T,year_day = 252):
    '''
    输入：净值序列 和基准净值序列
    
    输出：绩效指标
    '''
    
    # 新高日期数 #突破能力
    max_T = 0
    # 循环净值
    for s in range(2,len(T)):
        # 节点划分
        l = T[:s]
        # 判断当前节点为最大值
        if l[-1] > l[:-1].max():
            # 新高日数+1
            max_T += 1
            
    # 净值新高天数占比
    max_day_rate = max_T/(len(T)-1)
    max_day_rate = round(max_day_rate*100,2)
    
    # 获取最终净值
    net_values = round(T[-1],4)
    # 计算算术年化收益率
    year_ret_mean = T.pct_change().dropna().mean()*year_day
    year_ret_mean = round(year_ret_mean*100,2)
    
    # 计算几何年化收益率
    year_ret_sqrt = net_values**(year_day/len(T))-1
    year_ret_sqrt = round(year_ret_sqrt*100,2)
    
    # 计算年化波动率
    volitiy = T.pct_change().dropna().std()*np.sqrt(year_day)
    volitiy = round(volitiy*100,2)
    
    #计算夏普，无风险收益率记3%
    Sharpe = (year_ret_sqrt - 0.03)/volitiy
    Sharpe = round(Sharpe,2)

    # 计算最大回撤
    downlow = maxdrawdown(T)
    downlow = round(downlow*100,2)
    
    # 输出
    return [net_values,year_ret_sqrt,downlow,Sharpe,volitiy,max_day_rate]
#------------------------------------------------------------------------

def get_new_stock_filter(stock_list,date_list, newly_listed_threshold=120):

    listed_date_list = [rqdatac.instruments(stock).listed_date for stock in stock_list]        
    newly_listed_window = pd.Series(index=stock_list, data=[rqdatac.get_next_trading_date(listed_date, n=newly_listed_threshold) for listed_date in listed_date_list])     
    newly_listed_label = pd.DataFrame(index=date_list, columns=stock_list, data=0.0)

    # 上市时间短语指定窗口的新股标记为1，否则为0
    for stock in newly_listed_window.index:
        newly_listed_label.loc[:newly_listed_window.loc[stock], stock] = 1.0
                    #剔除新股
    newly_listed_label.replace(1,True,inplace = True)
    newly_listed_label.replace(0,False,inplace = True)
    newly_listed_label = newly_listed_label.shift(-1).fillna(method = 'ffill')
    print('剔除新股已构建')

    return newly_listed_label

def get_st_filter(stock_list,date_list):
    # 对st股票做标记,st=1,非st=0

    st_filter = rqdatac.is_st_stock(stock_list,date_list[0],date_list[-1]).astype('float').reindex(columns=stock_list,index = date_list)                                #剔除ST
    st_filter.replace(1,True,inplace = True)
    st_filter.replace(0,False,inplace = True)
    st_filter = st_filter.shift(-1).fillna(method = 'ffill')
    print('剔除ST已构建')

    return st_filter

def get_suspended_filter(stock_list,date_list):

    suspended_filter = rqdatac.is_suspended(stock_list,date_list[0],date_list[-1]).astype('float').reindex(columns=stock_list,index=date_list)

    suspended_filter.replace(1,True,inplace = True)
    suspended_filter.replace(0,False,inplace = True)
    suspended_filter = suspended_filter.shift(-1).fillna(method = 'ffill')
    print('剔除停牌已构建')

    return suspended_filter

def get_limit_up_down_filter(stock_list,date_list):

    # 涨停则赋值为1,反之为0    
    df = pd.DataFrame(index = date_list,columns=stock_list,data=0.0)
    total_price = rqdatac.get_price(stock_list,date_list[0],date_list[-1],adjust_type='none')

    for stock in stock_list:

        try:
            price = total_price.loc[stock]
        except:
            print('no stock data:',stock)
            df[stock] = np.nan
            continue                    

        # 如果close == limit_up or limit down,则股票涨停或者跌停        
        condition = ((price['open'] == price['limit_up']))#|(price['close'] == price['limit_down']))        
        if condition.sum()!=0:
            df.loc[condition.loc[condition==True].index,stock] = 1.0

    df.replace(1,True,inplace = True)
    df.replace(0,False,inplace = True)
    df = df.shift(-1).fillna(method = 'ffill')
    print('剔除开盘涨停已构建')

    return df

# 数据清洗函数 -----------------------------------------------------------
# MAD:中位数去极值
def filter_extreme_MAD(series,n): 
    median = series.median()
    new_median = ((series - median).abs()).median()
    return series.clip(median - n*new_median,median + n*new_median)

def winsorize_std(series, n=3):
    mean, std = series.mean(), series.std()
    return series.clip(mean - std*n, mean + std*n)


def winsorize_percentile(series, left=0.025, right=0.975):
    lv, rv = np.percentile(series, [left*100, right*100])
    return series.clip(lv, rv)

def neutralization(factor):#,order_book_ids,datetime_period,start_date,end_date
    #输入数据需要stack，双重索引【交易日期，股票代码】
    order_book_ids = factor.columns.tolist()
    datetime_period = factor.index.tolist()
    start_date = datetime_period[0]
    end_date = datetime_period[-1]
    #获取市值数据
    f = Factor('market_cap_3')
    df_market_cap_whole = execute_factor(f,order_book_ids,start_date,end_date).stack()
    df_market_cap_whole = np.log(df_market_cap_whole)
    #获取行业暴露度
    industry_df = get_industry_exposure(order_book_ids,datetime_period)
    #合并因子
    cfoa_industy_market = pd.concat([factor.stack(),df_market_cap_whole,industry_df],axis = 1)
    cfoa_industy_market.dropna(inplace = True)
    #OLS回归
    cfoa_industy_market = cfoa_industy_market.reset_index().set_index(['level_0'])
    cfoa_result = pd.DataFrame()
    for i in datetime_period:
        cfoa_day = cfoa_industy_market.loc[i]    #截面数据做回归
        cfoa_day = cfoa_day.reset_index().set_index(['level_0','level_1'])
        x = cfoa_day.iloc[:,1:]   #市值/行业
        y = cfoa_day.iloc[:,0]    #因子值
        cfoa_day_result = pd.DataFrame(sm.OLS(y.astype(float),x.astype(float),hasconst=False, missing='drop').fit().resid)
        cfoa_result = pd.concat([cfoa_result,cfoa_day_result],axis = 0)
    return cfoa_result

def get_industry_exposure(order_book_ids,datetime_period):
    zx2019_industry = rqdatac.client.get_client().execute('__internal__zx2019_industry')
    df = pd.DataFrame(zx2019_industry)
    df.set_index(['order_book_id', 'start_date'], inplace=True)
    df = df['first_industry_name'].sort_index()
    print('中信行业数据已获取')

    #构建动态行业数据表格
    index = pd.MultiIndex.from_product([order_book_ids, datetime_period], names=['order_book_id', 'datetime'])
    pos = df.index.searchsorted(index, side='right') - 1
    index = index.swaplevel()   # level change (oid, datetime) --> (datetime, oid)
    result = pd.Series(df.values[pos], index=index)
    result = result.sort_index()
    print('动态行业数据已构建')

    #生成行业虚拟变量
    return pd.get_dummies(result)

# 单因子检测函数 -----------------------------------------------------------

# IC计算 
def Factor_Return_N_IC(factor,n,Rank_IC = True):

    date_list_whole = sorted(list(set(factor.index.get_level_values(0))))
    start_date = date_list_whole[0]
    end_date = date_list_whole[-1]
    stock_list = sorted(list(set(factor.index.get_level_values(1))))

    close = get_price(stock_list, start_date=start_date, end_date=end_date,
                      frequency='1d',fields='close', adjust_type='pre', 
                      skip_suspended =False, market='cn', expect_df=True).close.unstack().T 
    close = close.pct_change(n).shift(-n).stack()
    close = pd.concat([close,factor],axis =1).dropna().reset_index()
    close.columns = ['date','stock','change_days','factor']
    if Rank_IC == True:
        rank_ic = close.groupby('date')['change_days','factor'].corr(method = 'spearman').reset_index().set_index(['date'])
        return rank_ic[rank_ic.level_1 == 'factor'][['change_days']]
    else:
        normal_ic = close.groupby('date')['change_days','factor'].corr(method = 'pearson').reset_index().set_index(['date'])
        return normal_ic[normal_ic.level_1 == 'factor'][['change_days']]

def ic_ir(x):
    t_stat, p_value = stats.ttest_1samp(x, 0)
    return ['IC mean:{}'.format(round(x.mean()[0],4)),
            'IC std:{}'.format(round(x.std()[0],4)),
            'IR:{}'.format(round(x.mean()[0]/x.std()[0],4)),
            'IC>0:{}'.format(round(len(x[x>0].dropna())/len(x),4)),
            'ABS_IC>2%:{}'.format(round(len(x[abs(x) > 0.02].dropna())/len(x),4)),
            't_stat:{}'.format(t_stat.round(4)[0]),
            'p_value:{}'.format(p_value.round(4)[0]),
            'skew:{}'.format(stats.skew(x).round(4)[0]),
            'kurtosis:{}'.format(stats.kurtosis(x).round(4)[0]),
           ]


#### 分层效应
def group_5(factor,n):
    '''
    factor: 因子值 stack形式
    n = 调仓日期
    '''
    date_list_whole = sorted(list(set(factor.index.get_level_values(0))))
    start_date = date_list_whole[0]
    end_date = date_list_whole[-1]
    stock_list = sorted(list(set(factor.index.get_level_values(1))))

    current_return = get_price(stock_list,get_previous_trading_date(start_date,1,market='cn'),end_date,
                                '1d','close','pre',False,True).close.unstack('order_book_id').pct_change().dropna(axis = 0,how = 'all').stack()
    group = pd.concat([factor,current_return],axis = 1).dropna()
    group.reset_index(inplace = True)
    group.columns = ['date','stock','factor','current_renturn']
    group1_5_period = pd.DataFrame()
    G1_temp,G2_temp,G3_temp,G4_temp,G5_temp = [],[],[],[],[]
    turnover = pd.DataFrame()
    for i in range(0,len(date_list_whole),n):
        single = group[group.date == date_list_whole[i]].sort_values(by = 'factor')
        G1 = single.iloc[:int(len(single)*0.2)].stock.tolist()
        G2 = single.iloc[int(len(single)*0.2):int(len(single)*0.4)].stock.tolist()
        G3 = single.iloc[int(len(single)*0.4):int(len(single)*0.6)].stock.tolist()
        G4 = single.iloc[int(len(single)*0.6):int(len(single)*0.8)].stock.tolist()
        G5 = single.iloc[int(len(single)*0.8):].stock.tolist()
        if i != 0:
            temp = pd.DataFrame([len(set(G1).difference(set(G1_temp)))/2/len(set(G1_temp)),
             len(set(G2).difference(set(G2_temp)))/2/len(set(G2_temp)),
             len(set(G3).difference(set(G3_temp)))/2/len(set(G3_temp)),
             len(set(G4).difference(set(G4_temp)))/2/len(set(G4_temp)),
             len(set(G5).difference(set(G5_temp)))/2/len(set(G5_temp))
            ],index = ['G1','G2','G3','G4','G5'],columns = [date_list_whole[i]]).T
            turnover = pd.concat([turnover,temp],axis = 0)
            G1_temp,G2_temp,G3_temp,G4_temp,G5_temp =G1,G2,G3,G4,G5
        else:
            G1_temp,G2_temp,G3_temp,G4_temp,G5_temp =G1,G2,G3,G4,G5
            
        if i < len(date_list_whole)-n:
            period = group[group.date.isin(date_list_whole[i:i+n])]
        else:
            period = group[group.date.isin(date_list_whole[i:])]
            
        group1 = period[period.stock.isin(G1)].set_index(['date','stock']).current_renturn.unstack('stock').mean(axis = 1)
        group2 = period[period.stock.isin(G2)].set_index(['date','stock']).current_renturn.unstack('stock').mean(axis = 1)
        group3 = period[period.stock.isin(G3)].set_index(['date','stock']).current_renturn.unstack('stock').mean(axis = 1)
        group4 = period[period.stock.isin(G4)].set_index(['date','stock']).current_renturn.unstack('stock').mean(axis = 1)
        group5 = period[period.stock.isin(G5)].set_index(['date','stock']).current_renturn.unstack('stock').mean(axis = 1)
        group1_5 = pd.concat([group1,group2,group3,group4,group5],axis = 1)
        group1_5_period = pd.concat([group1_5_period,group1_5],axis = 0)
        print('\r 当前：{} / 总量：{}'.format(i,len(date_list_whole)),end='')
    
    return group1_5_period,turnover