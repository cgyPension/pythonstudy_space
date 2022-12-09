import pandas as pd
from factors import data
from factors import factors
from factors import model
from factors import backtest
from factors.strategies import *


df=data.get_all_index_data()
bench=data.get_index_data(index="000300",start_date="20200101",end_date="20220330",renew=False)

df=factors.getTA(df)
df['label']=df.groupby('symbol')['close'].shift(10)
df['label']=df['close']/df['label']

df_train,df_valid,df_pred=model.datasplit(df,train_end='2019-01-01',valid_end='2020-01-01')
model.lgbtrain(df_train,df_valid,label='label')
preds=model.lgbpred(df_pred,label='label')
preds['score']=preds['pred']
preds['rank']=preds.groupby('date')['pred'].rank()
preds['signal']=preds.apply(lambda x: 1 if x['rank']<=10 else 0 ,axis=1)

returns=backtest.test(preds,Top10Strategy)
backtest.analysis(returns,bench)