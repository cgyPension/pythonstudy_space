import os
import sys
# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import numpy as np
import pandas as pd
from factor_config import *
import statsmodels.api as sm
# from tqdm import tqdm
import math


def cal_rank_ic_ir(all_data, factor_info,):
    #  涉及策略，我删了

    return all_data


# 横截面
# pct：是否以排名的百分比显示排名；所有排名和最大排名的百分比
def cal_rank_by_cross(df, factor, pct_enable=False):
    # ===数据预处理
    df = df.set_index(['交易日期', '股票代码']).sort_index()
    factor_rank = factor + '_rank'
    # 横截面排名  pct：bool，默认为False，是否以百分比形式显示返回的排名。
    # ascending=True 表示升序排列 False是降序
    df[factor_rank] = df.groupby('交易日期')[factor].rank(ascending=True, pct=True)
    # df[factor_rank] = df.groupby('交易日期')[factor_rank].apply(lambda x: x.fillna(x.median()))
    valua_na = df[factor_rank].median()
    df[factor_rank] = df.groupby('交易日期')[factor_rank].fillna(value=valua_na)

    df.reset_index(inplace=True)

    return df


def cal_step_data(df, factor, per=20):
    mean_ret_next = df["下周期涨跌幅"].mean()

    factor_rank = factor + '_rank'

    # print("rows:",rows)
    border = [0.01 * per * i for i in range(1000) if 0.01 * per * i <= 1.0]
    piecewise = [(border[n], border[n + 1]) for n in range(len(border) - 1)]

    data = {}
    for pie_min, pie_max in piecewise:
        name = "[%.2f,%.2f)" % (pie_min, pie_max)
        tmp_df = pd.DataFrame()
        # data[name] = df[(df[bar] >= pie_min) & (df[bar] <  pie_max)]["ret_next"].median() / medin_ret_next - 1 # 中位数
        neutralization_mean = True  # 中性化
        if (neutralization_mean):
            data[name] = df[(df[factor_rank] >= pie_min) & (df[factor_rank] < pie_max)][
                             "下周期涨跌幅"].mean() / mean_ret_next - 1  # 平均数
        else:
            logic_df = (df[factor_rank] >= pie_min) & (df[factor_rank] < pie_max)
            tmp_df = df[logic_df]
            data[name] = tmp_df["下周期涨跌幅"].mean() - 1  # 平均数
    return data


def get_ic_recession(df, factor_name):
    """
    计算因子衰退
    """
    recession_x = []
    recession_y = []
    analysis_df = pd.DataFrame()

    # i = 1, shift = 1-1 = 0,也就是下一期
    # for i in range(1, 13):
    for i in range(1, 7):
        factor_rec_x_name = f'下{i}期涨跌幅'
        factor_rec_y_name = f'IC衰退_{i}'
        # df[factor_rec_x_name] = df.groupby("股票代码")["下周期涨跌幅"].apply(lambda x: x.shift(1 - i))
        df[factor_rec_x_name] = df.groupby("股票代码")["下周期涨跌幅"].shift(1 - i)

        # https://blog.csdn.net/lost0910/article/details/104632889
        # spearman similar rank_corr, spearman is corr.
        analysis_df[factor_rec_y_name] = df.groupby("交易日期")[[factor_name, factor_rec_x_name]].apply(
            lambda x: x[factor_name].corr(x[factor_rec_x_name], method="spearman"))

        # tmp_df = np_pearson_corr(df, factor_name, factor_rec_x_name,
        #                                                   method="spearman")
        recession_x.append(factor_rec_x_name)
        recession_y.append(analysis_df[factor_rec_y_name].mean())

    return recession_x, recession_y


def cal_factor_score(df, factor):
    # score_list = []

    score_dic = {}

    ret, stat = regression_testing(df, factor, method='OLS')
    # print(stat)

    score_dic['T_ABS均值'] = stat['t值绝对值的均值']
    score_dic['T_ABS>2(%)'] = stat['t值绝对值大于等于2的概率'][-1]

    ret, stat = ic_ir_testing(df, factor, method='spearman')
    # print(stat)

    score_dic['IC_均值'] = stat['IC_mean']
    score_dic['IC_IR'] = stat['IC_mean_IR']
    score_dic['IC>0.00(%)'] = stat['IC_prob_0.00']
    score_dic['IC>0.02(%)'] = stat['IC_prob_0.02']
    score_dic['IC>0.05(%)'] = stat['IC_prob_0.05']

    return list(score_dic.values())


def regression_testing(df, factor, date='交易日期', target='下周期涨跌幅', method='RLM'):
    assert method in ['OLS', 'RLM']

    # 每个时间截面，因子标准化，target对factor做回归, 得到因子收益：factor的斜率
    res = pd.DataFrame()

    for gid, group in df.groupby(date):
        y = group[target]
        temp_fac = group[factor]
        temp_mean = group[factor].mean()
        temp_std = group[factor].std()
        # X = (group[factor] - group[factor].mean()) / group[factor].std()

        if math.isnan(temp_std):
            print("is np.nan")
            continue

        X = (temp_fac - temp_mean) / temp_std
        X = sm.add_constant(X)
        if method == 'OLS':
            model = sm.OLS(y, X).fit()
        elif method == 'RLM':
            model = sm.RLM(y, X, M=sm.robust.norms.HuberT()).fit()
        else:
            raise ValueError

        res.loc[gid, 'factor_ret'] = model.params[factor]
        res.loc[gid, 'factor_t'] = model.tvalues[factor]

    res['factor_net'] = res['factor_ret'].cumsum()
    stat = pd.DataFrame()
    # 因子平均收益
    ret_mean = res['factor_ret'].mean()
    stat.loc[factor, 'factor_ret_mean'] = ret_mean
    # 因子收益t值
    stat.loc[factor, 'factor_ret_t'] = np.abs(ret_mean) / res['factor_ret'].std()

    # t值绝对值均值
    # stat.loc[factor, 'factor_t_abs'] = res['factor_t'].abs().mean()

    stat.loc[factor, 't值绝对值的均值'] = res['factor_t'].abs().mean()
    # t值绝对值大于等于2的概率---回归假设检验的t值
    stat.loc[factor, 't值绝对值大于等于2的概率'] = (res['factor_t'].abs() > 2).sum() / len(res['factor_t'])

    return res, stat


def ic_ir_testing(df, factor, date='交易日期', target='下周期涨跌幅', method='spearman'):
    assert method in ['pearson', 'spearman']

    res = pd.DataFrame()
    for gid, group in df.groupby(date):
        target_val = group[target]
        factor_val = group[factor]

        res.loc[gid, 'IC'] = factor_val.corr(target_val, method=method)

    res['cum_IC'] = res['IC'].cumsum()
    stat = pd.DataFrame()
    ic_mean = res['IC'].mean()
    stat.loc[factor, 'IC_mean'] = ic_mean  # IC均值
    stat.loc[factor, 'IC_mean_IR'] = np.abs(ic_mean) / res['IC'].std()  # IR

    if ic_mean >= 0:
        stat.loc[factor, 'IC_prob_0.00'] = (res['IC'] >= 0.00).sum() / len(res['IC'])
        stat.loc[factor, 'IC_prob_0.02'] = (res['IC'] >= 0.02).sum() / len(res['IC'])
        stat.loc[factor, 'IC_prob_0.05'] = (res['IC'] >= 0.05).sum() / len(res['IC'])
    else:
        stat.loc[factor, 'IC_prob_0.00'] = (res['IC'] < 0.00).sum() / len(res['IC'])
        stat.loc[factor, 'IC_prob_0.02'] = (res['IC'] <= -0.02).sum() / len(res['IC'])
        stat.loc[factor, 'IC_prob_0.05'] = (res['IC'] <= -0.05).sum() / len(res['IC'])

    return res, stat
