# 已知：总体均值3.30kg，样本数量35，样本均值3.42kg，标准差0.40kg。问：样本均值与总体均值是否有差异？
# 
# 提出原假设和备择假设：
# H0: 样本均值与总体均值相等，无差异
# H1: 样本均值与总体均值不相等，存在差异
from scipy.stats import ttest_1samp
from scipy import stats

# 1. 生成均值为3.42，标准差为0.40的样本
samp = stats.norm.rvs(loc=3.42, scale=0.40, size=35)
print(f"samp's type is {type(samp)}, size = {len(samp)}, mean = {samp.mean()}, std = {samp.std()}")
# out: samp's type is <class 'numpy.ndarray'>, size = 35, mean = 3.3751406109024398, std = 0.3993899466032266

# 2. 第一个参数是样本数据，第二个参数是已知总体均数
t, p = ttest_1samp(samp, 3.42)
print(f"t = {t}, p = {p}")
# t = -1.2655105930679713, p = 0.21429231177437397

# 结论：p = 0.214 > 0.05，表明差异无统计学意义，按 α = 0.05的水准接受H0，
# 即根据现有的样本信息，尚不能认为样本均值与总体均值存在差异