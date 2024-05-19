from environment import Environment
from factor.factors import ROCSpread
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

env = Environment("ashare")
env.load_bar_data_from_csv("ashare_bar_data.csv")
for lb in [6, 12]:
    for ma in [6, 12]:
        factor = ROCSpread(env, "d", lb, ma)
        env.load_factor(factor)

env.factor_analysis("roc_spread")
