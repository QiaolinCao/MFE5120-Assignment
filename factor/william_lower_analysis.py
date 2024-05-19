from environment import Environment
from factor.factors import WilliamsLowerShadow
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

env = Environment("ashare")
env.load_bar_data_from_csv("ashare_bar_data.csv")


for lb_window in [5, 20, 30]:
    factor = WilliamsLowerShadow(env, "d", lb_window)
    env.load_factor(factor)

env.factor_analysis("william_lower_shadow")
