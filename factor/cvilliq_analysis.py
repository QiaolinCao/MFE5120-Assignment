from environment import Environment
from factor.factors import CVILLIQ
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

env = Environment("ashare")
env.load_bar_data_from_csv("ashare_bar_data.csv")


for lb_window in [10, 15, 20]:
    factor = CVILLIQ(env, "d", lb_window)
    env.load_factor(factor)

env.factor_analysis("CVILLIQ")
