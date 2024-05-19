from environment import Environment
from factor.factors import VPT
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

env = Environment("ashare")
env.load_bar_data_from_csv("ashare_bar_data.csv")

factor = VPT(env)
env.load_factor(factor)

env.factor_analysis("VPT")