from abc import ABC

import numpy as np
from pandas import Series
from typing import Optional, TYPE_CHECKING


class FactorTemplate(ABC):

    name = None

    def __init__(self, environment):
        self.environment = environment
        self.factor_series: Optional[Series] = None

    def calculate_factor(self) -> Series:
        """计算因子值；未标准化；返回的dataframe为alphalens支持的multiindex series"""
        pass

    def factor_standardization(self) -> None:
        """因子z值标准化"""
        mean = self.factor_series.mean()
        std = self.factor_series.std()

        self.factor_series = (self.factor_series - mean) / std

    def get_factor_series(self) -> Series:
        if self.factor_series is None:
            self.calculate_factor()
            self.factor_standardization()
        return self.factor_series


class MomentumFactor(FactorTemplate):
    """
    A股反转强于动量；
    因此计算因子值后，乘以-1
    """
    name = "momentum"

    def __init__(self, environment, frequency: str, looking_back: int) -> None:
        super().__init__(environment)
        self.freq = frequency
        self.lb_window = looking_back
        self.name = f"{MomentumFactor.name }_{looking_back}{frequency}"

    def calculate_factor(self) -> None:
        """log_return需为行索引为日期，列索引为股票代码"""

        # 计算对数收益率
        log_return = self.environment.get_log_return()

        momentum = log_return.rolling(self.lb_window).sum()
        momentum = momentum.stack()

        self.factor_series = -momentum


class Volatility(FactorTemplate):
    """
    原始因子值预测方向为负；因此计算后乘以-1
    因子ic满足标准，但稳定性不强，没有明显单调性
    """
    name = "volatility"

    def __init__(self, environment, frequency: str, looking_back: int) -> None:
        super().__init__(environment)
        self.freq = frequency
        self.lb_window = looking_back
        self.name = f"{Volatility.name}_{looking_back}{frequency}"

    def calculate_factor(self) -> None:
        """log_return需为行索引为日期，列索引为股票代码"""

        # 计算对数收益率
        log_return = self.environment.get_log_return()

        volatility = log_return.rolling(self.lb_window).std()
        volatility = volatility.stack()

        self.factor_series = -volatility


class ROCSpread(FactorTemplate):
    """
    变动率指标
    ROC与ROCMA的差值；
    # 需满足突破，否则因子值设为0
    参考：https://zhuanlan.zhihu.com/p/42834842；
    原始因子值与下期收益负相关，因此计算出来后乘-1
    经测试，ic值不满足标准
    """
    name = "ROC"

    def __init__(self, environment, frequency: str, looking_back: int=12, ma_window: int=6) -> None:
        super().__init__(environment)

        self.freq = frequency
        self.lb_days = looking_back  # 基准是多少日前的收盘价
        self.ma_window = ma_window   # ma窗口宽度
        self.name = f"{ROCSpread.name}_{looking_back}n_{ma_window}m"

    def calculate_factor(self) -> None:
        """close_df需为行索引为日期，列索引为股票代码"""
        close_df = self.environment.get_close()

        roc_df = (close_df - close_df.shift(self.lb_days)) / close_df.shift(self.lb_days) * 100
        roc_ma = roc_df.rolling(self.ma_window).mean()
        roc_spread = roc_df - roc_ma

        """
        # 计算roc_spread是否与上日异号，异号说明突破，为1
        sign_today = np.sign(roc_spread)
        sign_yesterday = np.sign(roc_spread.shift(1))
        comparison = sign_today * sign_yesterday  # 若异号或等于0， 达成cross条件
        is_cross = comparison.applymap(lambda x: 1 if x <= 0 else 0)
        roc_spread *= is_cross
        """   # 上述计算会丢掉过多数据，不满足分析要求，弃用

        roc_spread = roc_spread.stack()

        self.factor_series = -roc_spread


class CVILLIQ(FactorTemplate):
    """
    Akbas, F. (2011). The volatility of liquidity and expected stock returns. Texas A&M University.
    经测试， IC值不满足标准
    原始值乘以-1
    """
    name = "CVILLIQ"
    description = "非流动性变异系数"

    def __init__(self, environment, frequency: str, looking_back: int = 20) -> None:
        super().__init__(environment)

        self.freq = frequency
        self.lb_window = looking_back  # 基准是多少日前的收盘价
        self.name = f"{CVILLIQ.name}_{looking_back}{frequency}"

    def calculate_factor(self) -> None:
        """log_return, volume_df需为行索引为日期，列索引为股票代码"""
        log_return = self.environment.get_log_return()
        abs_return = np.abs(log_return)
        volume_df = self.environment.get_volume()

        illiq_df = abs_return / volume_df

        std = illiq_df.rolling(self.lb_window).std()
        mean = illiq_df.rolling(self.lb_window).mean()

        cvilliq = std / mean
        cvilliq = cvilliq.stack()

        self.factor_series = -cvilliq


class VPT(FactorTemplate):
    name = "VPT"
    description = "量价趋势因子"

    def __init__(self, environment, ) -> None:
        super().__init__(environment)

    def calculate_factor(self) -> None:
        close_df = self.environment.get_close()
        close_pct_return = close_df.pct_change()
        volume = self.environment.get_volume()

        increment = close_pct_return * volume
        increment.iloc[0] = 0

        vpt = increment.fillna(0).cumsum()
        vpt = vpt.stack()

        self.factor_series = -vpt


class AbsRetNight(FactorTemplate):
    """
    参考因子日历
    因子ic值满足标准，但稳定性、单调性不足
    """
    name = "abs_ret_overnight"
    description = """隔夜跳空因子是隔夜收益率绝对值的累加, 代表过去一段时间内隔夜累计跳空的幅度，
                    与未来收益负相关。隔夜累计跳空幅度越大，未来收益越差"""

    def __init__(self, environment, frequency: str, lb_window: int) -> None:
        super().__init__(environment)

        self.lb_window = lb_window
        self.name = f"{AbsRetNight.name}_{lb_window}{frequency}"

    def calculate_factor(self) -> None:
        open_df = self.environment.get_open()
        close_yesterday = self.environment.get_close().shift(1)
        overnight_ret = np.log(open_df / close_yesterday)
        abs_return = np.abs(overnight_ret)
        cul_abs_ret = abs_return.rolling(self.lb_window).sum()
        cul_abs_ret = cul_abs_ret.stack()

        self.factor_series = -cul_abs_ret


class WilliamsUpperShadow(FactorTemplate):
    """
    参考：https://github.com/hugo2046/QuantsPlaybook/tree/master/B-%E5%9B%A0%E5%AD%90%E6%9E%84%E5%BB%BA%E7%B1%BB/%E4%B8%8A%E4%B8%8B%E5%BD%B1%E7%BA%BF%E5%9B%A0%E5%AD%90
    """
    name = "william_upper_shadow"
    description = "标准化威廉上影线"

    def __init__(self, environment, frequency: str, lb_window: int) -> None:
        super().__init__(environment)

        self.lb_window = lb_window
        self.name = f"{WilliamsUpperShadow.name}_{lb_window}{frequency}"

    def calculate_factor(self) -> None:
        high_df = self.environment.get_high()
        close_df = self.environment.get_close()

        will_upper = high_df - close_df

        std_will_upper = will_upper / will_upper.rolling(self.lb_window).mean()
        std_will_upper = std_will_upper.stack()

        self.factor_series = std_will_upper


class WilliamsLowerShadow(FactorTemplate):
    """
    参考：https://github.com/hugo2046/QuantsPlaybook/tree/master/B-%E5%9B%A0%E5%AD%90%E6%9E%84%E5%BB%BA%E7%B1%BB/%E4%B8%8A%E4%B8%8B%E5%BD%B1%E7%BA%BF%E5%9B%A0%E5%AD%90
    """
    name = "william_lower_shadow"
    description = "标准化威廉下影线"

    def __init__(self, environment, frequency: str, lb_window: int) -> None:
        super().__init__(environment)

        self.lb_window = lb_window
        self.name = f"{WilliamsLowerShadow.name}_{lb_window}{frequency}"

    def calculate_factor(self) -> None:
        low_df = self.environment.get_low()
        close_df = self.environment.get_close()

        will_lower = close_df - low_df

        std_will_lower = will_lower / will_lower.rolling(self.lb_window).mean()
        std_will_lower = std_will_lower.stack()

        self.factor_series = -std_will_lower
