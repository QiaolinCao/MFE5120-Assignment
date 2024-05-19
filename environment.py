from typing import List, Optional, Union, Type
from pathlib import Path
import pandas as pd
from pandas import DataFrame
import numpy as np
import alphalens as al
from datetime import datetime
from tqdm import tqdm

from datahandler.handler import DataHandler
from object import BarDataCache
from constant import Market
from setting import SETTINGS
from factor.factors import FactorTemplate
from utilities import summary_ic_data, plot_return


class Environment:
    def __init__(self, market: str) -> None:
        self.market: Market = Market(market)
        self.data_handler: DataHandler = DataHandler(market)

        # 待分析因子
        self.factors: List[FactorTemplate] = []

        # 数据缓存
        self.bar_data_cache: Optional[BarDataCache] = None
        self.close_df_cache: Optional[DataFrame] = None
        self.open_df_cache: Optional[DataFrame] = None
        self.high_df_cache: Optional[DataFrame] = None
        self.low_df_cache: Optional[DataFrame] = None

        self.volume_df_cache: Optional[DataFrame] = None
        self.log_return_cache: Optional[DataFrame] = None

    def load_bar_data_from_csv(
            self,
            path: Union[str, Path],
            symbol_head: str = "symbol",
            exchange_head: str = "exchange",
            interval_head: str = "interval",
            datetime_head: str = "datetime",
            open_head: str = "open_price",
            high_head: str = "high_price",
            low_head: str = "low_price",
            close_head: str = "close_price",
            volume_head: str = "volume",
            turnover_head: str = "turnover"
    ) -> None:

        field_mapping = {
            symbol_head: "symbol",
            exchange_head: "exchange",
            interval_head: "interval",
            datetime_head: "datetime",
            open_head: "open_price",
            high_head: "high_price",
            low_head: "low_price",
            close_head: "close_price",
            volume_head: "volume",
            turnover_head: "turnover"
        }

        bar_df: DataFrame = pd.read_csv(path, dtype={symbol_head: str})
        bar_df.rename(columns=field_mapping, inplace=True)

        bar_df["datetime"] = pd.to_datetime(bar_df["datetime"])

        start: str = bar_df["datetime"].min().strftime("%Y-%m-%d %H:%M:%S")
        end: str = bar_df["datetime"].max().strftime("%Y-%m-%d %H:%M:%S")
        symbols_num: int = len(bar_df["symbol"].unique())

        cache: BarDataCache = BarDataCache(self.market, start, end, symbols_num, bar_df)

        self.bar_data_cache = cache

        self.describe_bar_data_cache()

    def load_bar_data_from_database(self, symbols: Union[str, List[str]], interval: str, start: str, end: str) -> None:
        if type(symbols) is str:
            if symbols == "all":
                symbols = self.data_handler.get_all_ashare_stock_symbol()
            else:
                symbols = [symbols]

        bar_df = self.data_handler.get_bar_data_from_database(symbols, interval, start, end)
        bars_cache = BarDataCache(self.market, start, end, len(symbols), bar_df)
        self.bar_data_cache = bars_cache
        self.data_handler.logger.info("a股bar数据加载完成")

        self.describe_bar_data_cache()

    def describe_bar_data_cache(self) -> None:
        cache = self.bar_data_cache
        message = f"市场类型：{self.market.value}  开始时间：{cache.start}  结束时间：{cache.end} symbol数量：{cache.symbols_num}"
        self.data_handler.logger.info(message)

    def get_bar_data_df(self) -> DataFrame:
        if self.bar_data_cache is None:
            print("请先加载bar数据")
            return pd.DataFrame()

        return self.bar_data_cache.dataframe

    def get_matrix_from_bar_data_cache(self, col_name: str) -> DataFrame:
        """
        输出DataFrame行索引为datetime， 列索引为symbol
        """
        if self.bar_data_cache is None:
            print("请先加载bar数据")
            return pd.DataFrame()

        bar_df = self.bar_data_cache.dataframe
        matrix_df = bar_df.pivot(index="datetime", columns="symbol", values=col_name)

        matrix_df.index = pd.to_datetime(matrix_df.index)
        matrix_df.index = matrix_df.index.tz_convert(SETTINGS["timezone"])

        return matrix_df

    def get_close(self) -> DataFrame:
        if self.close_df_cache is None:
            close_df = self.get_matrix_from_bar_data_cache("close_price")
            self.close_df_cache = close_df
        else:
            close_df = self.close_df_cache
        return close_df

    def get_open(self) -> DataFrame:
        if self.open_df_cache is None:
            open_df = self.get_matrix_from_bar_data_cache("open_price")
            self.open_df_cache = open_df
        else:
            open_df = self.open_df_cache
        return open_df

    def get_high(self) -> DataFrame:
        if self.high_df_cache is None:
            high_df = self.get_matrix_from_bar_data_cache("high_price")
            self.high_df_cache = high_df
        else:
            high_df = self.high_df_cache
        return high_df

    def get_low(self) -> DataFrame:
        if self.low_df_cache is None:
            low_df = self.get_matrix_from_bar_data_cache("low_price")
            self.low_df_cache = low_df
        else:
            low_df = self.low_df_cache
        return low_df

    def get_volume(self) -> DataFrame:
        if self.volume_df_cache is None:
            volume_df = self.get_matrix_from_bar_data_cache("volume")
            self.volume_df_cache = volume_df
        else:
            volume_df = self.volume_df_cache
        return volume_df

    def get_log_return(self) -> DataFrame:
        if self.log_return_cache is None:
            close_df = self.get_close()
            log_return = np.log(close_df).diff()
            self.log_return_cache = log_return
        else:
            log_return = self.log_return_cache
        return log_return

    def load_factor(self, factor) -> None:
        self.factors.append(factor)

    def factor_analysis(self, report_name: str = "", periods=(1, 5, 10)):
        # 导出结果设置
        if report_name == "":
            now = datetime.now().strftime("%Y%m%d_%H%M_%S")
            report_name = f"analysis_at_{now}"

        folder_dir = (Path(SETTINGS["project.abs_path"]).joinpath(SETTINGS["factor.report_direction"])
                      .joinpath(report_name))
        folder_dir.mkdir(parents=True, exist_ok=True)

        # 下期收益率；防止使用未来信息，采用下期收益率
        next_open = self.get_open().shift(-1)
        next_open = next_open.iloc[:-1]

        iters = tqdm(self.factors)
        for factor in iters:
            iters.set_description(f"正在分析{factor.name}")
            factor_ser = factor.get_factor_series()
            factor_data = al.utils.get_clean_factor_and_forward_returns(factor_ser, next_open, periods=periods)

            # 去除na、inf
            na_mask = factor_data.isna()
            factor_data = factor_data[~na_mask.any(axis=1)]

            inf_mask = np.isinf(factor_data)
            factor_data = factor_data[~inf_mask.any(axis=1)]

            # ic分析
            ic_data = al.performance.factor_information_coefficient(factor_data)
            sum_df = summary_ic_data(ic_data)
            idx = [f"{i}d" for i in periods]
            sum_df.index = idx
            sum_df = sum_df.T
            ic_output_name = f"ic_{factor.name}.csv"
            sum_df.to_csv(folder_dir.joinpath(ic_output_name))

            # 回报率分析
            pic_name = f"retrun_{factor.name}"
            save_path = folder_dir.joinpath(pic_name)
            plot_return(factor_data, save_pic=True, save_path=save_path)

        # 因子间相关性分析
        pass
