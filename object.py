from dataclasses import dataclass
from pandas import DataFrame
from constant import Market


@dataclass
class BarDataCache:
    market: Market
    start: str
    end: str
    symbols_num: int
    dataframe: DataFrame
