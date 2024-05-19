import akshare as ak
import pandas as pd
from pandas import DataFrame
from typing import List, Tuple
from utilities import match_stock_exchange
from constant import Interval, QueryStatus

col_schemas_map: dict = {
    "date": "datetime",
    "open": "open_price",
    "high": "high_price",
    "low": "low_price",
    "close": "close_price",
    "amount": "turnover"
}


class AkshareDatafeed:
    @staticmethod
    def query_ashare_daily(symbol: str, start: str, end: str, adjust: str = "hfq") -> Tuple[DataFrame, QueryStatus]:
        """
        采用akshare的stock_zh_a_daily接口
        参考：https://zhuanlan.zhihu.com/p/671295006, 该接口数据的复权方式并非加减复权，适合回测
        多次获取容易封禁 IP，因此设计中采用sleep，不采用多线程，运行时间长
        :param symbol: 股票代码, 如“sh600000”
        :param start: 开始日期， “YYYYmmdd”
        :param end: 结束日期， “YYYYmmdd”
        :param adjust: 复权方式， {"qfq", "hfq", ""}
        :return:
        """
        exchange: str = match_stock_exchange(symbol)
        query_status: QueryStatus = QueryStatus.SUCCESSED

        try:
            data_df: DataFrame = ak.stock_zh_a_daily(exchange + symbol, start, end, adjust)
        except:
            query_status = QueryStatus.FAILED
            data_df = pd.DataFrame()

        if len(data_df) > 0:
            data_df.drop("turnover", axis=1, inplace=True)  # 删除换手率，将该字段留给成交额
            data_df.rename(columns=col_schemas_map, inplace=True)
            data_df["symbol"] = symbol
            data_df["exchange"] = exchange
            data_df["interval"] = Interval.DAILY.value
            data_df["datetime"] = data_df["datetime"].astype(str)
        else:
            query_status = QueryStatus.EMPTY

        return data_df, query_status


    @staticmethod
    def get_all_ashare_stock_symbol() -> List[str]:
        df = ak.stock_zh_a_spot_em()
        return list(df["代码"])


