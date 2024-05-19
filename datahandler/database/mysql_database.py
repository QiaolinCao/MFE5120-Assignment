from typing import Dict, List, Union
import pandas as pd
from pandas import DataFrame
from tortoise import Tortoise
from datetime import datetime
from datahandler.datafeed.akshare_datafeed import AkshareDatafeed

from datahandler.database.ashare_schemas import DbBarData
from constant import Market, Interval
from setting import SETTINGS


user = SETTINGS["database.user"]
password = SETTINGS["database.password"]
host = SETTINGS["database.host"]
port = SETTINGS["database.port"]

Market_database_map: Dict[Market, str] = {
    Market.ASHARE: "mfe5210_ashare"
}

Market_models_map: Dict[Market, str] = {
    Market.ASHARE: "datahandler.database.ashare_schemas"
}


class MysqlDatabase:
    def __init__(self, market: str) -> None:
        market: Market = Market(market)
        database = Market_database_map[market]
        schemas = Market_models_map[market]

        self.market = market
        self.db_url: str = f"mysql://{user}:{password}@{host}:{port}/{database}"
        self.models: str = schemas
        self.tortoise = Tortoise()
        self.connected: bool = False

    async def connect(self):
        await self.tortoise.init(
            db_url=self.db_url,
            modules={"models": [self.models]}
        )

        await Tortoise.generate_schemas(safe=True)
        self.connected = True

    async def save_bar_data(self, bars_df: DataFrame) -> bool:
        """
        :param bars_df: DataFrame, 每行为bar_data的一条记录；字段需与schema一致
        :return:
        """
        if not self.connected:
            await self.connect()

        records = bars_df.to_dict(orient='records')

        for record in records:
            await DbBarData.get_or_create(
                symbol=record["symbol"],
                exchange=record["exchange"],
                interval=record["interval"],
                datetime=record["datetime"],
                defaults={
                    "open_price": record["open_price"],
                    "high_price": record["high_price"],
                    "low_price": record["low_price"],
                    "close_price": record["close_price"],
                    "volume": record["volume"],
                    "turnover": record["turnover"]

                }
            )
        return True

    async def query_ashare_bar_data(
            self,
            symbols: List[str],
            interval: Interval,
            start: Union[str, datetime],
            end: Union[str, datetime]
            ) -> DataFrame:
        """
        时间如果是字符串，需为形如“20240101”的格式
        """
        if self.market != Market.ASHARE:
            print("当前市场类型不支持此函数")

        def _to_datetime(time: Union[str, datetime]):
            if type(time) is str:
                return datetime.strptime(time, "%Y%m%d")
            return time

        start = _to_datetime(start)
        end = _to_datetime(end)

        bar_datas = await (
                DbBarData.filter(
                    symbol__in=symbols,
                    interval=interval.value,
                    datetime__gte=start,
                    datetime__lte=end,
                ).order_by("datetime")
            )

        data = [
            {
                "symbol": bar_data.symbol,
                "exchange": bar_data.exchange,
                "interval": bar_data.interval,
                "datetime": bar_data.datetime,

                "open_price": bar_data.open_price,
                "high_price": bar_data.high_price,
                "low_price": bar_data.low_price,
                "close_price": bar_data.close_price,

                "volume": bar_data.volume,
                "turnover": bar_data.turnover
            }
            for bar_data in bar_datas
        ]

        bar_df = pd.DataFrame(data)
        return bar_df

