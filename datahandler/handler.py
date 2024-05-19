import asyncio
from tortoise import run_async
from typing import Union, List
import random
import time
from datetime import datetime
import pandas as pd
from pandas import DataFrame
import jqdatasdk as jq
from tqdm import tqdm
from pathlib import Path
from typing import Optional

from log.logger import Logger
from constant import Market, QueryStatus, Interval
from .database.mysql_database import MysqlDatabase
from .datafeed.akshare_datafeed import AkshareDatafeed
from setting import SETTINGS


class DataHandler:
    def __init__(self, market: str) -> None:
        self.market: Market = Market(market)
        self.datafeed = AkshareDatafeed()
        self.database = MysqlDatabase(market)
        self.loop = asyncio.get_event_loop()
        self.logger = Logger("DataHandler")

        # 添加jqdatasdk环境，方便使用一些jqdata的函数
        self.jq_user_name: str = SETTINGS["jq.user_name"]
        self.jq_password: str = SETTINGS["jq.password"]
        jq.auth(self.jq_user_name, self.jq_password)

    def download_bar_data_to_database(self, symbols: Union[str, List[str]], start: str, end: str, interval: str) -> None:
        if self.market == Market.ASHARE:
            self.download_ashare_bar_data_to_database(symbols, start, end)

    def download_ashare_bar_data_to_database(self, symbols: Union[str, List[str]], start: str, end: str) -> None:
        """目前新浪的接口仅有日度数据，暂不考虑其他数据频率"""
        if not self.database.connected:
            run_async(self.database.connect())

        time_str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        task_name: str = f"download_ashare_bar_data_to_database-{time_str}"
        self.logger.add_file_handler(task_name)

        if type(symbols) is str:
            symbols = [symbols]

        task_df: DataFrame = pd.DataFrame({
            "task": symbols,
            "status": [QueryStatus.NOTQUERIED.value] * len(symbols)
        })

        task_path = (Path(SETTINGS["project.abs_path"]).joinpath(SETTINGS["log.file_direction"])
                     .joinpath(f"{task_name}.csv"))

        task_df.to_csv(task_path,index=False, mode="w")

        async def _download_ashare_bar_data_to_database():
            iters = tqdm(symbols)
            for symbol in iters:
                df, status = self.datafeed.query_ashare_daily(symbol, "20130101", "20231231")
                sleep_time = random.randint(4, 7)
                time.sleep(sleep_time)

                idx = task_df[task_df["task"] == symbol].index
                if status == QueryStatus.SUCCESSED:
                    await self.database.save_bar_data(df)
                    task_df.loc[idx, "status"] = QueryStatus.SUCCESSED
                elif status == QueryStatus.EMPTY:
                    self.logger.info(f"股票{symbol}的数据为空")
                    task_df.loc[idx, "status"] = QueryStatus.EMPTY
                elif status == QueryStatus.FAILED:
                    self.logger.warning(f"股票{symbol}查询失败")
                    task_df.loc[idx, "status"] = QueryStatus.FAILED

                task_df.to_csv(task_path,index=False, mode="w")
                iters.set_description(f"Processing {symbol}")

            self.logger.info("任务完成")

        run_async(_download_ashare_bar_data_to_database())

    def get_all_ashare_stock_symbol(self) -> List[str]:
        if self.market == Market.ASHARE:
            return self.datafeed.get_all_ashare_stock_symbol()
        else:
            self.logger.info("当前市场类型不支持此函数")
            return []

    def get_bar_data_from_database(
            self,
            symbols: Union[str, List[str]],
            interval: str,
            start: str,
            end: str
        ) -> DataFrame:
        if self.market == Market.ASHARE:
            return self.get_ashare_bar_data(symbols, interval, start, end)

    def get_ashare_bar_data(
            self,
            symbols: Union[str, List[str]],
            interval: str,
            start: str,
            end: str
        ) -> DataFrame:

        if not self.database.connected:
            run_async(self.database.connect())

        if type(symbols) is str:
            if symbols == "all":
                symbols = AkshareDatafeed.get_all_ashare_stock_symbol()
            else:
                symbols = [symbols]

        loop = self.loop
        future = asyncio.ensure_future(
            self.database.query_ashare_bar_data(symbols, Interval(interval), start, end),
            loop=loop
        )
        loop.run_until_complete(future)
        bar_df = future.result()
        return bar_df






