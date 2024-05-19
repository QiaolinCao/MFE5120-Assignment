from datetime import datetime
from tortoise import fields
from tortoise.models import Model
from typing import Optional


class DbBarData(Model):
    """K线数据表映射对象"""

    id: int = fields.IntField(pk=True)

    symbol: str = fields.CharField(max_length=64)
    exchange: str = fields.CharField(max_length=64)
    datetime: datetime = fields.DatetimeField()
    interval: str = fields.CharField(max_length=16)

    open_price: float = fields.FloatField()
    high_price: float = fields.FloatField()
    low_price: float = fields.FloatField()
    close_price: float = fields.FloatField()
    volume: float = fields.FloatField()
    turnover: float = fields.FloatField()

    class Meta:
        table: str = "db_bar_data"
        indexes: list = [("symbol", "exchange", "interval", "datetime"),]


class DbFactorData(Model):
    id: int = fields.IntField(pk=True)

    factor_name: str = fields.CharField(max_length=32)
    interval: str = fields.CharField(max_length=16)

    symbol: str = fields.CharField(max_length=64)
    exchange: str = fields.CharField(max_length=64)
    datetime: datetime = fields.DatetimeField()

    factor_value: float = fields.FloatField()

    class Meta:
        table: str = "db_factor_data"
        indexes: list = [("factor_name", "symbol", "exchange", "interval", "datetime"),]
