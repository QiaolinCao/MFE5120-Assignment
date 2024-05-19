from enum import Enum


class Interval(Enum):
    MINUTE = "1m"
    HOUR = "1h"
    DAILY = "d"
    WEEKLY = "w"
    MONTHLY = "mon"


class Market(Enum):
    ASHARE = "ashare"


class QueryStatus(Enum):
    NOTQUERIED = "not_queied"
    FAILED = "failed"
    EMPTY = "empty"
    SUCCESSED = "successed"
