from typing import Annotated

from pandera.typing.polars import Series
from polars import Datetime

from baikal.common.models.trade_model import TradeModel


class TimeSeries(TradeModel):
    date_time: Series[Annotated[Datetime, "us", "UTC"]]
