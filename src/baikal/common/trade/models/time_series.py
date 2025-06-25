from typing import Annotated

from pandera.polars import DataFrameModel
from pandera.typing.polars import Series
from polars import Datetime


class TimeSeries(DataFrameModel):
    date_time: Series[Annotated[Datetime, "us", "UTC"]]

    class Config:
        coerce = True
