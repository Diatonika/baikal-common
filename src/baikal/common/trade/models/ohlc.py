from pandera import Field
from polars import Float64

from baikal.common.trade.models.time_series import TimeSeries


class OHLC(TimeSeries):
    open: Float64 = Field(nullable=True)
    high: Float64 = Field(nullable=True)
    low: Float64 = Field(nullable=True)
    close: Float64 = Field(nullable=True)
