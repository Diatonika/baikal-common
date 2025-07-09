from polars import Float64

from baikal.common.trade.models.time_series import TimeSeries


class OHLC(TimeSeries):
    open: Float64
    high: Float64
    low: Float64
    close: Float64
