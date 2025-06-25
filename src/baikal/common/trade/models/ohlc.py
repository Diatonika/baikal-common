from polars import Float64

from baikal.common.trade.models.time_series import TimeSeries


class OHLC(TimeSeries):
    open_price: Float64
    high_price: Float64
    low_price: Float64
    close_price: Float64
