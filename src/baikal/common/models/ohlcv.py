from polars import Float64

from baikal.common.models.ohlc import OHLC


class OHLCV(OHLC):
    volume: Float64
