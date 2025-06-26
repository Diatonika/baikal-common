from pandera import Field
from polars import Float64

from baikal.common.trade.models.ohlc import OHLC


class OHLCV(OHLC):
    volume: Float64 = Field(nullable=True)
