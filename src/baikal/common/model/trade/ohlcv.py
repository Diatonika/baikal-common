from ibis.expr.datatypes import Float64

from baikal.common.model.trade.ohlc import OHLC


class OHLCV(OHLC):
    volume: Float64
