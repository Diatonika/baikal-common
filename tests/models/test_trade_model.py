from polars import Datetime, Float64, Int64

from baikal.common.models import TradeModel


def test_column_names() -> None:
    class TestTradeModel(TradeModel):
        first_column: int
        second_column: int

    assert TestTradeModel.column_names() == ("first_column", "second_column")


def test_polar_schema() -> None:
    class TestTradeModel(TradeModel):
        first_column: int
        second_column: Float64
        third_column: Datetime

    assert TestTradeModel.polar_schema() == {
        "first_column": Int64,
        "second_column": Float64,
        "third_column": Datetime(),
    }
