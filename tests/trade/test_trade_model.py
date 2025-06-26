from baikal.common.trade.models import TradeModel


def test_column_names() -> None:
    class TestTradeModel(TradeModel):
        first_column: int
        second_column: int

    assert TestTradeModel.column_names() == ("first_column", "second_column")
