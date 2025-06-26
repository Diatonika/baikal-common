from pandera.polars import DataFrameModel


class TradeModel(DataFrameModel):
    @classmethod
    def column_names(cls) -> tuple[str, ...]:
        return tuple(cls.to_schema().columns)

    class Config:
        coerce = True
