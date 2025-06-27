from typing import Any

from pandera.polars import DataFrameModel
from polars import Schema


class TradeModel(DataFrameModel):
    @classmethod
    def column_names(cls) -> tuple[str, ...]:
        return tuple(cls.to_schema().columns)

    @classmethod
    def polar_schema(cls) -> Schema:
        types: dict[str, Any] = {
            name: dtype.type for name, dtype in cls.to_schema().dtypes.items()
        }

        return Schema(types, check_dtypes=True)

    class Config:
        coerce = True
