import enum

import polars as polar
import pyarrow as arrow


class ParquetTSPartition(enum.IntEnum):
    YEAR = 1
    MONTH = 2
    DAY = 3
    HOUR = 4

    def parquet_schema(self) -> arrow.Schema:
        fields: dict[str, arrow.DataType] = {}

        if self >= ParquetTSPartition.YEAR:
            fields["__year"] = arrow.int32()

        if self >= ParquetTSPartition.MONTH:
            fields["__month"] = arrow.int8()

        if self >= ParquetTSPartition.DAY:
            fields["__day"] = arrow.int8()

        if self >= ParquetTSPartition.HOUR:
            fields["__hour"] = arrow.int8()

        if self > ParquetTSPartition.HOUR:
            error = f"Unsupported partition type: {self}"
            raise ValueError(error)

        return arrow.schema(fields)

    def polar_expressions(self, date_time_column: str) -> dict[str, polar.Expr]:
        columns: dict[str, polar.Expr] = {}

        if self >= ParquetTSPartition.YEAR:
            columns["__year"] = polar.col(date_time_column).dt.year()

        if self >= ParquetTSPartition.MONTH:
            columns["__month"] = polar.col(date_time_column).dt.month()

        if self >= ParquetTSPartition.DAY:
            columns["__day"] = polar.col(date_time_column).dt.day()

        if self >= ParquetTSPartition.HOUR:
            columns["__hour"] = polar.col(date_time_column).dt.hour()

        if self > ParquetTSPartition.HOUR:
            error = f"Unsupported partition type: {self}"
            raise ValueError(error)

        return columns
