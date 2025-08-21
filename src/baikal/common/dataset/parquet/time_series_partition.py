import enum

import polars as polar
import pyarrow as arrow


class ParquetTimeSeriesPartition(enum.IntEnum):
    YEAR = 1
    MONTH = 2
    DAY = 3
    HOUR = 4

    def parquet_schema(self) -> arrow.Schema:
        fields: dict[str, arrow.DataType] = {}

        if self >= ParquetTimeSeriesPartition.YEAR:
            fields["__year"] = arrow.int32()

        if self >= ParquetTimeSeriesPartition.MONTH:
            fields["__month"] = arrow.int8()

        if self >= ParquetTimeSeriesPartition.DAY:
            fields["__day"] = arrow.int8()

        if self >= ParquetTimeSeriesPartition.HOUR:
            fields["__hour"] = arrow.int8()

        if self > ParquetTimeSeriesPartition.HOUR:
            error = f"Unsupported partition type: {self}"
            raise ValueError(error)

        return arrow.schema(fields)

    def polar_expressions(self, date_time_column: str) -> dict[str, polar.Expr]:
        columns: dict[str, polar.Expr] = {}

        if self >= ParquetTimeSeriesPartition.YEAR:
            columns["__year"] = polar.col(date_time_column).dt.year()

        if self >= ParquetTimeSeriesPartition.MONTH:
            columns["__month"] = polar.col(date_time_column).dt.month()

        if self >= ParquetTimeSeriesPartition.DAY:
            columns["__day"] = polar.col(date_time_column).dt.day()

        if self >= ParquetTimeSeriesPartition.HOUR:
            columns["__hour"] = polar.col(date_time_column).dt.hour()

        if self > ParquetTimeSeriesPartition.HOUR:
            error = f"Unsupported partition type: {self}"
            raise ValueError(error)

        return columns
