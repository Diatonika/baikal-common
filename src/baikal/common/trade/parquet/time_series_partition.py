import enum

from polars import Expr, col


class ParquetTimeSeriesPartition(enum.IntEnum):
    YEAR = 1
    MONTH = 2
    DAY = 3
    HOUR = 4

    def partition_by(self) -> tuple[str, ...]:
        match self:
            case ParquetTimeSeriesPartition.YEAR:
                return ("__year",)
            case ParquetTimeSeriesPartition.MONTH:
                return "__year", "__month"
            case ParquetTimeSeriesPartition.DAY:
                return "__year", "__month", "__day"
            case ParquetTimeSeriesPartition.HOUR:
                return "__year", "__month", "__day", "__hour"

        error = f"Unsupported partition type: {self}"
        raise ValueError(error)

    def partition_columns(self, date_time_column: str) -> dict[str, Expr]:
        columns: dict[str, Expr] = {}

        if self >= ParquetTimeSeriesPartition.YEAR:
            columns["__year"] = col(date_time_column).dt.year()

        if self >= ParquetTimeSeriesPartition.MONTH:
            columns["__month"] = col(date_time_column).dt.month()

        if self >= ParquetTimeSeriesPartition.DAY:
            columns["__day"] = col(date_time_column).dt.day()

        if self >= ParquetTimeSeriesPartition.HOUR:
            columns["__hour"] = col(date_time_column).dt.hour()

        if self > ParquetTimeSeriesPartition.HOUR:
            error = f"Unsupported partition type: {self}"
            raise ValueError(error)

        return columns
