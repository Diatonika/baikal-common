from collections.abc import Iterable
from datetime import datetime
from typing import Literal, override

from pyarrow import (
    RecordBatch,
    Schema as ArrowSchema,
    Table as ArrowTable,
    TimestampScalar,
    concat_tables,
    scalar,
    schema as create_schema,
)
from pyarrow.compute import (
    Expression,
    equal,
    field,
    greater,
    greater_equal,
    less,
    less_equal,
)

from baikal.common.dataset import TimeSeriesReader
from baikal.common.dataset.parquet.bounded_table import BoundedTable
from baikal.common.dataset.parquet.parquet_dataset_exception import (
    ParquetDatasetException,
)


class MMapTimeSeriesReader(TimeSeriesReader):
    def __init__(self, datetime_column: str, tables: Iterable[BoundedTable]) -> None:
        self._datetime_column = datetime_column

        self._tables = list(tables)
        self._schema = MMapTimeSeriesReader._validate_schema(tables)

    # region Implementation

    @override
    def min(self) -> datetime:
        return min(table.minimum for table in self._tables)

    @override
    def max(self) -> datetime:
        return max(table.maximum for table in self._tables)

    @override
    def get(self, at: datetime) -> ArrowTable:
        concatenated: list[ArrowTable] = []
        expr = equal(field(self._datetime_column), scalar(at))

        for group in self._tables:
            if group.minimum > at or group.maximum < at:
                continue

            filtered = group.table.filter(expr)

            if isinstance(filtered, ArrowTable):
                concatenated.append(filtered)
            elif isinstance(filtered, RecordBatch):
                concatenated.append(ArrowTable.from_batches([filtered], self._schema))
            else:
                message = f"Unexpected filter return type: {type(filtered)}."
                raise ParquetDatasetException(message)

        if len(concatenated):
            return concat_tables(concatenated)

        return self._schema.empty_table()

    @override
    def slice(
        self,
        start: datetime,
        end: datetime,
        how: Literal["left", "right", "both", "none"] = "left",
    ) -> ArrowTable:
        if start > end:
            message = f"Invalid boundaries: {start} > {end}."
            raise ValueError(message)

        concatenated: list[ArrowTable] = []
        datetime_column = field(self._datetime_column)

        lower_bound = MMapTimeSeriesReader._lower_bound_filter(
            datetime_column, scalar(start), how
        )

        upper_bound = MMapTimeSeriesReader._upper_bound_filter(
            datetime_column, scalar(end), how
        )

        for group in self._tables:
            if group.minimum > end or group.maximum < start:
                continue

            filtered = group.table.filter(lower_bound & upper_bound)

            if isinstance(filtered, ArrowTable):
                concatenated.append(filtered)
            elif isinstance(filtered, RecordBatch):
                concatenated.append(ArrowTable.from_batches([filtered], self._schema))
            else:
                message = f"Unexpected filter return type: {type(filtered)}."
                raise ParquetDatasetException(message)

        if len(concatenated):
            return concat_tables(concatenated)

        return self._schema.empty_table()

    # endregion
    # region Private

    @staticmethod
    def _validate_schema(groups: Iterable[BoundedTable]) -> ArrowSchema:
        schema: ArrowSchema | None = None
        for group in groups:
            schema = schema or group.table.schema
            if not schema.equals(group.table.schema):
                message = f"Ambiguous schema choice: {schema} and {group.table.schema}."
                raise ParquetDatasetException(message)

        return schema or create_schema({})

    @staticmethod
    def _lower_bound_filter(
        datetime_column: Expression,
        lower_bound: TimestampScalar,
        how: Literal["left", "right", "both", "none"],
    ) -> Expression:
        match how:
            case "left" | "both":
                return greater_equal(datetime_column, lower_bound)
            case "right" | "none":
                return greater(datetime_column, lower_bound)

        message = f"Unexpected how: {how}"
        raise ValueError(message)

    @staticmethod
    def _upper_bound_filter(
        datetime_column: Expression,
        upper_bound: TimestampScalar,
        how: Literal["left", "right", "both", "none"],
    ) -> Expression:
        match how:
            case "left" | "none":
                return less(datetime_column, upper_bound)
            case "right" | "both":
                return less_equal(datetime_column, upper_bound)

        message = f"Unexpected how: {how}"
        raise ValueError(message)

    # endregion
