from collections.abc import Collection, Iterable
from datetime import datetime
from typing import Any, Literal, assert_never

from attrs import frozen
from pyarrow import (
    RecordBatch,
    Schema as ArrowSchema,
    Table as ArrowTable,
    scalar,
    schema as create_schema,
)
from pyarrow.compute import Expression, field

from baikal.common.dataset.arrow.batch_with_metadata import BatchWithMetaData
from baikal.common.dataset.arrow.exceptions import (
    SchemaValidationException,
)
from baikal.common.dataset.arrow.time_series_metadata import TimeSeriesMetaData


@frozen
class TimeSeriesSlicer:
    schema: ArrowSchema
    batches: tuple[BatchWithMetaData[TimeSeriesMetaData], ...]

    @staticmethod
    def from_batches(
        batches: Collection[BatchWithMetaData[TimeSeriesMetaData]],
    ) -> "TimeSeriesSlicer":
        return TimeSeriesSlicer(
            schema=_validate_schema(batches), batches=tuple(batches)
        )

    def slice(
        self,
        start: datetime,
        end: datetime,
        how: Literal["left", "right", "both", "none"] = "left",
    ) -> ArrowTable:
        if start > end:
            message = f"Invalid boundaries: {start} > {end}."
            raise ValueError(message)

        slices: list[RecordBatch] = []
        for batch in self.batches:
            if batch.metadata.min > end or batch.metadata.max < start:
                continue

            if batch.metadata.min >= start and batch.metadata.max <= end:
                slices.append(batch.data)
                continue

            lower_bound: Expression
            match how:
                case "left" | "both":
                    lower_bound = field(batch.metadata.sort_column) >= scalar(start)
                case "right" | "none":
                    lower_bound = field(batch.metadata.sort_column) > scalar(start)
                case _:
                    assert_never(how)

            upper_bound: Expression
            match how:
                case "right" | "both":
                    upper_bound = field(batch.metadata.sort_column) <= scalar(end)
                case "left" | "none":
                    upper_bound = field(batch.metadata.sort_column) < scalar(end)
                case _:
                    assert_never(how)

            filtered = batch.data.filter(lower_bound & upper_bound)
            assert isinstance(filtered, RecordBatch)

            slices.append(filtered)

        if len(slices):
            return ArrowTable.from_batches(slices, self.schema)

        return self.schema.empty_table()


# region Private


def _validate_schema(batches: Iterable[BatchWithMetaData[Any]]) -> ArrowSchema:
    schema: ArrowSchema | None = None
    for batch in batches:
        schema = schema or batch.data.schema
        if not schema.equals(batch.data.schema):
            message = f"Ambiguous schema: {schema} and {batch.data.schema}."
            raise SchemaValidationException(message)

    return schema or create_schema({})


# endregion
