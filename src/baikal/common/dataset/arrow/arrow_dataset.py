from collections.abc import Iterable
from datetime import datetime
from typing import Literal

from pyarrow import (
    RecordBatch,
    Schema as ArrowSchema,
    Table as ArrowTable,
    schema as create_schema,
)

from baikal.common.dataset.arrow.batch_with_metadata import BatchWithMetaData
from baikal.common.dataset.arrow.exceptions import (
    SchemaValidationException,
)


class ArrowDataset:
    schema: ArrowSchema
    batches: tuple[BatchWithMetaData, ...]

    def __init__(self, batches: Iterable[BatchWithMetaData]) -> None:
        self.schema = _validate_schema(batches)
        self.batches = _sort_batches(batches)

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

            # sort_column = batch.batch.column(batch.metadata.sort_column)

        if len(slices):
            return ArrowTable.from_batches(slices, self.schema)

        return self.schema.empty_table()


# region Private


def _validate_schema(batches: Iterable[BatchWithMetaData]) -> ArrowSchema:
    schema: ArrowSchema | None = None
    for batch in batches:
        schema = schema or batch.batch.schema
        if not schema.equals(batch.batch.schema):
            message = f"Ambiguous schema: {schema} and {batch.batch.schema}."
            raise SchemaValidationException(message)

    return schema or create_schema({})


def _sort_batches(
    batches: Iterable[BatchWithMetaData],
) -> tuple[BatchWithMetaData, ...]:
    def _sort_by(value: BatchWithMetaData) -> datetime:
        return value.metadata.min

    return tuple(sorted(batches, key=_sort_by))


# endregion
