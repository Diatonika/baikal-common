from pathlib import Path
from types import TracebackType
from typing import Self

from pandera.typing.polars import DataFrame
from polars import DataFrame as PolarDataFrame, concat
from pyarrow import Schema, Table as ArrowTable, schema as create_schema
from pyarrow.fs import FileSystem, LocalFileSystem
from pyarrow.parquet import (
    FileMetaData,
    SortingColumn,
    write_metadata,
    write_to_dataset,
)

from baikal.common.trade.models import TimeSeries
from baikal.common.trade.parquet.time_series_partition import ParquetTimeSeriesPartition


class ParquetTimeSeriesWriter:
    def __init__(
        self,
        root: Path,
        partition: ParquetTimeSeriesPartition,
        *,
        file_system: FileSystem | None = None,
    ) -> None:
        self._root = root

        self._partition_columns = partition.parquet_schema().names
        self._polar_expressions = partition.polar_expressions(TimeSeries.date_time)

        self._file_system = file_system or LocalFileSystem()

        self._group: list[PolarDataFrame] = []
        self._group_key: tuple[object, ...] = ()
        self._metadata: list[FileMetaData] = []
        self._schema: Schema | None = None

    def write[M: TimeSeries](self, chunk: DataFrame[M]) -> None:
        """Writes ordered `TimeSeries` polars chunks to parquet hive dataset.

        Parameters
        ----------
        chunk: DataFrame[M]
            Polars ordered TimeSeries chunk.

        Warnings
        --------
        `ParquetTimeSeriesWriter` expects ordered time series chunks:
        both inside every chunk and between sequential chunks.

        Ordering is not checked.
        In case ordering is violated, resulting parquet correctness is not guaranteed.
        """
        groups = chunk.with_columns(**self._polar_expressions).partition_by(
            *self._partition_columns,
            as_dict=True,
            include_key=True,
            maintain_order=True,
        )

        if not len(groups):
            return

        for group_key, group_chunk in groups.items():
            if group_key != self._group_key:
                self._write_parquet(self._group)

                self._group = []
                self._group_key = group_key

            self._group.append(group_chunk)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._write_parquet(self._group)

        write_metadata(
            self._drop_partitions(self._schema or create_schema([])),
            filesystem=self._file_system,
            where=(self._root / "_common_metadata").as_posix(),
        )

        write_metadata(
            self._drop_partitions(self._schema or create_schema([])),
            filesystem=self._file_system,
            metadata_collector=self._metadata,
            where=(self._root / "_metadata").as_posix(),
        )

        self._group = []
        self._group_key = ()
        self._metadata = []
        self._schema = None

    def _write_parquet(self, frames: list[PolarDataFrame]) -> None:
        if not len(frames):
            return

        arrow: ArrowTable = concat(frames, rechunk=True).to_arrow()
        sorting_order = SortingColumn.from_ordering(
            arrow.schema,
            [(TimeSeries.date_time, "ascending")],
        )

        self._schema = self._schema or arrow.schema
        self._validate_schema(self._schema, arrow.schema)

        write_to_dataset(
            arrow,
            self._root,
            basename_template="part-{i}.parquet",
            existing_data_behavior="delete_matching",
            filesystem=self._file_system,
            metadata_collector=self._metadata,
            partitioning=list(self._partition_columns),
            partitioning_flavor="hive",
            schema=self._schema,
            sorting_columns=sorting_order,
            write_page_index=True,
        )

    def _drop_partitions(self, schema: Schema) -> Schema:
        for partition_column in self._partition_columns:
            if partition_column not in schema.names:
                continue

            indices = schema.get_all_field_indices(partition_column)
            for index in indices:
                schema = schema.remove(index)

        return schema

    @staticmethod
    def _validate_schema(expected: Schema, actual: Schema) -> None:
        if expected.equals(actual):
            return

        message = (
            f"Unequal schemas on parquet dataset write. "
            f"Expected: {expected}. "
            f"Actual: {actual}."
        )

        raise ValueError(message)
