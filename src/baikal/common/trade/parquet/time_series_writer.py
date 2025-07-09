from pathlib import Path
from types import TracebackType
from typing import Any, Self

from pandera.typing.polars import DataFrame
from polars import concat
from pyarrow import Table as ArrowTable
from pyarrow.dataset import write_dataset

from baikal.common.trade.models import TimeSeries
from baikal.common.trade.parquet.time_series_partition import ParquetTimeSeriesPartition


class ParquetTimeSeriesWriter[M: TimeSeries]:
    def __init__(self, root: Path, partition: ParquetTimeSeriesPartition) -> None:
        self._root = root
        self._partition_by = partition.partition_by()
        self._partition_columns = partition.partition_columns(TimeSeries.date_time)

        self._group: list[DataFrame[Any]] = []
        self._group_key: tuple[object, ...] = ()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._write_parquet(self._group)

        self._group = []
        self._group_key = ()

    def write(self, chunk: DataFrame[M]) -> None:
        groups = chunk.with_columns(**self._partition_columns).partition_by(
            *self._partition_by,
            maintain_order=True,
            include_key=True,
            as_dict=True,
        )

        if not len(groups):
            return

        for group_key, group_chunk in groups.items():
            if group_key != self._group_key:
                self._write_parquet(self._group)

                self._group = []
                self._group_key = group_key

            self._group.append(DataFrame(group_chunk))

    def _write_parquet(self, frames: list[DataFrame[Any]]) -> None:
        if not len(frames):
            return

        arrow: ArrowTable = concat(frames).to_arrow()
        write_dataset(
            arrow,
            self._root,
            format="parquet",
            partitioning=list(self._partition_by),
            partitioning_flavor="hive",
            schema=arrow.schema,
            existing_data_behavior="delete_matching",
        )
