from pathlib import Path
from tempfile import NamedTemporaryFile
from types import TracebackType
from typing import override

from attrs import define
from pyarrow import OSFile, Schema as ArrowSchema, Table as ArrowTable, ipc, memory_map
from pyarrow.dataset import Fragment, ParquetFileFragment, Partitioning, parquet_dataset
from pyarrow.fs import FileSystem

from baikal.common.dataset.parquet.bounded_table import BoundedTable
from baikal.common.dataset.parquet.mmap_time_series_reader import MMapTimeSeriesReader
from baikal.common.dataset.parquet.parquet_dataset_exception import (
    ParquetDatasetException,
)
from baikal.common.dataset.time_series_reader_factory import TimeSeriesReaderFactory


@define
class _MemoryMappedRowGroup:
    file: Path
    table: BoundedTable


class MMapTimeSeriesReaderFactory(TimeSeriesReaderFactory):
    def __init__(
        self,
        root: Path,
        datetime_column: str,
        *,
        schema: ArrowSchema | None = None,
        partition: Partitioning | None = None,
        file_system: FileSystem | None = None,
        mmap_dir: Path | None = None,
    ) -> None:
        self._datetime_column = datetime_column
        self._mmap_dir = mmap_dir
        self._groups: list[_MemoryMappedRowGroup] = []

        self._dataset = parquet_dataset(
            root / "_metadata",
            schema=schema,
            filesystem=file_system,
            partitioning=partition,
        )

    @override
    def __enter__(self) -> MMapTimeSeriesReader:
        if len(self._groups):
            message = f"{self.__class__.__name__} is entered twice."
            raise RuntimeError(message)

        self._groups = self._mmap_groups()
        return MMapTimeSeriesReader(
            self._datetime_column,
            (group.table for group in self._groups),
        )

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        for group in self._groups:
            group.file.unlink()

        self._groups.clear()

    # region Private

    def _mmap_groups(self) -> list[_MemoryMappedRowGroup]:
        infos: list[_MemoryMappedRowGroup] = []

        for fragment in self._dataset.get_fragments():
            assert isinstance(fragment, ParquetFileFragment)

            fragment.ensure_complete_metadata()

            groups = fragment.split_by_row_group()
            statistics = (group.statistics for group in fragment.row_groups)

            for group, statistic in zip(groups, statistics, strict=True):
                if statistic is None:
                    message = f"{fragment.path} row group has no statistics."
                    raise ParquetDatasetException(message)

                if (datetime_statistic := statistic.get(self._datetime_column)) is None:
                    message = (
                        f"{fragment.path} row group "
                        f"has no {self._datetime_column} statistics."
                    )

                    raise ParquetDatasetException(message)

                with NamedTemporaryFile(
                    "w+b", suffix=".arrow", dir=self._mmap_dir, delete=False
                ) as named_temporary_file:
                    file_path = Path(named_temporary_file.name)

                infos.append(
                    _MemoryMappedRowGroup(
                        file=file_path,
                        table=BoundedTable(
                            minimum=datetime_statistic["min"],
                            maximum=datetime_statistic["max"],
                            table=self._mmap_group(group, file_path),
                        ),
                    )
                )

        return infos

    def _mmap_group(self, group: Fragment, path: Path) -> ArrowTable:
        with (
            OSFile(path.as_posix(), "wb") as sink,
            ipc.new_file(sink, self._dataset.schema) as writer,
        ):
            for batch in group.scanner(self._dataset.schema).scan_batches():
                writer.write_batch(batch.record_batch)

        with memory_map(path.as_posix(), "rb") as reader:
            return ipc.open_file(reader).read_all()

    # endregion
