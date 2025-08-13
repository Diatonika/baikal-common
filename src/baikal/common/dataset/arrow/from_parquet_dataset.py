from collections.abc import Sequence
from pathlib import Path

from pyarrow import RecordBatchFileWriter, Schema as ArrowSchema, ipc
from pyarrow.dataset import ParquetFileFragment, Partitioning, parquet_dataset
from pyarrow.fs import FileSystem, LocalFileSystem
from pyarrow.lib import concat_batches
from pyarrow.parquet import RowGroupMetaData, SortingColumn

from baikal.common.dataset.arrow.exceptions import InvalidMetadataException
from baikal.common.dataset.arrow.record_batch_metadata import RecordBatchMetaData


def from_parquet_dataset(
    source: Path,
    destination: Path,
    *,
    schema: ArrowSchema | None = None,
    partition: Partitioning | None = None,
    source_file_system: FileSystem | None = None,
    destination_file_system: FileSystem | None = None,
    destination_clear: bool = True,
) -> Sequence[Path]:
    dataset = parquet_dataset(
        source / "_metadata",
        schema=schema,
        filesystem=source_file_system,
        partitioning=partition,
    )

    destination_file_system = destination_file_system or LocalFileSystem()

    destination_file_system.create_dir(destination.as_posix())
    if destination_clear:
        destination_file_system.delete_dir_contents(destination.as_posix())

    files: list[Path] = []
    for fragment in dataset.get_fragments():
        assert isinstance(fragment, ParquetFileFragment)

        fragment_relative_path = (
            Path(fragment.path).relative_to(source).with_suffix(".arrow")
        )

        fragment_destination_path = destination / fragment_relative_path
        destination_file_system.create_dir(fragment_destination_path.parent.as_posix())

        with (
            destination_file_system.open_output_stream(
                fragment_destination_path.as_posix()
            ) as sink,
            ipc.new_file(sink, fragment.physical_schema) as writer,
        ):
            _write_fragment_to(fragment, writer)
            files.append(fragment_destination_path)

    return files


# region Private


def _write_fragment_to(
    fragment: ParquetFileFragment, writer: RecordBatchFileWriter
) -> None:
    fragment.ensure_complete_metadata()

    groups = fragment.split_by_row_group()
    statistics = (group.statistics for group in fragment.row_groups)
    metadata = (group.metadata for group in fragment.row_groups)

    for group, statistic, meta in zip(groups, statistics, metadata, strict=True):
        assert isinstance(meta, RowGroupMetaData)

        keys, nulls = SortingColumn.to_ordering(
            fragment.physical_schema, tuple(meta.sorting_columns)
        )

        if len(keys) != 1:
            message = f"{fragment.path} row group has {len(keys)} sort columns."
            raise InvalidMetadataException(message)

        sort_column, sort_order = keys[0]

        if statistic is None:
            message = f"{fragment.path} row group has no statistics."
            raise InvalidMetadataException(message)

        if (index_statistic := statistic.get(sort_column)) is None:
            message = f"{fragment.path} row group has no {sort_column} statistics."
            raise InvalidMetadataException(message)

        arrow_batch = concat_batches(group.scanner().to_batches())
        arrow_batch_metadata = RecordBatchMetaData(
            min=index_statistic["min"],
            max=index_statistic["max"],
            sort_column=sort_column,
            sort_order=sort_order,
        )

        writer.write_batch(arrow_batch, arrow_batch_metadata.model_dump(mode="json"))


# endregion
