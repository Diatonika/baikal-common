from datetime import UTC, datetime, timedelta
from pathlib import Path
from random import shuffle

from pyarrow import OSFile, Table as ArrowTable, ipc, total_allocated_bytes
from pyarrow.dataset import dataset as read_dataset

from baikal.common.dataset.arrow import (
    ArrowDataset,
    RecordBatchMetaData,
    from_parquet_dataset,
    memory_map_dataset,
)
from baikal.common.dataset.parquet import ParquetTimeSeriesPartition
from baikal.common.models import OHLC
from tests.dataset.util import write_parquet_sample

# region Utility


def _default_memory_map_dataset(tmp_path: Path) -> ArrowDataset:
    _ = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    return memory_map_dataset(
        from_parquet_dataset(tmp_path / "parquet-data", tmp_path / "arrow-data")
    )


def _default_memory_map_dataset_asserts(dataset: ArrowDataset) -> None:
    assert len(dataset.batches)
    assert len(dataset) == 527_040

    assert min(batch.metadata.min for batch in dataset.batches) == datetime(
        2020, 1, 1, tzinfo=UTC
    )

    assert max(batch.metadata.max for batch in dataset.batches) == datetime(
        2020, 12, 31, 23, 59, tzinfo=UTC
    )

    assert not total_allocated_bytes()


def _assert_arrow_dataset_slice(
    table: ArrowTable, column: str, expected_min: datetime, expected_max: datetime
) -> None:
    python_column = table.column(column).to_pylist()
    expected_length = (expected_max - expected_min).total_seconds() // 60 + 1

    assert python_column[0] == expected_min
    assert python_column[-1] == expected_max
    assert len(python_column) == expected_length


# endregion


def test_from_parquet_dataset(tmp_path: Path) -> None:
    schema = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    files = from_parquet_dataset(
        tmp_path / "parquet-data", tmp_path / "arrow-data", schema=schema
    )

    assert len(files) == 12
    for file in files:
        with (
            OSFile(file.as_posix(), "rb") as source,
            ipc.open_file(source) as batches,
        ):
            assert batches.num_record_batches > 0
            for index in range(batches.num_record_batches):
                pack = batches.get_batch_with_custom_metadata(index)
                batch, metadata = pack.batch, pack.custom_metadata

                assert batch.schema == schema

                validated_metadata = RecordBatchMetaData.model_validate(metadata)
                assert validated_metadata.sort_column == OHLC.date_time
                assert validated_metadata.sort_order == "ascending"


def test_memory_map_dataset_from_files(tmp_path: Path) -> None:
    schema = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    arrow_files = from_parquet_dataset(
        tmp_path / "parquet-data", tmp_path / "arrow-data"
    )

    dataset = memory_map_dataset(arrow_files)

    assert dataset.schema == schema
    _default_memory_map_dataset_asserts(dataset)


def test_memory_map_dataset_from_directory(tmp_path: Path) -> None:
    schema = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    _ = from_parquet_dataset(tmp_path / "parquet-data", tmp_path / "arrow-data")

    dataset = memory_map_dataset(tmp_path / "arrow-data")

    assert dataset.schema == schema
    _default_memory_map_dataset_asserts(dataset)


def test_pyarrow_dataset_integration(tmp_path: Path) -> None:
    _ = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    _ = from_parquet_dataset(tmp_path / "parquet-data", tmp_path / "arrow-data")
    arrow_dataset = memory_map_dataset(tmp_path / "arrow-data")

    dataset = read_dataset(
        [meta_batch.batch for meta_batch in arrow_dataset.batches],
        schema=arrow_dataset.schema,
    )

    table = dataset.to_table()
    assert len(table) == 527_040

    sliced = dataset.take([0, 5, 10, 500_000, 527_039])
    assert len(sliced) == 5

    assert total_allocated_bytes() < 1_024


def test_memory_map_dataset_chunk_order(tmp_path: Path) -> None:
    _ = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    arrow_files = list(
        from_parquet_dataset(tmp_path / "parquet-data", tmp_path / "arrow-data")
    )

    shuffle(arrow_files)
    dataset = memory_map_dataset(arrow_files)

    batch_max: datetime = datetime(2019, 12, 31, 23, 59, tzinfo=UTC)
    for batch in dataset.batches:
        assert batch.metadata.min == batch_max + timedelta(minutes=1)
        batch_max = batch.metadata.max


def test_arrow_dataset_slice_left(tmp_path: Path) -> None:
    dataset = _default_memory_map_dataset(tmp_path)

    sliced = dataset.slice(
        datetime(2020, 5, 14, 12, 30, tzinfo=UTC),
        datetime(2020, 7, 30, 18, 45, tzinfo=UTC),
        "left",
    )

    _assert_arrow_dataset_slice(
        sliced,
        OHLC.date_time,
        datetime(2020, 5, 14, 12, 30, tzinfo=UTC),
        datetime(2020, 7, 30, 18, 44, tzinfo=UTC),
    )


def test_arrow_dataset_slice_right(tmp_path: Path) -> None:
    dataset = _default_memory_map_dataset(tmp_path)

    sliced = dataset.slice(
        datetime(2020, 3, 1, 5, 18, tzinfo=UTC),
        datetime(2020, 9, 21, 14, 53, tzinfo=UTC),
        "right",
    )

    _assert_arrow_dataset_slice(
        sliced,
        OHLC.date_time,
        datetime(2020, 3, 1, 5, 19, tzinfo=UTC),
        datetime(2020, 9, 21, 14, 53, tzinfo=UTC),
    )


def test_arrow_dataset_slice_both(tmp_path: Path) -> None:
    dataset = _default_memory_map_dataset(tmp_path)

    sliced = dataset.slice(
        datetime(2020, 1, 13, 19, 15, tzinfo=UTC),
        datetime(2020, 12, 30, 4, 30, tzinfo=UTC),
        "both",
    )

    _assert_arrow_dataset_slice(
        sliced,
        OHLC.date_time,
        datetime(2020, 1, 13, 19, 15, tzinfo=UTC),
        datetime(2020, 12, 30, 4, 30, tzinfo=UTC),
    )


def test_arrow_dataset_slice_none(tmp_path: Path) -> None:
    dataset = _default_memory_map_dataset(tmp_path)

    sliced = dataset.slice(
        datetime(2020, 2, 1, 0, 0, tzinfo=UTC),
        datetime(2020, 2, 1, 0, 2, tzinfo=UTC),
        "none",
    )

    _assert_arrow_dataset_slice(
        sliced,
        OHLC.date_time,
        datetime(2020, 2, 1, 0, 1, tzinfo=UTC),
        datetime(2020, 2, 1, 0, 1, tzinfo=UTC),
    )
