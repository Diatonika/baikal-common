from datetime import UTC, datetime
from pathlib import Path

from pyarrow import OSFile, Table as ArrowTable, ipc, total_allocated_bytes
from pyarrow.dataset import dataset as read_dataset

from baikal.common.dataset.arrow import (
    TimeSeriesMetaData,
    TimeSeriesSlicer,
    from_parquet,
)
from baikal.common.dataset.arrow.memory_map import MemoryMappedBatches, memory_map
from baikal.common.dataset.parquet import ParquetTimeSeriesPartition
from baikal.common.models import OHLC
from tests.dataset.util import assert_memory_usage, write_parquet_sample

# region Utility


def _default_memory_map(
    tmp_path: Path,
) -> MemoryMappedBatches[TimeSeriesMetaData]:
    _ = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    return memory_map(
        from_parquet(tmp_path / "parquet-data", tmp_path / "arrow-data"),
        TimeSeriesMetaData,
    )


def _default_memory_map_asserts(
    batches: MemoryMappedBatches[TimeSeriesMetaData],
) -> None:
    assert len(batches.batches)
    assert sum(len(batch.data) for batch in batches.batches) == 527_040

    assert min(batch.metadata.min for batch in batches.batches) == datetime(
        2020, 1, 1, tzinfo=UTC
    )

    assert max(batch.metadata.max for batch in batches.batches) == datetime(
        2020, 12, 31, 23, 59, tzinfo=UTC
    )

    assert not total_allocated_bytes()


def _assert_time_series_slice(
    table: ArrowTable, column: str, expected_min: datetime, expected_max: datetime
) -> None:
    python_column = table.column(column).to_pylist()
    expected_length = (expected_max - expected_min).total_seconds() // 60 + 1

    assert python_column[0] == expected_min
    assert python_column[-1] == expected_max
    assert len(python_column) == expected_length


# endregion


def test_from_parquet(tmp_path: Path) -> None:
    schema = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    files = from_parquet(
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

                validated_metadata = TimeSeriesMetaData.model_validate(
                    {key.decode("utf-8"): value for key, value in metadata.items()}
                )

                assert validated_metadata.sort_column == OHLC.date_time
                assert validated_metadata.sort_order == "ascending"


def test_memory_map_from_files(tmp_path: Path) -> None:
    _ = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    arrow_files = from_parquet(tmp_path / "parquet-data", tmp_path / "arrow-data")

    batches = memory_map(arrow_files, TimeSeriesMetaData)
    _default_memory_map_asserts(batches)


def test_memory_map_from_directory(tmp_path: Path) -> None:
    _ = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    _ = from_parquet(tmp_path / "parquet-data", tmp_path / "arrow-data")

    batches = memory_map(tmp_path / "arrow-data", TimeSeriesMetaData)
    _default_memory_map_asserts(batches)


def test_pyarrow_dataset_integration(tmp_path: Path) -> None:
    _ = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    _ = from_parquet(tmp_path / "parquet-data", tmp_path / "arrow-data")
    batches = memory_map(tmp_path / "arrow-data")
    dataset = read_dataset([meta_batch.data for meta_batch in batches.batches])

    table = dataset.to_table()
    assert len(table) == 527_040

    sliced = dataset.take([0, 5, 10, 500_000, 527_039])
    assert len(sliced) == 5

    assert total_allocated_bytes() < 1_024


def test_polars_integration(tmp_path: Path) -> None:
    _ = write_parquet_sample(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    _ = from_parquet(tmp_path / "parquet-data", tmp_path / "arrow-data")

    with assert_memory_usage(3 << 20):
        batches = memory_map(tmp_path / "arrow-data")

        frame = batches.to_polars()
        assert frame.height == 527_040


def test_slicer_slice_left(tmp_path: Path) -> None:
    batches = _default_memory_map(tmp_path)
    slicer = TimeSeriesSlicer.from_batches(batches.batches)

    sliced = slicer.slice(
        datetime(2020, 5, 14, 12, 30, tzinfo=UTC),
        datetime(2020, 7, 30, 18, 45, tzinfo=UTC),
        "left",
    )

    _assert_time_series_slice(
        sliced,
        OHLC.date_time,
        datetime(2020, 5, 14, 12, 30, tzinfo=UTC),
        datetime(2020, 7, 30, 18, 44, tzinfo=UTC),
    )


def test_slicer_slice_right(tmp_path: Path) -> None:
    batches = _default_memory_map(tmp_path)
    slicer = TimeSeriesSlicer.from_batches(batches.batches)

    sliced = slicer.slice(
        datetime(2020, 3, 1, 5, 18, tzinfo=UTC),
        datetime(2020, 9, 21, 14, 53, tzinfo=UTC),
        "right",
    )

    _assert_time_series_slice(
        sliced,
        OHLC.date_time,
        datetime(2020, 3, 1, 5, 19, tzinfo=UTC),
        datetime(2020, 9, 21, 14, 53, tzinfo=UTC),
    )


def test_slicer_slice_both(tmp_path: Path) -> None:
    batches = _default_memory_map(tmp_path)
    slicer = TimeSeriesSlicer.from_batches(batches.batches)

    sliced = slicer.slice(
        datetime(2020, 1, 13, 19, 15, tzinfo=UTC),
        datetime(2020, 12, 30, 4, 30, tzinfo=UTC),
        "both",
    )

    _assert_time_series_slice(
        sliced,
        OHLC.date_time,
        datetime(2020, 1, 13, 19, 15, tzinfo=UTC),
        datetime(2020, 12, 30, 4, 30, tzinfo=UTC),
    )


def test_slicer_slice_none(tmp_path: Path) -> None:
    batches = _default_memory_map(tmp_path)
    slicer = TimeSeriesSlicer.from_batches(batches.batches)

    sliced = slicer.slice(
        datetime(2020, 2, 1, 0, 0, tzinfo=UTC),
        datetime(2020, 2, 1, 0, 2, tzinfo=UTC),
        "none",
    )

    _assert_time_series_slice(
        sliced,
        OHLC.date_time,
        datetime(2020, 2, 1, 0, 1, tzinfo=UTC),
        datetime(2020, 2, 1, 0, 1, tzinfo=UTC),
    )
