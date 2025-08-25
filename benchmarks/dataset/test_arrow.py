from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, assert_never

import pytest

from polars import (
    DataFrame as PolarDataFrame,
    LazyFrame as PolarLazyFrame,
    all as polar_all,
    col,
    scan_parquet,
)
from pyarrow import Table as ArrowTable

from baikal.common.dataset.arrow import (
    ArrowDataset,
    from_parquet_dataset,
    memory_map_dataset,
)
from baikal.common.dataset.parquet import ParquetTimeSeriesPartition
from baikal.common.models import OHLC
from tests.dataset.util import write_parquet_sample

# region Utility


def _arrow_dataset_date_slice(
    dataset: ArrowDataset,
    start: datetime,
    end: datetime,
) -> ArrowTable:
    return dataset.slice(start, end)


def _polar_dataset_date_slice(
    dataset: PolarLazyFrame,
    engine: Literal["in-memory", "streaming"],
    start: datetime,
    end: datetime,
    on: str,
) -> PolarDataFrame:
    return dataset.filter(col(on) >= start, col(on) < end).collect(engine=engine)


def _arrow_dataset_index(dataset: ArrowTable, indices: Sequence[int]) -> ArrowTable:
    return dataset.take(indices)


def _polar_dataset_index(
    dataset: PolarLazyFrame,
    engine: Literal["in-memory", "streaming"],
    indices: Sequence[int],
) -> PolarDataFrame:
    return dataset.select(polar_all().gather(indices)).collect(engine=engine)


def _arrow_dataset_index_slice(
    dataset: ArrowTable, offset: int, length: int
) -> ArrowTable:
    return dataset.slice(offset, length)


def _polar_dataset_index_slice(
    dataset: PolarLazyFrame,
    engine: Literal["in-memory", "streaming"],
    offset: int,
    length: int,
) -> PolarDataFrame:
    return dataset.select(polar_all().slice(offset, length)).collect(engine=engine)


def _benchmark_date_slicing(
    parquet_path: Path,
    arrow_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["arrow", "polars", "polars-streaming"],
    start: datetime,
    end: datetime,
) -> None:
    match framework:
        case "arrow":
            arrow_dataset = memory_map_dataset(
                from_parquet_dataset(parquet_path, arrow_path)
            )

            sliced = benchmark(_arrow_dataset_date_slice, arrow_dataset, start, end)
            assert len(sliced)

        case "polars":
            polar_frame = scan_parquet(parquet_path / "**" / "*.parquet")
            polar_frame = polar_frame.set_sorted(OHLC.date_time)

            sliced = benchmark(
                _polar_dataset_date_slice,
                polar_frame,
                "in-memory",
                start,
                end,
                OHLC.date_time,
            )

            assert len(sliced)

        case "polars-streaming":
            polar_frame = scan_parquet(parquet_path / "**" / "*.parquet")
            polar_frame = polar_frame.set_sorted(OHLC.date_time)

            sliced = benchmark(
                _polar_dataset_date_slice,
                polar_frame,
                "streaming",
                start,
                end,
                OHLC.date_time,
            )

            assert len(sliced)

        case _:
            assert_never(framework)


def _benchmark_indexing(
    parquet_path: Path,
    arrow_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["arrow", "polars", "polars-streaming"],
    indices: Sequence[int],
) -> None:
    match framework:
        case "arrow":
            arrow_dataset = memory_map_dataset(
                from_parquet_dataset(parquet_path, arrow_path)
            )

            sliced = benchmark(_arrow_dataset_index, arrow_dataset.to_table(), indices)
            assert len(sliced) == len(indices)

        case "polars":
            polar_frame = scan_parquet(parquet_path / "**" / "*.parquet")
            polar_frame = polar_frame.set_sorted(OHLC.date_time)

            sliced = benchmark(_polar_dataset_index, polar_frame, "in-memory", indices)
            assert len(sliced) == len(indices)

        case "polars-streaming":
            polar_frame = scan_parquet(parquet_path / "**" / "*.parquet")
            polar_frame = polar_frame.set_sorted(OHLC.date_time)

            sliced = benchmark(_polar_dataset_index, polar_frame, "streaming", indices)
            assert len(sliced) == len(indices)

        case _:
            assert_never(framework)


def _benchmark_index_slicing(
    parquet_path: Path,
    arrow_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["arrow", "polars", "polars-streaming"],
    offset: int,
    length: int,
) -> None:
    match framework:
        case "arrow":
            arrow_dataset = memory_map_dataset(
                from_parquet_dataset(parquet_path, arrow_path)
            )

            sliced = benchmark(
                _arrow_dataset_index_slice, arrow_dataset.to_table(), offset, length
            )
            assert len(sliced) == length

        case "polars":
            polar_frame = scan_parquet(parquet_path / "**" / "*.parquet")
            polar_frame = polar_frame.set_sorted(OHLC.date_time)

            sliced = benchmark(
                _polar_dataset_index_slice, polar_frame, "in-memory", offset, length
            )
            assert len(sliced) == length

        case "polars-streaming":
            polar_frame = scan_parquet(parquet_path / "**" / "*.parquet")
            polar_frame = polar_frame.set_sorted(OHLC.date_time)

            sliced = benchmark(
                _polar_dataset_index_slice, polar_frame, "streaming", offset, length
            )
            assert len(sliced) == length

        case _:
            assert_never(framework)


# endregion


@pytest.mark.parametrize("framework", ["arrow", "polars", "polars-streaming"])
def test_benchmark_dataset_date_slice_short(
    tmp_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["arrow", "polars", "polars-streaming"],
) -> None:
    write_parquet_sample(
        tmp_path / "parquet-data",
        ParquetTimeSeriesPartition.MONTH,
        start=datetime(2020, 1, 1, tzinfo=UTC),
        end=datetime(2025, 1, 1, tzinfo=UTC),
    )

    _benchmark_date_slicing(
        tmp_path / "parquet-data",
        tmp_path / "arrow-data",
        benchmark,
        framework,
        datetime(2024, 2, 25, 4, 30, tzinfo=UTC),
        datetime(2024, 2, 25, 5, 45, tzinfo=UTC),
    )


@pytest.mark.parametrize("framework", ["arrow", "polars", "polars-streaming"])
def test_benchmark_arrow_dataset_date_slice_long(
    tmp_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["arrow", "polars", "polars-streaming"],
) -> None:
    write_parquet_sample(
        tmp_path / "parquet-data",
        ParquetTimeSeriesPartition.MONTH,
        start=datetime(2020, 1, 1, tzinfo=UTC),
        end=datetime(2025, 1, 1, tzinfo=UTC),
    )

    _benchmark_date_slicing(
        tmp_path / "parquet-data",
        tmp_path / "arrow-data",
        benchmark,
        framework,
        datetime(2021, 2, 15, 12, 30, tzinfo=UTC),
        datetime(2024, 7, 10, 17, 45, tzinfo=UTC),
    )


@pytest.mark.parametrize("framework", ["arrow", "polars", "polars-streaming"])
def test_benchmark_arrow_dataset_indexing(
    tmp_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["arrow", "polars", "polars-streaming"],
) -> None:
    write_parquet_sample(
        tmp_path / "parquet-data",
        ParquetTimeSeriesPartition.MONTH,
        start=datetime(2020, 1, 1, tzinfo=UTC),
        end=datetime(2025, 1, 1, tzinfo=UTC),
    )

    _benchmark_indexing(
        tmp_path / "parquet-data",
        tmp_path / "arrow-data",
        benchmark,
        framework,
        [0, 2_500_000, 100_000, 1, 2, 3, 4, 5, 2_000_000, 200_000, 500_000, 1_000_000],
    )


@pytest.mark.parametrize("framework", ["arrow", "polars", "polars-streaming"])
def test_benchmark_arrow_dataset_index_slicing(
    tmp_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["arrow", "polars", "polars-streaming"],
) -> None:
    write_parquet_sample(
        tmp_path / "parquet-data",
        ParquetTimeSeriesPartition.MONTH,
        start=datetime(2020, 1, 1, tzinfo=UTC),
        end=datetime(2025, 1, 1, tzinfo=UTC),
    )

    _benchmark_index_slicing(
        tmp_path / "parquet-data",
        tmp_path / "arrow-data",
        benchmark,
        framework,
        1_200_000,
        450_000,
    )
