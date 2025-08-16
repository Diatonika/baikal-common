from datetime import UTC, datetime, timedelta
from pathlib import Path
from random import shuffle

from pyarrow import OSFile, ipc

from baikal.common.dataset.arrow import (
    ArrowDataset,
    RecordBatchMetaData,
    from_parquet_dataset,
    memory_map_dataset,
)
from baikal.common.dataset.parquet import ParquetTimeSeriesPartition
from baikal.common.models import OHLC
from tests.dataset.util import write_parquet_sample


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


def _default_memory_map_dataset_asserts(dataset: ArrowDataset) -> None:
    assert len(dataset.batches)

    assert min(batch.metadata.min for batch in dataset.batches) == datetime(
        2020, 1, 1, tzinfo=UTC
    )

    assert max(batch.metadata.max for batch in dataset.batches) == datetime(
        2020, 12, 31, 23, 59, tzinfo=UTC
    )


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
