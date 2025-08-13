from collections.abc import Mapping
from datetime import UTC
from pathlib import Path
from typing import cast

from pyarrow import DataType, float64, schema as create_schema, timestamp

from baikal.common.dataset.arrow import from_parquet_dataset
from baikal.common.dataset.parquet import ParquetTimeSeriesPartition
from baikal.common.models import OHLC
from tests.dataset.util import write_parquet_sample


def test_from_parquet_dataset(tmp_path: Path) -> None:
    write_parquet_sample(tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH)

    schema = create_schema(
        cast(
            Mapping[str, DataType],
            {
                OHLC.date_time: timestamp("us", UTC),
                OHLC.open: float64(),
                OHLC.high: float64(),
                OHLC.low: float64(),
                OHLC.close: float64(),
            },
        )
    )

    files = from_parquet_dataset(
        tmp_path / "parquet-data", tmp_path / "arrow-data", schema=schema
    )

    assert len(files) == 12
