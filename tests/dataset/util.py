from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from pandera.typing.polars import DataFrame
from polars import DataFrame as PolarDataFrame, col, datetime_range
from pyarrow import (
    DataType,
    Schema as ArrowSchema,
    float64,
    schema as create_schema,
    timestamp,
)

from baikal.common.dataset.parquet import (
    ParquetTimeSeriesPartition,
    ParquetTimeSeriesWriter,
)
from baikal.common.models import OHLC


def write_parquet_sample(
    root: Path,
    partition: ParquetTimeSeriesPartition,
    *,
    start: datetime = datetime(2020, 1, 1, tzinfo=UTC),
    end: datetime = datetime(2021, 1, 1, tzinfo=UTC),
    chunk_interval: timedelta = timedelta(days=10),
    record_interval: timedelta = timedelta(minutes=1),
) -> ArrowSchema:
    with ParquetTimeSeriesWriter(root, partition) as writer:
        for range_start in datetime_range(
            start,
            end,
            interval=chunk_interval,
            closed="left",
            eager=True,
        ):
            polar_frame = (
                PolarDataFrame(
                    {
                        OHLC.date_time: datetime_range(
                            range_start,
                            range_start + chunk_interval,
                            interval=record_interval,
                            closed="left",
                            eager=True,
                        ),
                    }
                )
                .with_columns(
                    open=col(OHLC.date_time).dt.hour(),
                    high=col(OHLC.date_time).dt.hour() + 1,
                    low=col(OHLC.date_time).dt.hour() - 1,
                    close=col(OHLC.date_time).dt.hour(),
                )
                .filter(col(OHLC.date_time) < end)
            )

            writer.write(DataFrame[OHLC](polar_frame))

    return create_schema(
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
