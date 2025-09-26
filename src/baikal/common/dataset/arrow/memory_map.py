from collections.abc import Iterable
from pathlib import Path
from typing import Any, overload

from attrs import frozen
from polars import DataFrame as PolarDataFrame, from_arrow
from pyarrow import Table as ArrowTable, ipc, memory_map as mmap
from pydantic import BaseModel

from baikal.common.dataset.arrow.batch_with_metadata import BatchWithMetaData
from baikal.common.models import ExtraAllowModel


@frozen
class MemoryMappedBatches[T: BaseModel]:
    batches: tuple[BatchWithMetaData[T], ...]

    def to_polars(self) -> PolarDataFrame:
        frame = from_arrow(
            (meta_batch.data for meta_batch in self.batches), rechunk=False
        )

        assert isinstance(frame, PolarDataFrame)
        return frame

    def to_pyarrow(self) -> ArrowTable:
        return ArrowTable.from_batches(meta_batch.data for meta_batch in self.batches)


@overload
def memory_map(
    files: Path | Iterable[Path],
) -> MemoryMappedBatches[ExtraAllowModel]: ...


@overload
def memory_map[T: BaseModel](
    files: Path | Iterable[Path], metadata_model: type[T]
) -> MemoryMappedBatches[T]: ...


def memory_map[T: BaseModel](
    files: Path | Iterable[Path],
    metadata_model: type[T | ExtraAllowModel] = ExtraAllowModel,
) -> MemoryMappedBatches[ExtraAllowModel] | MemoryMappedBatches[T]:
    arrow_files = _collect_arrow_files(files)

    batches_with_metadata: list[Any] = []
    for arrow_file in arrow_files:
        batches_with_metadata.extend(_memory_map_arrow(arrow_file, metadata_model))

    return MemoryMappedBatches(tuple(batches_with_metadata))


# region Private


def _collect_arrow_files(files: Path | Iterable[Path]) -> tuple[Path, ...]:
    if not isinstance(files, Path):
        return tuple(files)

    return tuple(path for path in files.rglob("*.arrow"))


def _memory_map_arrow[T: BaseModel](
    path: Path, metadata_model: type[T]
) -> list[BatchWithMetaData[T]]:
    batches: list[BatchWithMetaData[T]] = []
    with mmap(path.as_posix(), "rb") as source, ipc.open_file(source) as reader:
        for index in range(reader.num_record_batches):
            batch_with_metadata = reader.get_batch_with_custom_metadata(index)
            raw_metadata = {
                key.decode("utf-8"): value
                for key, value in batch_with_metadata.custom_metadata.items()
            }

            batches.append(
                BatchWithMetaData(
                    data=batch_with_metadata.batch,
                    metadata=metadata_model.model_validate(raw_metadata),
                )
            )

    return batches


# endregion
