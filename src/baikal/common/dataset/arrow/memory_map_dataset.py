from collections.abc import Iterable
from pathlib import Path

from pyarrow import ipc, memory_map

from baikal.common.dataset.arrow import ArrowDataset
from baikal.common.dataset.arrow.batch_with_metadata import BatchWithMetaData
from baikal.common.dataset.arrow.record_batch_metadata import RecordBatchMetaData


def memory_map_dataset(files: Path | Iterable[Path]) -> ArrowDataset:
    arrow_files = _collect_arrow_files(files)

    batches_with_metadata: list[BatchWithMetaData] = []
    for arrow_file in arrow_files:
        batches_with_metadata.extend(_memory_map_arrow(arrow_file))

    return ArrowDataset(batches_with_metadata)


# region Private


def _collect_arrow_files(files: Path | Iterable[Path]) -> tuple[Path, ...]:
    if not isinstance(files, Path):
        return tuple(files)

    return tuple(path for path in files.rglob("*.arrow"))


def _memory_map_arrow(path: Path) -> list[BatchWithMetaData]:
    batches: list[BatchWithMetaData] = []
    with memory_map(path.as_posix(), "rb") as source, ipc.open_file(source) as reader:
        for index in range(reader.num_record_batches):
            batch_with_metadata = reader.get_batch_with_custom_metadata(index)

            batches.append(
                BatchWithMetaData(
                    batch=batch_with_metadata.batch,
                    metadata=RecordBatchMetaData.model_validate(
                        batch_with_metadata.custom_metadata
                    ),
                )
            )

    return batches


# endregion
