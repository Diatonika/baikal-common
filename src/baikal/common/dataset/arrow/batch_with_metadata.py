from attrs import define
from pyarrow import RecordBatch

from baikal.common.dataset.arrow.record_batch_metadata import RecordBatchMetaData


@define
class BatchWithMetaData:
    batch: RecordBatch
    metadata: RecordBatchMetaData
