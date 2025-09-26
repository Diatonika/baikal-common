from attrs import define
from pyarrow import RecordBatch
from pydantic import BaseModel


@define
class BatchWithMetaData[T: BaseModel]:
    data: RecordBatch
    metadata: T
