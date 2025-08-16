from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class SortOrder(StrEnum):
    ASCENDING = "ascending"
    DESCENDING = "descending"


class RecordBatchMetaData(BaseModel):
    min: datetime
    max: datetime

    sort_column: str
    sort_order: SortOrder
