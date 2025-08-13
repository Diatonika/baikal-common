from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class RecordBatchMetaData(BaseModel):
    min: datetime
    max: datetime

    sort_column: str
    sort_order: Literal["ascending", "descending"]
