from datetime import datetime
from enum import StrEnum

from baikal.common.models.pydantic import ExtraAllowModel


class SortOrder(StrEnum):
    ASCENDING = "ascending"
    DESCENDING = "descending"


class TimeSeriesMetaData(ExtraAllowModel):
    min: datetime
    max: datetime

    sort_column: str
    sort_order: SortOrder
