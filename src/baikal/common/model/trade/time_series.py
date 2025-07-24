from typing import Annotated

from ibis.expr.datatypes import Timestamp

from baikal.common.model.meta import BaseModel


class TimeSeries(BaseModel):
    date_time: Annotated[Timestamp, "UTC", 6, False]
