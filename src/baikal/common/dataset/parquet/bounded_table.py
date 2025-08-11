from datetime import datetime

from attrs import define
from pyarrow import Table as ArrowTable


@define
class BoundedTable:
    minimum: datetime
    maximum: datetime

    table: ArrowTable
