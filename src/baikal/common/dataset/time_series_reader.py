from abc import ABC, abstractmethod
from datetime import datetime

from pyarrow import Table as ArrowTable


class TimeSeriesReader(ABC):
    @abstractmethod
    def min(self) -> datetime: ...

    @abstractmethod
    def max(self) -> datetime: ...

    @abstractmethod
    def get(self, at: datetime) -> ArrowTable: ...

    @abstractmethod
    def slice(self, start: datetime, end: datetime) -> ArrowTable: ...
