from abc import ABC, abstractmethod
from types import TracebackType

from baikal.common.dataset import TimeSeriesReader


class TimeSeriesReaderFactory(ABC):
    @abstractmethod
    def __enter__(self) -> TimeSeriesReader: ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...
