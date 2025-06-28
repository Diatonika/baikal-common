import datetime


class TimeFraction:
    def __init__(self, start: datetime.datetime, end: datetime.datetime) -> None:
        self._start = start
        self._interval = (end - start).total_seconds()

    def fraction(self, current: datetime.datetime) -> float:
        return (current - self._start).total_seconds() / self._interval
