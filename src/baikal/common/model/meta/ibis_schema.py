from typing import Literal

from ibis import Schema, Table as IbisTable

type ValidationMode = Literal["normal", "strict"]


class IbisSchema:
    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    def cast(self, table: IbisTable) -> IbisTable:
        raise NotImplementedError

    def drop(self, table: IbisTable) -> IbisTable:
        raise NotImplementedError

    def validate(self, table: IbisTable, mode: ValidationMode = "normal") -> IbisTable:
        raise NotImplementedError
