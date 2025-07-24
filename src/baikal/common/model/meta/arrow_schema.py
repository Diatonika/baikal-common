from typing import Literal

from pyarrow import Schema, Table as ArrowTable

from baikal.common.model.meta.base_schema import BaseSchema

type ValidationMode = Literal["normal", "strict"]


class ArrowSchema(BaseSchema[ArrowTable]):
    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    def cast(self, table: ArrowTable) -> ArrowTable:
        raise NotImplementedError

    def drop(self, table: ArrowTable) -> ArrowTable:
        raise NotImplementedError

    def validate(
        self,
        table: ArrowTable,
        mode: ValidationMode = "normal",
    ) -> ArrowTable:
        raise NotImplementedError
