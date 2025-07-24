from collections.abc import Mapping
from typing import Any, Literal, cast

from polars import (
    DataFrame as PolarDataFrame,
    DataType as PolarDataType,
    LazyFrame as PolarLazyFrame,
    Schema,
)
from polars.exceptions import InvalidOperationError

from baikal.common.model.exceptions import CastError, ValidationError
from baikal.common.model.meta.base_schema import BaseSchema

type PolarFrame = PolarDataFrame | PolarLazyFrame
type ValidationMode = Literal["strict", "normal"]


class PolarSchema(BaseSchema[PolarFrame]):
    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    def cast[T: PolarFrame](self, table: T) -> T:
        mapping: Mapping[Any, PolarDataType] = dict(self.schema.items())

        try:
            return cast(T, table.cast(mapping))
        except InvalidOperationError as error:
            raise CastError from error

    def drop[T: PolarFrame](self, table: T) -> T:
        cleaned = table.select(self.schema.names())
        return cast(T, cleaned)

    def validate[T: PolarFrame](self, table: T, mode: ValidationMode = "normal") -> T:
        actual_schema = table.collect_schema()

        if mode == "normal":
            actual_schema = self._drop_excess_columns(self.schema, actual_schema)

        if self.schema != actual_schema:
            message = (
                f"Polar schema does not match the expected schema. "
                f"Expected: {self.schema}. "
                f"Actual: {actual_schema}."
            )

            raise ValidationError(message)

        return table

    @staticmethod
    def _drop_excess_columns(expected: Schema, actual: Schema) -> Schema:
        schema_copy = Schema(actual, check_dtypes=False)

        difference = set(actual.names()) - set(expected.names())
        for name in difference:
            schema_copy.pop(name)

        return schema_copy