from typing import Annotated

import pytest

from ibis.expr.datatypes import Array, Int64
from polars import (
    DataFrame,
    Int64 as PolarInt64,
    List as PolarList,
    Schema as PolarSchema,
    String as PolarString,
)

from baikal.common.model.meta import BaseModel
from baikal.common.model.exceptions import ValidationError


class SimpleModel(BaseModel):
    field: Int64


class ComplexModel(BaseModel):
    field: Int64
    complex: Annotated[Array[Int64], Int64, None, False]


def test_polar_schema() -> None:
    assert PolarSchema({"field": PolarInt64}) == SimpleModel.polar().schema

    dtypes = {"field": PolarInt64(), "complex": PolarList(PolarInt64)}
    expected = PolarSchema(dtypes)
    assert expected == ComplexModel.polar().schema


def test_polar_cast() -> None:
    frame = DataFrame({"field": ["1", "2", "3"]}, {"field": PolarString})
    casted = SimpleModel.polar().cast(frame)

    assert PolarInt64 == casted["field"].dtype
    assert casted["field"].to_list() == [1, 2, 3]


def test_polar_validate() -> None:
    frame = DataFrame({"field": ["1", "2", "3"]}, {"field": PolarString})

    with pytest.raises(ValidationError):
        SimpleModel.polar().validate(frame)

    casted = SimpleModel.polar().cast(frame)
    SimpleModel.polar().validate(casted)


def test_polar_clean() -> None:
    frame = DataFrame(
        {"field": [1, 2, 3], "other": ["A", "B", "C"]},
        {"field": PolarInt64, "other": PolarString},
    )

    expected = DataFrame({"field": [1, 2, 3]}, {"field": PolarInt64})
    assert expected.equals(SimpleModel.polar().drop(frame))


def test_polar_validate_strict() -> None:
    frame = DataFrame(
        {"field": [1, 2, 3], "other": ["A", "B", "C"]},
        {"field": PolarInt64, "other": PolarString},
    )

    with pytest.raises(ValidationError):
        SimpleModel.polar().validate(frame, "strict")

    SimpleModel.polar().validate(frame.drop("other"), "strict")


def test_polar_input_validation() -> None:
    @SimpleModel.polar().input("_")
    def func(_: DataFrame) -> None:
        return None

    with pytest.raises(ValidationError):
        func(DataFrame({"field": ["1", "2", "3"]}, {"field": PolarString}))

    func(DataFrame({"field": [1, 2, 3]}, {"field": PolarInt64}))


def test_polar_input_validation_extractor() -> None:
    @SimpleModel.polar().input("struct", lambda struct: struct[1])
    def func(struct: tuple[int, DataFrame], *, keyword: int = 0) -> None:
        return None

    frame = DataFrame({"field": ["1", "2", "3"]}, {"field": PolarString})
    with pytest.raises(ValidationError):
        func((0, frame), keyword=1)

    frame = DataFrame({"field": [1, 2, 3]}, {"field": PolarInt64})
    func((0, frame), keyword=2)


def test_polar_output_validation() -> None:
    @SimpleModel.polar().output()
    def func_invalid() -> DataFrame:
        return DataFrame({"field": ["1", "2", "3"]}, {"field": PolarString})

    @SimpleModel.polar().output()
    def func_valid() -> DataFrame:
        return DataFrame(
            {"field": [1, 2, 3], "other": ["A", "B", "C"]},
            {"field": PolarInt64, "other": PolarString},
        )

    with pytest.raises(ValidationError):
        func_invalid()

    frame = func_valid()
    assert PolarInt64 == frame["field"].dtype
    assert frame["field"].to_list() == [1, 2, 3]


def test_polar_output_validation_extractor() -> None:
    @SimpleModel.polar().output(lambda struct: struct[1])
    def func_with_extractor() -> tuple[str, DataFrame]:
        dataframe = DataFrame({"field": [1, 2, 3]}, {"field": PolarInt64})
        return "dummy", dataframe

    dummy, frame = func_with_extractor()
    assert PolarInt64 == frame["field"].dtype
    assert frame["field"].to_list() == [1, 2, 3]


