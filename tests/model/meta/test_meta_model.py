from typing import Annotated

from ibis.expr.datatypes import Array, Float64, Int32

from baikal.common.model.meta import BaseModel


class SimpleModel(BaseModel):
    a: Float64
    b: Annotated[Array[Int32], Int32(False), None, False]


def test_meta_model() -> None:
    expected = {
        "a": Float64(),
        "b": Array(value_type=Int32(False), length=None, nullable=False),
    }

    assert expected == SimpleModel.types