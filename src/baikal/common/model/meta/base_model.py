from typing import ClassVar

from ibis import DataType as IbisDataType
from polars import Schema as OriginPolarSchema

from baikal.common.model.meta.arrow_schema import ArrowSchema
from baikal.common.model.meta.ibis_schema import IbisSchema
from baikal.common.model.meta.polar_schema import PolarSchema

from baikal.common.model.meta.meta_model import MetaModel


class BaseModel(metaclass=MetaModel):
    types: ClassVar[dict[str, IbisDataType]]

    @classmethod
    def arrow(cls) -> ArrowSchema:
        return ArrowSchema(cls._origin_ibis_schema.to_pyarrow())

    @classmethod
    def ibis(cls) -> IbisSchema:
        return IbisSchema(cls._origin_ibis_schema)

    @classmethod
    def polar(cls) -> PolarSchema:
        return PolarSchema(OriginPolarSchema(cls._origin_ibis_schema.to_polars()))
