from typing import (
    Annotated,
    Any,
    ClassVar,
    get_origin,
    get_type_hints,
)

from ibis import DataType as IbisDataType, Schema as OriginIbisSchema


class MetaModel(type):
    def __init__(
        cls,
        name: str,
        bases: tuple[type, ...],
        attrs: dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(name, bases, attrs, **kwargs)

        cls.types = {}
        for variable, hint in get_type_hints(cls, include_extras=True).items():
            outer_type = hint.__origin__ if get_origin(hint) is Annotated else hint
            args = hint.__metadata__ if get_origin(hint) is Annotated else ()

            outer_type_origin = get_origin(outer_type) or outer_type
            if outer_type_origin is ClassVar:
                continue

            cls.types[variable] = cls._construct_type(outer_type_origin, *args)

    @property
    def _origin_ibis_schema(cls) -> OriginIbisSchema:
        return OriginIbisSchema.from_tuples(cls.types.items())

    @staticmethod
    def _construct_type[T: IbisDataType](dtype: type[T], *args: Any) -> T:
        if not issubclass(dtype, IbisDataType):
            message = f"Unsupported dtype: {dtype}. Only Ibis types are supported."
            raise TypeError(message)

        return dtype(*args)
