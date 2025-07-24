import functools
from abc import ABC, abstractmethod
from collections.abc import Callable
from inspect import signature
from typing import Any, Literal, cast

type ValidationMode = Literal["normal", "strict"]


class BaseSchema[T](ABC):
    @abstractmethod
    def cast(self, table: T) -> T: ...

    @abstractmethod
    def drop(self, table: T) -> T: ...

    @abstractmethod
    def validate(self, table: T, mode: ValidationMode = "normal") -> T: ...

    def input[C: Callable[..., Any]](
        self,
        parameter: str,
        getter: Callable[..., Any] | None = None,
        mode: ValidationMode = "normal",
    ) -> Callable[[C], C]:
        def outer_wrapper(func: C) -> C:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                bind = signature(func).bind(*args, **kwargs)
                bind.apply_defaults()

                candidate = bind.arguments[parameter]
                frame = candidate if getter is None else getter(candidate)
                self.validate(frame, mode)

                return func(*args, **kwargs)

            return cast(C, wrapper)

        return outer_wrapper

    def output[C: Callable[..., Any]](
        self,
        getter: Callable[..., T] | None = None,
        mode: ValidationMode = "normal",
    ) -> Callable[[C], C]:
        def outer_wrapper(func: C) -> C:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                output = func(*args, **kwargs)

                frame = output if getter is None else getter(output)
                self.validate(cast(T, frame), mode)

                return output

            return cast(C, wrapper)

        return outer_wrapper