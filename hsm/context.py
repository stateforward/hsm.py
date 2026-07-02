import dataclasses
import collections.abc
import concurrent.futures
import types
import typing


class ContextError(Exception):
    pass


class Canceled(ContextError):
    pass


class DeadlineExceeded(ContextError):
    pass


CanceledError = Canceled("context canceled")
DeadlineExceededError = DeadlineExceeded("context deadline exceeded")


@dataclasses.dataclass(frozen=True)
class ContextKey:
    name: str


class Context:
    def __init__(
        self,
        parent: "Context | None" = None,
        values: collections.abc.Mapping[typing.Hashable, object] | None = None,
    ):
        self._done = concurrent.futures.Future[None]()
        self._parent = parent
        self._values = types.MappingProxyType(dict(values or {}))
        if self._parent is not None:
            self._parent.Done().add_done_callback(lambda _: self.cancel())

    def is_done(self) -> bool:
        return self._done.done()

    def Deadline(self) -> tuple[None, bool]:
        return None, False

    deadline = Deadline

    def Err(self) -> ContextError | None:
        if self._done.done():
            return CanceledError
        return None

    err = Err

    def cancel(self) -> None:
        try:
            self._done.set_result(None)
        except concurrent.futures.InvalidStateError:
            pass

    def Done(self) -> concurrent.futures.Future[None]:
        return self._done

    done = Done

    def Value(self, key: typing.Hashable) -> object | None:
        if key in self._values:
            return self._values[key]
        if self._parent is not None:
            return self._parent.Value(key)
        return None

    value = Value

    def WithValue(self, key: typing.Hashable, value: object) -> "Context":
        return Context(
            self,
            values={key: value},
        )

    with_value = WithValue

    def WithCancel(self) -> tuple["Context", typing.Callable[[], None]]:
        return with_cancel(self)

    with_cancel = WithCancel


def with_cancel(ctx: "Context") -> tuple["Context", typing.Callable[[], None]]:
    new_ctx = Context(parent=ctx)
    return new_ctx, new_ctx.cancel


def with_value(ctx: "Context", key: typing.Hashable, value: object) -> "Context":
    return Context(
        parent=ctx,
        values={key: value},
    )


def new_context(
    parent: "Context | None" = None,
    values: collections.abc.Mapping[typing.Hashable, object] | None = None,
) -> "Context":
    return Context(parent=parent, values=values)
