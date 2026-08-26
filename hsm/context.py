import dataclasses
import collections.abc
import concurrent.futures
import posixpath
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


def _normalize_path(path: object) -> str:
    if not isinstance(path, str):
        raise TypeError("path must be a string")
    normalized = posixpath.normpath("/" + path.lstrip("/"))
    return "/" if normalized == "." else normalized


def _direct_descendants(
    values: collections.abc.Mapping[str, object], scope: str
) -> tuple[str, ...]:
    prefix = "/" if scope == "/" else scope + "/"
    descendants: list[str] = []
    seen: set[str] = set()
    for path in values:
        if not path.startswith(prefix):
            continue
        remainder = path[len(prefix) :]
        child = remainder.split("/", 1)[0]
        descendant = prefix + child if prefix != "/" else "/" + child
        if child and descendant not in seen:
            seen.add(descendant)
            descendants.append(descendant)
    return tuple(descendants)


class Context:
    def __init__(
        self,
        parent: "Context | None" = None,
        values: collections.abc.Mapping[typing.Hashable, object] | None = None,
        *,
        path_values: collections.abc.Mapping[str, object] | None = None,
    ):
        self._done = concurrent.futures.Future[None]()
        self._parent = parent
        self._values = types.MappingProxyType(dict(values or {}))
        empty_path_values: collections.abc.Mapping[str, object] = {}
        if path_values is not None:
            inherited_path_values: collections.abc.Mapping[str, object] = (
                types.MappingProxyType(dict(path_values))
            )
        elif parent is not None:
            inherited_path_values = typing.cast(
                collections.abc.Mapping[str, object],
                getattr(parent, "_path_values", empty_path_values),
            )
        else:
            inherited_path_values = types.MappingProxyType(empty_path_values)
        self._path_values: collections.abc.Mapping[str, object] = (
            inherited_path_values
        )
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

    def PathValue(self, path: object) -> object | None:
        return self._path_values.get(_normalize_path(path))

    path_value = PathValue

    def WithPathValue(self, path: str, value: object) -> "Context":
        path_values = dict(self._path_values)
        path_values[_normalize_path(path)] = value
        return Context(self, path_values=path_values)

    with_path_value = WithPathValue

    def Subcontext(self, path: str = "/") -> "Subcontext":
        return Subcontext(self, path)

    subcontext = Subcontext

    def WithCancel(self) -> tuple["Context", typing.Callable[[], None]]:
        return with_cancel(self)

    with_cancel = WithCancel


class Subcontext:
    """A lightweight direct-child view over a Context's flattened path store."""

    def __init__(
        self,
        root: Context,
        scope: str = "/",
    ):
        self._root = root
        self._scope = _normalize_path(scope)
        values = typing.cast(
            collections.abc.Mapping[str, object],
            getattr(root, "_path_values", {}),
        )
        self._paths = _direct_descendants(values, self._scope)

    @property
    def paths(self) -> tuple[str, ...]:
        return self._paths

    def _resolve(self, path: object) -> str:
        if not isinstance(path, str):
            raise TypeError("path must be a string")
        if path.startswith("/"):
            return _normalize_path(path)
        return _normalize_path(posixpath.join(self._scope, path))

    def Value(self, path: object) -> object | None:
        return self._root.PathValue(self._resolve(path))

    value = Value

    def WithValue(self, path: object, value: object) -> "Subcontext":
        return Subcontext(
            self._root.WithPathValue(self._resolve(path), value),
            self._scope,
        )

    with_value = WithValue

    def Subcontext(self, path: str = "") -> "Subcontext":
        return Subcontext(self._root, self._resolve(path))

    subcontext = Subcontext

    def Done(self) -> concurrent.futures.Future[None]:
        return self._root.Done()

    done = Done

    def Err(self) -> ContextError | None:
        return self._root.Err()

    err = Err

    def is_done(self) -> bool:
        return self._root.is_done()


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
