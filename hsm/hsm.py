from __future__ import annotations

import asyncio
import builtins
import collections
import collections.abc
import copy
import fnmatch
import functools
import inspect
import posixpath
import re
import sys
import threading
import typing
from dataclasses import dataclass, field
from typing import final
from datetime import datetime, timedelta
from enum import IntEnum
from types import MappingProxyType, SimpleNamespace

from .kind import IsKind, MakeKind, is_kind, make_kind

TElement = typing.TypeVar("TElement", bound="Element")
TInstance = typing.TypeVar("TInstance", bound="Instance")
TData = typing.TypeVar("TData", default=typing.Any)
TKey = typing.TypeVar("TKey", bound=typing.Hashable)
TValue = typing.TypeVar("TValue")
TNewData = typing.TypeVar("TNewData")
_next_id_counter = 0

OperationCallback = typing.Callable[
    ["Context", TInstance, "Event"],
    typing.Awaitable[None] | None,
]
BehaviorArgument = OperationCallback[TInstance] | str
Expression = typing.Callable[
    ["Context", TInstance, "Event"],
    typing.Awaitable[bool] | bool,
]
ExpressionArgument = Expression[TInstance] | str
expression = Expression
Duration = typing.Callable[
    ["Context", TInstance, "Event"],
    typing.Awaitable[timedelta] | timedelta,
]
Timepoint = typing.Callable[
    ["Context", TInstance, "Event"],
    typing.Awaitable[datetime] | datetime,
]
SleepFunction = typing.Callable[[timedelta], typing.Awaitable[None] | None]
WhenExpression = typing.Callable[
    ["Context", TInstance, "Event"],
    typing.Any,
]
OperationImplementation = typing.Callable[..., typing.Any]


def traceback() -> tuple[str, int]:
    frame = sys._getframe(3)  # type: ignore
    return frame.f_code.co_filename, frame.f_lineno


def join(path: str, *paths: str) -> str:
    # path.Join equivalent for DSL paths; used during model definition and finalization.
    return posixpath.normpath(posixpath.join(path, *paths))


@functools.lru_cache(maxsize=None)
def _parent_path(path: str) -> str:
    # Cached path.Dir equivalent; used by finalized tables and runtime lookup.
    return posixpath.dirname(path)


def _future_done() -> asyncio.Future[None]:
    # Python returns awaitables where Go returns closed channels.
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.set_result(None)
    return future


async def _maybe_await(value: typing.Any) -> typing.Any:
    # User behaviors can be sync or async Python callables; Go has one call shape.
    if inspect.isawaitable(value):
        return await typing.cast(typing.Awaitable[typing.Any], value)
    return value


async def _completed_none() -> None:
    # Keeps no-op public operations awaitable without allocating a task.
    return None


async def _await_all(
    awaitables: collections.abc.Iterable[typing.Awaitable[typing.Any]],
) -> None:
    # asyncio fanout join; Go uses channel synchronization instead.
    await asyncio.gather(*awaitables)


async def _await_all_shielded(
    awaitables: collections.abc.Iterable[typing.Awaitable[typing.Any]],
) -> None:
    # Awaiter cancellation must not cancel already-submitted dispatch work.
    await asyncio.gather(*(asyncio.shield(awaitable) for awaitable in awaitables))


def _dispatch_machines(
    dispatches: collections.abc.Iterable[tuple["HSM[typing.Any]", Event]],
) -> typing.Awaitable[None]:
    pending = list(dispatches)
    if not pending:
        return _completed_none()

    async def run() -> None:
        await _await_all_shielded(machine.dispatch(event) for machine, event in pending)

    return asyncio.create_task(run())


def _callable_is_synchronous(callback: typing.Callable[..., typing.Any]) -> bool:
    try:
        callback = inspect.unwrap(callback)
    except Exception:
        pass
    return not inspect.iscoroutinefunction(callback)


def _validate_synchronous_callback(
    role: str,
    callback: typing.Callable[..., typing.Any],
    traceback: tuple[str, int],
) -> None:
    if not _callable_is_synchronous(callback):
        raise ValidationError(
            f"{traceback[0]}:{traceback[1]}: {role} must be a synchronous function"
        )


def _close_awaitable(value: typing.Any) -> None:
    # Close coroutine probes so sync-path detection does not leak awaitables.
    close = getattr(value, "close", None)
    if callable(close):
        close()


def _wrap_instance_operation_call(
    callback: OperationImplementation,
) -> OperationImplementation:
    if inspect.iscoroutinefunction(callback):

        async def wrapped(
            ctx: "Context", instance: "Instance", *args: typing.Any
        ) -> typing.Any:
            del ctx, instance
            return await callback(*args)

        return wrapped

    def wrapped(ctx: "Context", instance: "Instance", *args: typing.Any) -> typing.Any:
        del ctx, instance
        return callback(*args)

    return wrapped


def _resolve_operation_qualified_name(model: "Model", name: str) -> str:
    if name in model.operations:
        return name
    return _resolve_model_path(model, name)


def _resolve_operation(
    model: "Model", qualified_name: str, requested_name: str | None = None
) -> OperationElement:
    operation = model.operations.get(qualified_name)
    if operation is None:
        raise ValidationError(f'missing operation "{requested_name or qualified_name}"')
    return operation


def _instance_operation_callable(
    operation: OperationElement, instance: "Instance"
) -> OperationImplementation:
    callback = getattr(
        instance,
        operation.declared_name or posixpath.basename(operation.qualified_name),
        None,
    )
    if callback is None:
        raise ValidationError(
            f'missing operation "{operation.declared_name or operation.qualified_name}"'
        )
    return _wrap_instance_operation_call(callback)


def _operation_callback(
    operation: OperationElement,
    instance: "Instance",
) -> OperationImplementation:
    if operation.callback is not None:
        return operation.callback
    return _instance_operation_callable(operation, instance)


def _invoke_operation(
    operation: OperationElement,
    ctx: "Context",
    instance: "Instance",
    event: "Event",
) -> typing.Any:
    callback = (
        operation.callback
        if operation.callback is not None
        else _instance_operation_callable(operation, instance)
    )
    result = callback(ctx, instance, event)
    if inspect.isawaitable(result):
        _close_awaitable(result)
        raise RuntimeError(
            f'operation "{operation.declared_name or operation.qualified_name}" returned awaitable'
        )
    return result


def _operation_behavior_callback(
    operation: OperationElement,
) -> OperationCallback[typing.Any]:
    if operation.callback is not None:
        _validate_synchronous_callback(
            "operation behavior",
            operation.callback,
            ("", 0),
        )
    declared_name = operation.declared_name or posixpath.basename(
        operation.qualified_name
    )

    def invoke(ctx: "Context", instance: "Instance", event: "Event") -> typing.Any:
        return _invoke_operation(operation, ctx, instance, event)

    invoke.__name__ = declared_name
    return invoke


def _operation_async_behavior_callback(
    operation: OperationElement,
) -> OperationCallback[typing.Any]:
    if operation.callback is not None:
        return operation.callback
    declared_name = operation.declared_name or posixpath.basename(
        operation.qualified_name
    )

    async def invoke(
        ctx: "Context", instance: "Instance", event: "Event"
    ) -> typing.Any:
        callback = _instance_operation_callable(operation, instance)
        return await _maybe_await(callback(ctx, instance, event))

    invoke.__name__ = declared_name
    return invoke


def _operation_guard_callback(
    operation: OperationElement,
) -> Expression[typing.Any]:
    if operation.callback is not None:
        _validate_synchronous_callback(
            "operation guard",
            operation.callback,
            ("", 0),
        )
    declared_name = operation.declared_name or posixpath.basename(
        operation.qualified_name
    )

    def guard(ctx: "Context", instance: "Instance", event: "Event") -> bool:
        return bool(_invoke_operation(operation, ctx, instance, event))

    guard.__name__ = declared_name
    return guard


def _finalize_operation_references(model: "Model") -> None:
    for event in model.events.values():
        if event.kind != Kinds.CallEvent:
            continue
        operation_name = event.source or event.qualified_name.removeprefix("@call:")
        qualified_name = _resolve_operation_qualified_name(model, operation_name)
        event.source = _resolve_operation(
            model, qualified_name, operation_name
        ).qualified_name
    for element in model.members.values():
        if isinstance(element, BehaviorElement) and element.operation_name:
            qualified_name = _resolve_operation_qualified_name(
                model, element.operation_name
            )
            operation = _resolve_operation(
                model, qualified_name, element.operation_name
            )
            if element.kind == Kinds.Concurrent:
                element.operation = _operation_async_behavior_callback(operation)
            else:
                element.operation = _operation_behavior_callback(operation)
            element.operation_name = ""
        elif isinstance(element, GuardElement) and element.operation_name:
            qualified_name = _resolve_operation_qualified_name(
                model, element.operation_name
            )
            operation = _resolve_operation(
                model, qualified_name, element.operation_name
            )
            element.expression = _operation_guard_callback(operation)
            element.operation_name = ""


def _task_is_cancelling() -> bool:
    # asyncio cancellation state drives timer/activity cleanup; Go has no direct equivalent.
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


async def _normalize_waitable(value: typing.Any) -> None:
    # When() accepts runtime Python waitables/events in addition to awaitables.
    if value is None:
        return
    if isinstance(value, asyncio.Event):
        await value.wait()
        return
    if isinstance(value, asyncio.Future):
        await asyncio.shield(value)
        return
    if inspect.isawaitable(value):
        await typing.cast(typing.Awaitable[typing.Any], value)
        return
    wait = getattr(value, "wait", None)
    if callable(wait):
        result = wait()
        if isinstance(result, asyncio.Future):
            await asyncio.shield(result)
            return
        if inspect.isawaitable(result):
            await typing.cast(typing.Awaitable[typing.Any], result)
            return
    raise TypeError(f"unsupported When() result {type(value)!r}")


async def _asyncio_sleep(duration: timedelta) -> None:
    # Default Clock adapter from timedelta to asyncio seconds.
    await asyncio.sleep(duration.total_seconds())


def _next_id() -> str:
    # Instance IDs are runtime data, not model-finalized data.
    global _next_id_counter
    _next_id_counter += 1
    return f"hsm-{_next_id_counter}"


def _namespace(model: Model, stack: list["NamedElement"]) -> str:
    namespace = find(stack, Model, SubmachineStateElement)
    if namespace is not None:
        return namespace.qualified_name
    return model.qualified_name


def _resolve_path(model: Model, stack: list["NamedElement"], name: str) -> str:
    if name == "":
        return ""
    if posixpath.isabs(name):
        qualified = posixpath.normpath(name)
        if (
            IsAncestor(model.qualified_name, qualified)
            or qualified == model.qualified_name
        ):
            return qualified
        return join(model.qualified_name, qualified.lstrip("/"))
    return join(_namespace(model, stack), name)


def _resolve_vertex_path(model: Model, stack: list["NamedElement"], name: str) -> str:
    if name == "":
        return ""
    if posixpath.isabs(name):
        qualified = posixpath.normpath(name)
        if is_path_in_path(qualified, model.qualified_name):
            return qualified
        return join(model.qualified_name, qualified.lstrip("/"))
    state = find(stack, StateElement)
    if state is not None and not name.startswith(state.qualified_name):
        return join(state.qualified_name, name)
    return posixpath.normpath(name)


def _resolve_model_path(model: Model, name: str) -> str:
    return _resolve_path(model, [], name)


def _validate_slashless_name(
    kind: str, name: str, traceback_info: tuple[str, int] | None = None
) -> None:
    # Model-definition validation helper, not dispatch-time work.
    if "/" not in name:
        return
    location = (
        "" if traceback_info is None else f"{traceback_info[0]}:{traceback_info[1]}: "
    )
    raise ValidationError(f'{location}{kind} name "{name}" cannot contain "/"')


def Match(value: str, *patterns: str) -> bool:
    # DispatchTo matches runtime instance IDs, so model finalization cannot decide this.
    if not patterns:
        return False
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


match = Match


def _to_snake_case(name: str) -> str:
    # Import-time alias generation for Python ergonomics.
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return value.lower()


class ValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class ContextKey:
    name: str


class ContextKeys:
    Instances: ContextKey = ContextKey("Instances")
    Owner: ContextKey = ContextKey("Owner")
    HSM: ContextKey = ContextKey("HSM")


Keys = ContextKeys()


@typing.final
class Context:
    def __init__(
        self,
        parent: "Context | None" = None,
        values: collections.abc.Mapping[typing.Hashable, typing.Any] | None = None,
    ):
        self._done = False
        self._parent = parent
        self._listeners: list[typing.Callable[[], None]] = []
        self._done_future: asyncio.Future[None] | None = None
        self._values = MappingProxyType(dict(values or {}))

    @property
    def done(self) -> bool:
        return self._done

    def is_done(self) -> bool:
        return self._done

    def cancel(self) -> None:
        if self._done:
            return
        self._done = True
        for listener in list(self._listeners):
            try:
                listener()
            except Exception:
                pass
        self._listeners.clear()
        if self._done_future is not None and not self._done_future.done():
            self._done_future.set_result(None)

    def add_listener(self, event: str, callback: typing.Callable[[], None]) -> None:
        if event != "done":
            return
        if self._done:
            try:
                callback()
            except Exception:
                pass
            return
        self._listeners.append(callback)

    def remove_listener(self, event: str, callback: typing.Callable[[], None]) -> None:
        if event == "done" and callback in self._listeners:
            self._listeners.remove(callback)

    async def wait_done(self) -> None:
        if self._done:
            return
        if self._done_future is None or self._done_future.cancelled():
            self._done_future = asyncio.get_running_loop().create_future()
        await asyncio.shield(self._done_future)

    def Value(self, key: typing.Hashable) -> TValue:
        if key in self._values:
            return self._values[key]
        if self._parent is not None:
            return self._parent.Value(key)
        return None

    def value(self, key: typing.Hashable) -> typing.Any:
        return self.Value(key)

    def WithValue(self, key: typing.Hashable, value: typing.Any) -> "Context":
        return Context(
            self,
            values={key: value},
        )

    with_value = WithValue


context_key = ContextKey
keys = Keys


def ContextWithValues(
    parent: Context, values: collections.abc.Mapping[typing.Hashable, TValue]
) -> Context:
    return Context(
        parent,
        values,
    )


def ContextValue(ctx: Context, key: typing.Hashable) -> TValue:
    return ctx.Value(key)


def FromContext(ctx: Context) -> tuple["HSM[typing.Any] | Group | None", bool]:
    machine = ctx.Value(Keys.HSM)
    if isinstance(machine, (HSM, Group)):
        return machine, True
    return None, False


def InstancesFromContext(ctx: Context | None) -> tuple[list[typing.Any], bool]:
    if ctx is None:
        return [], False
    instances = ctx.Value(Keys.Instances)
    if isinstance(instances, collections.abc.Mapping):
        return list(instances.values()), True
    if isinstance(instances, collections.abc.Iterable) and not isinstance(
        instances, (str, bytes)
    ):
        return list(instances), True
    return [], False


from_context = FromContext
instances_from_context = InstancesFromContext


def _WithRuntimeHSM(ctx: Context, machine: "HSM[typing.Any] | Group") -> Context:
    # Context is immutable; the mutable instance registry is only a context value.
    instances = ctx.Value(Keys.Instances)
    if not isinstance(instances, collections.abc.MutableMapping):
        instances = {}
        values = dict(ctx._values)
        values[Keys.Instances] = instances
        ctx._values = MappingProxyType(values)
    return (
        ctx.WithValue(Keys.Instances, instances)
        .WithValue(Keys.Owner, ctx.Value(Keys.HSM))
        .WithValue(Keys.HSM, machine)
    )


def _runtime_context_parent(ctx: Context) -> Context:
    current: Context | None = ctx
    if Keys.HSM in current._values:
        current = current._parent
    if current is not None and Keys.Owner in current._values:
        current = current._parent
    if (
        current is not None
        and Keys.Instances in current._values
        and current._parent is not None
    ):
        current = current._parent
    return current or Context()


class Kinds(IntEnum):
    Null = MakeKind()
    Element = MakeKind()
    Partial = MakeKind(Element)
    Namespace = MakeKind(Element)
    NamedElement = MakeKind(Element)
    Vertex = MakeKind(Element)
    State = MakeKind(Vertex, NamedElement, Namespace)
    SubmachineState = MakeKind(State)
    FinalState = MakeKind(State)
    Transition = MakeKind(NamedElement)
    Pseudostate = MakeKind(Vertex)
    Initial = MakeKind(Pseudostate)
    Choice = MakeKind(Pseudostate)
    ShallowHistory = MakeKind(Pseudostate)
    DeepHistory = MakeKind(Pseudostate)
    EntryPoint = MakeKind(Pseudostate)
    ExitPoint = MakeKind(Pseudostate)
    External = MakeKind(Transition)
    Self = MakeKind(Transition)
    Internal = MakeKind(Transition)
    Local = MakeKind(Transition)
    Behavior = MakeKind(NamedElement)
    StateMachine = MakeKind(Behavior, Namespace)
    Concurrent = MakeKind(Behavior)
    Sequential = MakeKind(Behavior)
    Constraint = MakeKind(NamedElement)
    Event = MakeKind(Element)
    CompletionEvent = MakeKind(Event)
    ErrorEvent = MakeKind(CompletionEvent)
    TimeEvent = MakeKind(Event)
    ChangeEvent = MakeKind(Event)
    CallEvent = MakeKind(Event)
    Attribute = MakeKind(NamedElement)
    Operation = MakeKind(NamedElement)


NullKind = Kinds.Null
ElementKind = Kinds.Element
PartialKind = Kinds.Partial
NamespaceKind = Kinds.Namespace
NamedElementKind = Kinds.NamedElement
VertexKind = Kinds.Vertex
ConstraintKind = Kinds.Constraint
BehaviorKind = Kinds.Behavior
ConcurrentKind = Kinds.Concurrent
SequentialKind = Kinds.Sequential
StateMachineKind = Kinds.StateMachine
StateKind = Kinds.State
TransitionKind = Kinds.Transition
InternalKind = Kinds.Internal
ExternalKind = Kinds.External
LocalKind = Kinds.Local
SelfKind = Kinds.Self
EventKind = Kinds.Event
TimeEventKind = Kinds.TimeEvent
CompletionEventKind = Kinds.CompletionEvent
ChangeEventKind = Kinds.ChangeEvent
CallEventKind = Kinds.CallEvent
ErrorEventKind = Kinds.ErrorEvent
PseudostateKind = Kinds.Pseudostate
InitialKind = Kinds.Initial
FinalStateKind = Kinds.FinalState
SubmachineStateKind = Kinds.SubmachineState
ChoiceKind = Kinds.Choice
ShallowHistoryKind = Kinds.ShallowHistory
DeepHistoryKind = Kinds.DeepHistory
EntryPointKind = Kinds.EntryPoint
ExitPointKind = Kinds.ExitPoint
AttributeKind = Kinds.Attribute
OperationKind = Kinds.Operation

kinds = Kinds
null_kind = NullKind
element_kind = ElementKind
partial_kind = PartialKind
namespace_kind = NamespaceKind
named_element_kind = NamedElementKind
vertex_kind = VertexKind
constraint_kind = ConstraintKind
behavior_kind = BehaviorKind
concurrent_kind = ConcurrentKind
sequential_kind = SequentialKind
state_machine_kind = StateMachineKind
state_kind = StateKind
transition_kind = TransitionKind
internal_kind = InternalKind
external_kind = ExternalKind
local_kind = LocalKind
self_kind = SelfKind
event_kind = EventKind
time_event_kind = TimeEventKind
completion_event_kind = CompletionEventKind
change_event_kind = ChangeEventKind
call_event_kind = CallEventKind
error_event_kind = ErrorEventKind
pseudostate_kind = PseudostateKind
initial_kind = InitialKind
final_state_kind = FinalStateKind
submachine_state_kind = SubmachineStateKind
choice_kind = ChoiceKind
shallow_history_kind = ShallowHistoryKind
deep_history_kind = DeepHistoryKind
entry_point_kind = EntryPointKind
exit_point_kind = ExitPointKind
attribute_kind = AttributeKind
operation_kind = OperationKind


@dataclass
class Element:
    kind: int = Kinds.Element
    id: str | None = None
    owned_elements: list["NamedElement"] = field(default_factory=list)

    def owner(self) -> str:
        return ""


@dataclass
class NamespaceElement(Element):
    kind: int = Kinds.Namespace
    members: dict[str, typing.Union["Element", "Event[typing.Any]"]] = field(
        default_factory=dict
    )


@dataclass
class NamedElement(Element):
    kind: int = Kinds.NamedElement
    qualified_name: str = field(default_factory=str)

    def owner(self) -> str:
        if self.qualified_name in ("", "/"):
            return ""
        return _parent_path(self.qualified_name)

    def name(self) -> str:
        return posixpath.basename(self.qualified_name)


def find(
    stack: list["NamedElement"], *kinds: typing.Type[TElement]
) -> typing.Optional[TElement]:
    for element in reversed(stack):
        if isinstance(element, kinds):
            return typing.cast(TElement, element)
    return None


def apply(
    element: "NamedElement",
    model: "Model",
    stack: list["NamedElement"],
    elements: list["NamedElement"],
) -> "NamedElement":
    scoped_stack = [*stack, element]
    for partial in elements:
        if isinstance(partial, PartialElement):
            partial.apply(model, scoped_stack)
    return element


@dataclass
class PartialElement(NamedElement):
    kind: int = Kinds.Partial
    traceback: tuple[str, int] = field(default_factory=traceback)

    def apply(
        self, model: "Model", stack: list["NamedElement"]
    ) -> typing.Optional["NamedElement"]:
        return None


async def noop_operation(ctx: Context, instance: "Instance", event: "Event") -> None:
    return None


_TIMER_ACTIVITY_NAMES = frozenset({"run_after", "run_at", "run_every"})


def _is_timer_activity(behavior: "BehaviorElement[typing.Any]") -> bool:
    return getattr(behavior.operation, "__name__", "") in _TIMER_ACTIVITY_NAMES


@dataclass
class BehaviorElement(typing.Generic[TInstance], NamedElement, NamespaceElement):
    kind: int = Kinds.Behavior
    operation: OperationCallback[TInstance] = field(default=noop_operation)
    operation_name: str = ""
    scope: str = field(default_factory=str)
    defer_events: bool = False


@dataclass
class StateMachineElement(BehaviorElement[TInstance]):
    kind: int = Kinds.StateMachine


@dataclass
class VertexElement(NamedElement):
    kind: int = Kinds.Vertex
    transitions: list[str] = field(default_factory=list)


@dataclass
class StateElement(VertexElement, NamespaceElement):
    kind: int = Kinds.State
    initial: str = field(default_factory=str)
    entry: list[str] = field(default_factory=list)
    exit: list[str] = field(default_factory=list)
    activity: list[str] = field(default_factory=list)
    deferred: list[str] = field(default_factory=list)


@dataclass
class SubmachineStateElement(StateElement):
    kind: int = Kinds.SubmachineState
    machine: "Model | None" = None


@dataclass
class AttributeElement(NamedElement):
    kind: int = Kinds.Attribute
    declared_name: str = ""
    default: typing.Any = None
    value_type: type[typing.Any] | None = None
    dynamic: bool = False
    implicit: bool = False


@dataclass
class OperationElement(NamedElement):
    kind: int = Kinds.Operation
    declared_name: str = ""
    callback: OperationImplementation | None = None


@dataclass
class Clock:
    sleep: SleepFunction | None = None

    def with_defaults(self) -> "Clock":
        return Clock(sleep=self.sleep or _asyncio_sleep)

    async def Sleep(self, duration: timedelta) -> None:
        sleep = self.sleep or _asyncio_sleep
        await _maybe_await(sleep(duration))


DefaultClock = Clock()
clock = Clock
default_clock = DefaultClock


@dataclass(init=False)
class Config:
    ID: str = ""
    Name: str = ""
    Data: typing.Any = None
    Clock: Clock | None = None
    Queue: "Queue | None" = None

    def __init__(
        self,
        ID: str = "",
        Name: str = "",
        Data: typing.Any = None,
        Clock: Clock | None = None,
        Queue: "Queue | None" = None,
        *,
        id: str | None = None,
        name: str | None = None,
        data: typing.Any = None,
        clock: Clock | None = None,
        queue: "Queue | None" = None,
    ) -> None:
        self.ID = ID if id is None else id
        self.Name = Name if name is None else name
        self.Data = Data if data is None else data
        self.Clock = Clock if clock is None else clock
        self.Queue = Queue if queue is None else queue

    @property
    def id(self) -> str:
        return self.ID

    @id.setter
    def id(self, value: str) -> None:
        self.ID = value

    @property
    def name(self) -> str:
        return self.Name

    @name.setter
    def name(self, value: str) -> None:
        self.Name = value

    @property
    def data(self) -> typing.Any:
        return self.Data

    @data.setter
    def data(self, value: typing.Any) -> None:
        self.Data = value

    @property
    def clock(self) -> Clock | None:
        return self.Clock

    @clock.setter
    def clock(self, value: Clock | None) -> None:
        self.Clock = value

    @property
    def queue(self) -> "Queue | None":
        return self.Queue

    @queue.setter
    def queue(self, value: "Queue | None") -> None:
        self.Queue = value


config = Config


@dataclass
class Model(StateElement):
    events: dict[str, "Event[typing.Any]"] = field(default_factory=dict)
    attributes: dict[str, AttributeElement] = field(default_factory=dict)
    operations: dict[str, OperationElement] = field(default_factory=dict)
    transition_map: dict[str, dict[str, list[typing.Any]]] = field(default_factory=dict)
    snapshot_event_map: dict[str, tuple["EventSnapshot", ...]] = field(
        default_factory=dict
    )
    deferred_map: dict[str, dict[str, str]] = field(default_factory=dict)
    direct_deferred_map: dict[str, builtins.set[str]] = field(default_factory=dict)
    submachine_owner_map: dict[str, str] = field(default_factory=dict)
    history_paths: dict[tuple[str, str], tuple[str, ...]] = field(default_factory=dict)

    def add(self, partial: PartialElement) -> None:
        self.owned_elements.append(partial)

    @typing.overload
    def get(self, name: str) -> typing.Union[Element, "Event[typing.Any]", None]: ...

    @typing.overload
    def get(
        self, name: str, kind: typing.Type["Event[TData]"]
    ) -> "Event[TData] | None": ...

    @typing.overload
    def get(
        self,
        name: str,
        *kinds: typing.Type[TElement],
    ) -> TElement | None: ...

    def get(
        self,
        name: str,
        kind: typing.Type[typing.Any] | None = None,
        *kinds: typing.Type[typing.Any],
    ) -> typing.Any:
        all_kinds = () if kind is None else (kind, *kinds)
        if len(all_kinds) == 1:
            requested_kind = typing.cast(
                int, getattr(all_kinds[0], "kind", all_kinds[0])
            )
            if is_kind(requested_kind, Kinds.Event):
                event = self.events.get(name)
                return (
                    event
                    if event is not None and is_kind(event.kind, requested_kind)
                    else None
                )
            if is_kind(requested_kind, Kinds.Attribute):
                attribute = self.attributes.get(name)
                return (
                    attribute
                    if attribute is not None and is_kind(attribute.kind, requested_kind)
                    else None
                )
            if is_kind(requested_kind, Kinds.Operation):
                operation = self.operations.get(name)
                return (
                    operation
                    if operation is not None and is_kind(operation.kind, requested_kind)
                    else None
                )
        element = self.members.get(name)
        if element is None:
            return None
        if not all_kinds:
            return element
        requested_kinds = tuple(
            typing.cast(int, getattr(kind_value, "kind", kind_value))
            for kind_value in all_kinds
        )
        if not any(is_kind(element.kind, kind_value) for kind_value in requested_kinds):
            return None
        return element

    def set(self, qualified_name: str, element: Element) -> None:
        self.members[qualified_name] = element


@dataclass(init=False)
class Event(typing.Generic[TData]):
    name: str = field(default_factory=str)
    data: TData = field(default=typing.cast(TData, None))
    kind: int = Kinds.Event
    id: str = field(default_factory=str)
    source: str = field(default_factory=str)
    target: str = field(default_factory=str)
    qualified_name: str = field(default_factory=str)
    schema: typing.Any = None

    @typing.overload
    def __init__(
        self,
        name: str = "",
        data: None = None,
        kind: int = Kinds.Event,
        id: str = "",
        source: str = "",
        target: str = "",
        qualified_name: str = "",
        schema: typing.Any = None,
    ) -> None: ...

    @typing.overload
    def __init__(
        self,
        name: str,
        data: TData,
        kind: int = Kinds.Event,
        id: str = "",
        source: str = "",
        target: str = "",
        qualified_name: str = "",
        schema: typing.Any = None,
    ) -> None: ...

    @typing.overload
    def __init__(
        self,
        *,
        data: TData,
        name: str = "",
        kind: int = Kinds.Event,
        id: str = "",
        source: str = "",
        target: str = "",
        qualified_name: str = "",
        schema: typing.Any = None,
    ) -> None: ...

    def __init__(
        self,
        name: str = "",
        data: typing.Any = None,
        kind: int = Kinds.Event,
        id: str = "",
        source: str = "",
        target: str = "",
        qualified_name: str = "",
        schema: typing.Any = None,
    ) -> None:
        self.name = name
        self.data = data
        self.kind = kind
        self.id = id
        self.source = source
        self.target = target
        self.qualified_name = qualified_name or name
        self.schema = schema

    def __post_init__(self) -> None:
        if not self.qualified_name:
            self.qualified_name = self.name

    def WithData(self, data: TNewData) -> "Event[TNewData]":
        return Event(
            name=self.name,
            data=data,
            kind=self.kind,
            id=self.id,
            source=self.source,
            target=self.target,
            qualified_name=self.qualified_name,
            schema=self.schema,
        )

    def WithDataAndID(self, data: TNewData, id: str) -> "Event[TNewData]":
        return Event(
            name=self.name,
            data=data,
            kind=self.kind,
            id=id,
            source=self.source,
            target=self.target,
            qualified_name=self.qualified_name,
            schema=self.schema,
        )

    with_data = WithData
    with_data_and_id = WithDataAndID

    @property
    def Name(self) -> str:
        return self.name

    @property
    def Data(self) -> TData:
        return self.data

    @property
    def ID(self) -> str:
        return self.id

    @property
    def Source(self) -> str:
        return self.source

    @property
    def Target(self) -> str:
        return self.target

    @property
    def Kind(self) -> int:
        return self.kind

    @property
    def QualifiedName(self) -> str:
        return self.qualified_name

    @property
    def Schema(self) -> typing.Any:
        return self.schema


def _register_event(model: Model, event: "Event[typing.Any]") -> None:
    key = event.qualified_name or event.name
    if not key:
        return
    existing = model.events.get(key)
    if existing is not None:
        existing_kind = existing.kind or Kinds.Event
        new_kind = event.kind or Kinds.Event
        if existing_kind != new_kind:
            raise ValidationError(
                f'event "{key}" already defined with a different kind'
            )
        return
    model.events[key] = event


class CompletionEvent(Event[typing.Any]):
    def __init__(self, name: str = "", data: typing.Any = None):
        super().__init__(name=name, data=data, kind=Kinds.CompletionEvent)


@dataclass
class CallData:
    name: str
    args: tuple[typing.Any, ...]


@dataclass
class AttributeChange:
    name: str
    value: typing.Any
    old_value: typing.Any


def _readonly_snapshot_value(value: typing.Any) -> typing.Any:
    # Built-in containers are frozen recursively; arbitrary leaf objects are
    # deep-copied but may still expose their own mutating APIs.
    if isinstance(value, collections.abc.Mapping):
        return MappingProxyType(
            {
                copy.deepcopy(key): _readonly_snapshot_value(item)
                for key, item in value.items()
            }
        )
    if isinstance(value, tuple):
        return tuple(_readonly_snapshot_value(item) for item in value)
    if isinstance(value, list):
        return tuple(_readonly_snapshot_value(item) for item in value)
    if isinstance(value, collections.abc.Set):
        return frozenset(_readonly_snapshot_value(item) for item in value)
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


@dataclass(frozen=True)
class EventSnapshot:
    Name: str
    Kind: int
    Target: str | None
    GuardElement: bool
    Schema: typing.Any

    def __post_init__(self) -> None:
        object.__setattr__(self, "Schema", _readonly_snapshot_value(self.Schema))

    @property
    def name(self) -> str:
        return self.Name

    @property
    def kind(self) -> int:
        return self.Kind

    @property
    def target(self) -> str | None:
        return self.Target

    @property
    def guard(self) -> bool:
        return self.GuardElement

    @property
    def schema(self) -> typing.Any:
        return self.Schema


@dataclass(frozen=True)
class Snapshot:
    ID: str = ""
    QualifiedName: str = ""
    StateElement: str = ""
    Attributes: typing.Mapping[str, typing.Any] | None = None
    QueueLen: int = 0
    Events: tuple[EventSnapshot, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        attributes = (
            None
            if self.Attributes is None
            else _readonly_snapshot_value(self.Attributes)
        )
        events = tuple(self.Events)
        object.__setattr__(self, "Attributes", attributes)
        object.__setattr__(self, "Events", events)

    @property
    def id(self) -> str:
        return self.ID

    @property
    def qualified_name(self) -> str:
        return self.QualifiedName

    @property
    def state(self) -> str:
        return self.StateElement

    @property
    def attributes(self) -> typing.Mapping[str, typing.Any] | None:
        return self.Attributes

    @property
    def queue_len(self) -> int:
        return self.QueueLen

    @property
    def events(self) -> tuple[EventSnapshot, ...]:
        return self.Events


InitialEvent = Event(name="hsm_initial", kind=Kinds.Event)
ErrorEvent = Event(name="hsm_error", kind=Kinds.ErrorEvent)
AnyEvent = Event(name="*", kind=Kinds.Event)
FinalEvent = Event(name="hsm_final", kind=Kinds.CompletionEvent)
event = Event
completion_event = CompletionEvent
call_data = CallData
attribute_change = AttributeChange
event_snapshot = EventSnapshot
snapshot = Snapshot
initial_event = InitialEvent
error_event = ErrorEvent
any_event = AnyEvent
final_event = FinalEvent
InfiniteDuration = timedelta.max
infinite_duration = InfiniteDuration


@dataclass
class PseudostateElement(VertexElement):
    kind: int = Kinds.Pseudostate


@dataclass
class InitialElement(PseudostateElement):
    kind: int = Kinds.Initial


@dataclass
class EntryPointElement(PseudostateElement):
    kind: int = Kinds.EntryPoint


@dataclass
class ChoiceElement(PseudostateElement):
    kind: int = Kinds.Choice


@dataclass
class ShallowHistoryElement(PseudostateElement):
    kind: int = Kinds.ShallowHistory


@dataclass
class DeepHistoryElement(PseudostateElement):
    kind: int = Kinds.DeepHistory


@dataclass
class ExitPointElement(PseudostateElement):
    kind: int = Kinds.ExitPoint


@dataclass
class FinalStateElement(StateElement):
    kind: int = Kinds.FinalState


@dataclass
class TransitionPathElement:
    target: str = field(default_factory=str)
    enter: list[str] = field(default_factory=list)
    exit: list[str] = field(default_factory=list)
    effect_failure_state_index: int = -1
    effect_failure_state: str = field(default_factory=str)


@dataclass
class TransitionElement(NamedElement):
    kind: int = Kinds.Transition
    source: str = field(default_factory=str)
    target: str = field(default_factory=str)
    when: str | None = None
    generated_when: str | None = None
    when_attribute: str | None = None
    history_target_owner: str | None = None
    guard: str | None = None
    effect: list[str] = field(default_factory=list)
    events: list[str] = field(default_factory=list)
    paths: dict[str, TransitionPathElement] = field(default_factory=dict)
    effect_failure_state: str = field(default_factory=str)
    effect_failure_state_index: int = -1


def transition_has_wildcard_event(transition: TransitionElement) -> bool:
    # Wildcard priority is fixed before runtime dispatch.
    return any(
        event_name == AnyEvent.qualified_name for event_name in transition.events
    )


@dataclass
class SortTransitions(PartialElement):
    vertex: VertexElement = field(default_factory=VertexElement)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        self.vertex.transitions.sort(
            key=lambda name: (
                (transition := model.get(name, TransitionElement)) is not None
                and not transition_has_wildcard_event(transition)
            )
        )


@dataclass
class PartialState(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> StateElement:
        _validate_slashless_name("state", self.qualified_name, self.traceback)
        namespace = find(stack, StateElement)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: state must be called within Define() or StateElement()"
            )
        state = StateElement(
            qualified_name=_resolve_vertex_path(model, stack, self.qualified_name)
        )
        model.set(state.qualified_name, state)
        apply(state, model, stack, self.owned_elements)
        model.add(SortTransitions(vertex=state, traceback=self.traceback))
        return state


@dataclass
class PartialSubmachineState(PartialElement):
    machine: Model | None = None

    def apply(self, model: Model, stack: list[NamedElement]) -> SubmachineStateElement:
        _validate_slashless_name(
            "submachine state", self.qualified_name, self.traceback
        )
        namespace = find(stack, StateElement)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: SubmachineStateElement must be called within Define() or StateElement()"
            )
        if self.machine is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: SubmachineStateElement requires a model"
            )
        state = SubmachineStateElement(
            qualified_name=_resolve_vertex_path(model, stack, self.qualified_name),
            machine=self.machine,
        )
        model.set(state.qualified_name, state)
        compose_stack = [*stack, state]
        apply(state, model, compose_stack, self.machine.owned_elements)
        apply(state, model, stack, self.owned_elements)
        if any(
            isinstance(
                partial,
                (
                    PartialState,
                    PartialInitial,
                    PartialFinal,
                    PartialChoice,
                    PartialHistory,
                ),
            )
            for partial in self.owned_elements
        ):
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: SubmachineStateElement cannot contain nested states, initial, final, or pseudostates"
            )
        model.add(SortTransitions(vertex=state, traceback=self.traceback))
        return state


@dataclass
class PartialInitial(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> InitialElement:
        state = find(stack, StateElement)
        if state is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: initial must be called within a StateElement"
            )
        initial = InitialElement(
            qualified_name=_resolve_vertex_path(model, stack, self.qualified_name)
        )
        model.set(initial.qualified_name, initial)
        if state.initial:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: StateElement {state.qualified_name} already has an initial state {state.initial}"
            )
        state.initial = initial.qualified_name
        initial_transition = TransitionElement(
            source=initial.qualified_name,
            qualified_name=join(initial.qualified_name, "initial"),
        )
        model.set(initial_transition.qualified_name, initial_transition)
        scoped_stack = [*stack, initial, initial_transition]
        initial_transition.events.append(InitialEvent.qualified_name)
        _register_event(model, InitialEvent)
        for partial in self.owned_elements:
            if isinstance(partial, PartialElement):
                partial.apply(model, scoped_stack)
        if initial_transition.guard is not None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: InitialElement transition {initial_transition.qualified_name} cannot have a guard"
            )
        if not initial_transition.target:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: initial state is required for state machine"
            )
        if not is_ancestor(
            state.qualified_name, initial_transition.target
        ) and state.qualified_name != _parent_path(initial_transition.target):
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: InitialElement transition {initial_transition.qualified_name} must target a nested state not {initial_transition.target}"
            )
        initial.transitions.append(initial_transition.qualified_name)
        model.add(ResolvePaths(transition=initial_transition, traceback=self.traceback))
        return initial


@dataclass
class PartialHistory(PartialElement):
    history_type: typing.Type[PseudostateElement] = ShallowHistoryElement

    def apply(self, model: Model, stack: list[NamedElement]) -> PseudostateElement:
        history_name = self.history_type.__name__.replace("VertexElement", "")
        _validate_slashless_name(history_name, self.qualified_name, self.traceback)
        owner_state = find(stack, StateElement)
        if owner_state is None or owner_state is model:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: you must call {history_name}() within a nested StateElement"
            )
        if not self.owned_elements:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: {history_name} requires a default transition"
            )
        history = self.history_type(
            qualified_name=_resolve_vertex_path(model, stack, self.qualified_name)
        )
        model.set(history.qualified_name, history)
        if self.owned_elements:
            default_transition = TransitionElement(
                source=history.qualified_name,
                qualified_name=join(history.qualified_name, "default"),
            )
            model.set(default_transition.qualified_name, default_transition)
            apply(default_transition, model, [*stack, history], self.owned_elements)
            history.transitions.append(default_transition.qualified_name)
            model.add(
                ResolvePaths(transition=default_transition, traceback=self.traceback)
            )
        return history


@dataclass
class ResolvePaths(PartialElement):
    transition: TransitionElement = field(default_factory=TransitionElement)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        if self.transition.kind == Kinds.Internal:
            for name in model.members:
                if name.startswith(self.transition.source):
                    self.transition.paths[name] = TransitionPathElement(
                        target=self.transition.target,
                        enter=[],
                        exit=[],
                    )
            return
        enter: list[str] = []
        entering = self.transition.target
        lca = (
            _parent_path(self.transition.source)
            if self.transition.kind == Kinds.Self
            else LCA(self.transition.source, self.transition.target)
        )
        while entering not in ("", "/", lca):
            enter.insert(0, entering)
            entering = _parent_path(entering)
        source_element = model.get(self.transition.source, VertexElement)
        if isinstance(source_element, InitialElement):
            self.transition.paths[_parent_path(self.transition.source)] = (
                TransitionPathElement(
                    target=self.transition.target,
                    enter=enter,
                    exit=[],
                )
            )
            return
        if self.transition.source == "/" and self.transition.target:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Top level transitions must have a source and target, or no source and target"
            )
        if self.transition.kind == Kinds.Internal and not self.transition.effect:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Internal transitions require an effect"
            )
        for qualified_name, element in model.members.items():
            if not isinstance(element, (StateMachineElement, VertexElement)):
                continue
            if not qualified_name.startswith(self.transition.source):
                continue
            exit_path: list[str] = []
            if self.transition.kind != Kinds.Internal:
                exiting = qualified_name
                while exiting not in ("", lca):
                    exit_path.append(exiting)
                    exiting = _parent_path(exiting)
            self.transition.paths[qualified_name] = TransitionPathElement(
                target=self.transition.target,
                enter=enter,
                exit=exit_path,
            )


def LCA(a: str, b: str) -> str:
    if a == b:
        return _parent_path(a)
    if not a:
        return b
    if not b:
        return a
    if _parent_path(a) == _parent_path(b):
        return _parent_path(a)
    if IsAncestor(a, b):
        return a
    if IsAncestor(b, a):
        return b
    return LCA(_parent_path(a), _parent_path(b))


def least_common_ancestor(source: str, target: str) -> str:
    return LCA(source, target)


def IsAncestor(current: str, target: str) -> bool:
    current_norm = posixpath.normpath(current)
    target_norm = posixpath.normpath(target)
    if current_norm in ("", ".", target_norm):
        return False
    if current_norm == "/":
        return True
    parent = _parent_path(target_norm)
    while parent not in ("", ".", "/"):
        if parent == current_norm:
            return True
        parent = _parent_path(parent)
    return parent == current_norm


def is_ancestor(source: str, target: str) -> bool:
    return IsAncestor(source, target)


def is_path_in_path(child: str, parent: str) -> bool:
    parent_abs = posixpath.abspath(parent)
    child_abs = posixpath.abspath(child)
    return posixpath.commonpath([parent_abs]) == posixpath.commonpath(
        [parent_abs, child_abs]
    )


@dataclass
class ValidateVertex(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        if model.get(self.qualified_name, VertexElement) is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: VertexElement "{self.qualified_name}" not found'
            )


@dataclass
class PartialTransition(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionElement:
        vertex = find(stack, VertexElement)
        if vertex is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: transition must be called within a StateElement() or Define()"
            )
        name = self.qualified_name or f"transition_{len(model.members)}"
        transition = TransitionElement(
            qualified_name=_resolve_vertex_path(model, stack, name),
            source=".",
        )
        model.set(transition.qualified_name, transition)
        apply(transition, model, stack, self.owned_elements)
        if transition.source in ("", "."):
            transition.source = vertex.qualified_name
        source_element = model.get(transition.source, VertexElement)
        if source_element is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Source "{transition.source}" not found for transition "{transition.qualified_name}"'
            )
        source_element.transitions.append(transition.qualified_name)
        if (
            not transition.events
            and transition.when is None
            and not isinstance(source_element, PseudostateElement)
        ):
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: TransitionElement "{transition.qualified_name}" has no events'
            )
        classification_target = transition.target
        entry_point_target = _split_entry_point_target(model, transition.target)
        if entry_point_target is not None:
            classification_target = entry_point_target[0]
        if classification_target == transition.source:
            transition.kind = Kinds.Self
        elif not classification_target:
            transition.kind = Kinds.Internal
        elif IsAncestor(transition.source, classification_target):
            transition.kind = Kinds.Local
        else:
            transition.kind = Kinds.External
        model.add(ResolvePaths(transition=transition, traceback=self.traceback))
        return transition


@dataclass
class PartialSource(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionElement:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: hsm.Source() must be called within a hsm.TransitionElement()"
            )
        if transition.source not in ("", "."):
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: TransitionElement "{transition.qualified_name}" already has a source "{transition.source}"'
            )
        if self.owned_elements:
            source = self.owned_elements[0]
            if isinstance(source, PartialElement):
                resolved = source.apply(model, stack)
                if resolved is None:
                    raise ValidationError(
                        f'{self.traceback[0]}:{self.traceback[1]}: missing source "{self.qualified_name}"'
                    )
                source = resolved
            resolved_name = source.qualified_name
        else:
            resolved_name = _resolve_vertex_path(model, stack, self.qualified_name)
            model.add(
                ValidateVertex(qualified_name=resolved_name, traceback=self.traceback)
            )
        transition.source = resolved_name
        return transition


@dataclass
class PartialTarget(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionElement:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Target() must be called within TransitionElement()"
            )
        if transition.target:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: TransitionElement "{transition.qualified_name}" already has a target "{transition.target}"'
            )
        if self.owned_elements:
            target = self.owned_elements[0]
            if isinstance(target, PartialElement):
                resolved = target.apply(model, stack)
                if resolved is None:
                    raise ValidationError(
                        f'{self.traceback[0]}:{self.traceback[1]}: missing target "{self.qualified_name}"'
                    )
                target = resolved
            resolved_name = target.qualified_name
        else:
            resolved_name = _resolve_vertex_path(model, stack, self.qualified_name)
            if (
                _split_entry_point_target(model, resolved_name) is None
                and model.get(resolved_name, ExitPointElement) is None
            ):
                model.add(
                    ValidateVertex(
                        qualified_name=resolved_name, traceback=self.traceback
                    )
                )
        transition.target = resolved_name
        return transition


@dataclass
class PartialBehaviors(typing.Generic[TInstance], PartialElement):
    operations: list[BehaviorArgument[TInstance]] = field(default_factory=list)
    type: typing.Type[NamedElement] = field(default=NamedElement)
    concurrent: bool = False

    def apply(self, model: Model, stack: list[NamedElement]) -> NamedElement:
        element = find(stack, self.type)
        if element is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: {self.qualified_name} must be called within a {self.type.__name__}"
            )
        behaviors = getattr(element, self.qualified_name, None)
        if behaviors is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: {element.qualified_name} has no {self.qualified_name}"
            )
        for operation in self.operations:
            if isinstance(operation, str):
                callback = noop_operation
                behavior_operation_name = operation
                label = operation
            else:
                callback = operation
                behavior_operation_name = ""
                label = getattr(operation, "__name__", "anonymous")
                if not self.concurrent:
                    _validate_synchronous_callback(
                        self.qualified_name, callback, self.traceback
                    )
            behavior = BehaviorElement(
                qualified_name=join(
                    element.qualified_name,
                    self.qualified_name,
                    label,
                    str(len(behaviors)),
                ),
                operation=callback,
                operation_name=behavior_operation_name,
                kind=Kinds.Concurrent if self.concurrent else Kinds.Sequential,
                scope=element.qualified_name,
                defer_events=self.concurrent,
            )
            behaviors.append(behavior.qualified_name)
            model.set(behavior.qualified_name, behavior)
        return element


def noop_expression(ctx: Context, instance: "Instance", event: Event) -> bool:
    return True


@dataclass
class GuardElement(typing.Generic[TInstance], NamedElement):
    kind: int = Kinds.Constraint
    expression: Expression[TInstance] = field(default=noop_expression)
    operation_name: str = ""
    scope: str = field(default_factory=str)


@dataclass
class PartialGuard(typing.Generic[TInstance], PartialElement):
    expression: ExpressionArgument[TInstance] = field(default=noop_expression)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: guard must be called within a TransitionElement"
            )
        expression = self.expression
        source = find(stack, VertexElement)
        operation_name = ""
        if isinstance(expression, str):
            operation_name = expression
            expression = noop_expression
        else:
            _validate_synchronous_callback("guard", expression, self.traceback)
        guard = GuardElement(
            qualified_name=join(transition.qualified_name, self.qualified_name),
            expression=expression,
            operation_name=operation_name,
            scope="" if source is None else source.qualified_name,
        )
        model.set(guard.qualified_name, guard)
        transition.guard = guard.qualified_name


@dataclass
class PartialTrigger(PartialElement):
    events: list[Event] = field(default_factory=list)

    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionElement:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: trigger must be called within a TransitionElement"
            )
        for event in self.events:
            _register_event(model, event)
            transition.events.append(event.qualified_name)
        return transition


@dataclass
class PartialDefer(PartialElement):
    events: list[Event] = field(default_factory=list)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        state = find(stack, StateElement)
        if state is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: defer must be called within a state"
            )
        for event in self.events:
            _register_event(model, event)
            state.deferred.append(event.qualified_name)


@dataclass
class PartialChoice(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> ChoiceElement:
        state_or_transition = find(stack, StateElement, TransitionElement)
        if state_or_transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: choice must be called within a state or transition"
            )
        if isinstance(state_or_transition, TransitionElement):
            source_name = state_or_transition.source
            if source_name in ("", "."):
                source_vertex = find(stack, VertexElement)
                if source_vertex is None:
                    raise ValidationError(
                        f"{self.traceback[0]}:{self.traceback[1]}: choice must be called within a state"
                    )
                source_name = source_vertex.qualified_name
            if isinstance(
                model.get(source_name, PseudostateElement), PseudostateElement
            ):
                state_or_transition = find(stack, StateElement)
                if state_or_transition is None:
                    raise ValidationError(
                        f"{self.traceback[0]}:{self.traceback[1]}: choice must be called within a state"
                    )
        choice = ChoiceElement(
            qualified_name=_resolve_vertex_path(
                model,
                [*stack, state_or_transition],
                self.qualified_name or f"choice_{len(model.members)}",
            )
        )
        model.set(choice.qualified_name, choice)
        apply(choice, model, stack, self.owned_elements)
        if not choice.transitions:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: choice "{choice.qualified_name}" has no transitions'
            )
        default_transition = model.get(choice.transitions[-1], TransitionElement)
        if default_transition is not None and default_transition.guard is not None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: the last transition of choice state "{choice.qualified_name}" cannot have a guard'
            )
        return choice


@dataclass
class ValidateFinalState(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        final_state = model.get(self.qualified_name, FinalStateElement)
        if final_state is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" not found'
            )
        if final_state.transitions:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" cannot have transitions'
            )
        if final_state.entry:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" cannot have an entry action'
            )
        if final_state.exit:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" cannot have an exit action'
            )
        if final_state.activity:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" cannot have an activity'
            )


@dataclass
class PartialFinal(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        _validate_slashless_name("final", self.qualified_name, self.traceback)
        namespace = find(stack, StateElement)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Final must be called within a namespace"
            )
        final_state = FinalStateElement(
            qualified_name=_resolve_vertex_path(model, stack, self.qualified_name)
        )
        model.set(final_state.qualified_name, final_state)
        model.add(ValidateFinalState(qualified_name=final_state.qualified_name))


@dataclass
class PartialEntryPoint(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> NamedElement:
        _validate_slashless_name("entry point", self.qualified_name, self.traceback)
        transition = find(stack, TransitionElement)
        if transition is not None and not self.owned_elements:
            if _split_entry_point_target(model, transition.target) is not None:
                raise ValidationError(
                    f'{self.traceback[0]}:{self.traceback[1]}: TransitionElement "{transition.qualified_name}" already has an entry point target "{transition.target}"'
                )
            boundary = transition.target or "."
            transition.target = join(boundary, self.qualified_name)
            return transition
        namespace = find(stack, Model, SubmachineStateElement)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: EntryPoint must be declared within Define() or used within TransitionElement()"
            )
        connector = join(_namespace(model, stack), self.qualified_name)
        if model.get(connector, VertexElement) is not None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: EntryPoint name "{self.qualified_name}" collides with an existing vertex at "{connector}"'
            )
        entry_transition = TransitionElement(
            source=connector,
            qualified_name=join(connector, "_entry"),
        )
        model.set(entry_transition.qualified_name, entry_transition)
        scoped_stack = [*stack, entry_transition]
        for partial in self.owned_elements:
            if isinstance(partial, PartialElement):
                partial.apply(model, scoped_stack)
        if entry_transition.guard is not None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: EntryPoint {self.qualified_name} cannot have a guard"
            )
        if not entry_transition.target:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: EntryPoint {self.qualified_name} requires a target"
            )
        ResolvePaths(transition=entry_transition, traceback=self.traceback).apply(
            model, []
        )
        entry_point = EntryPointElement(qualified_name=connector)
        entry_point.transitions.append(entry_transition.qualified_name)
        model.set(entry_point.qualified_name, entry_point)
        if isinstance(namespace, SubmachineStateElement):
            namespace.owned_elements.append(entry_point)
        return entry_point


@dataclass
class PartialExitPoint(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> NamedElement:
        _validate_slashless_name("exit point", self.qualified_name, self.traceback)
        transition = find(stack, TransitionElement)
        source_state = find(stack, StateElement)
        if transition is not None and not self.owned_elements:
            source_name = transition.source
            if source_name in ("", "."):
                source_name = (
                    "" if source_state is None else source_state.qualified_name
                )
            if not source_name:
                raise ValidationError(
                    f"{self.traceback[0]}:{self.traceback[1]}: ExitPoint outcome must be used within a state transition"
                )
            event = Event(
                name=_exit_point_event_name(source_name, self.qualified_name),
                qualified_name=_exit_point_event_name(source_name, self.qualified_name),
                kind=Kinds.CompletionEvent,
            )
            _register_event(model, event)
            transition.events.append(event.qualified_name)
            return transition
        namespace = find(stack, Model, SubmachineStateElement)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: ExitPoint must be declared within Define() or used within TransitionElement()"
            )
        exit_point = ExitPointElement(
            qualified_name=_resolve_path(model, stack, self.qualified_name)
        )
        model.set(exit_point.qualified_name, exit_point)
        exit_transition = TransitionElement(
            source=exit_point.qualified_name,
            qualified_name=join(exit_point.qualified_name, "exit"),
        )
        model.set(exit_transition.qualified_name, exit_transition)
        scoped_stack = [*stack, exit_point, exit_transition]
        for partial in self.owned_elements:
            if isinstance(partial, PartialElement):
                partial.apply(model, scoped_stack)
        exit_point.transitions.append(exit_transition.qualified_name)
        if isinstance(namespace, SubmachineStateElement):
            namespace.owned_elements.append(exit_point)
        return exit_point


@dataclass
class PartialAttribute(PartialElement):
    default: typing.Any = None
    value_type: type[typing.Any] | None = None
    dynamic: bool = False
    implicit: bool = False

    def apply(self, model: Model, stack: list[NamedElement]) -> AttributeElement:
        _validate_slashless_name("attribute", self.qualified_name, self.traceback)
        if self.qualified_name == "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: attribute name cannot be empty"
            )
        qualified_name = _resolve_path(model, stack, self.qualified_name)
        if self.implicit:
            parent_scoped = _resolve_model_path(
                model, posixpath.basename(self.qualified_name)
            )
            existing = model.attributes.get(parent_scoped)
            if existing is not None and not existing.implicit:
                return existing
        if qualified_name in model.attributes and not self.implicit:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: duplicate attribute {self.qualified_name}"
            )
        existing = model.attributes.get(qualified_name)
        if existing is not None:
            return existing
        attribute = AttributeElement(
            qualified_name=qualified_name,
            declared_name=self.qualified_name,
            default=self.default,
            value_type=self.value_type,
            dynamic=self.dynamic,
            implicit=self.implicit,
        )
        model.attributes[attribute.qualified_name] = attribute
        return attribute


@dataclass
class PartialOperationDeclaration(PartialElement):
    callback: OperationImplementation | None = None

    def apply(self, model: Model, stack: list[NamedElement]) -> OperationElement:
        _validate_slashless_name("operation", self.qualified_name, self.traceback)
        if self.qualified_name == "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: operation name cannot be empty"
            )
        qualified_name = _resolve_path(model, stack, self.qualified_name)
        if qualified_name in model.operations:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: duplicate operation {self.qualified_name}"
            )
        if not stack:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: operation must be called within Define() or StateElement()"
            )
        operation = OperationElement(
            qualified_name=qualified_name,
            declared_name=self.qualified_name,
            callback=self.callback,
        )
        model.operations[operation.qualified_name] = operation
        _register_event(
            model,
            Event(
                name=_oncall_event_name(self.qualified_name),
                qualified_name=_oncall_event_name(self.qualified_name),
                kind=Kinds.CallEvent,
                source=operation.qualified_name,
                schema=CallData,
            ),
        )
        return operation


def _oncall_event_name(name: str) -> str:
    return f"@call:{name}"


def _exit_point_event_name(submachine_state: str, name: str) -> str:
    return f"@exit:{submachine_state}:{name}"


@dataclass
class PartialOnSet(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionElement:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: OnSet() must be called within a TransitionElement"
            )
        if self.qualified_name == "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: OnSet() requires a non-empty attribute name"
            )
        attribute = PartialAttribute(
            qualified_name=self.qualified_name, implicit=True
        ).apply(model, stack)
        event = Event(
            name=attribute.declared_name,
            qualified_name=attribute.declared_name,
            kind=Kinds.ChangeEvent,
            source=attribute.declared_name,
            schema=AttributeChange,
        )
        _register_event(model, event)
        transition.events.append(event.qualified_name)
        return transition


@dataclass
class PartialOnCall(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionElement:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: OnCall() must be called within a TransitionElement"
            )
        if self.qualified_name == "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: OnCall() requires a non-empty operation name"
            )
        operation_name = _resolve_operation_qualified_name(model, self.qualified_name)
        event = Event(
            name=_oncall_event_name(self.qualified_name),
            qualified_name=_oncall_event_name(self.qualified_name),
            kind=Kinds.CallEvent,
            source=operation_name,
            schema=CallData,
        )
        _register_event(model, event)
        transition.events.append(event.qualified_name)
        return transition


async def noop_duration(ctx: Context, instance: "Instance", event: Event) -> timedelta:
    return timedelta(seconds=0)


async def noop_timepoint(ctx: Context, instance: "Instance", event: Event) -> datetime:
    return datetime.now()


@dataclass
class PartialTimeExpression(PartialElement):
    transition: TransitionElement = field(default_factory=TransitionElement)
    timer_event: Event[typing.Any] = field(default_factory=Event)
    operation: OperationCallback[typing.Any] = field(default=noop_operation)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        source = model.get(self.transition.source, StateElement)
        if source is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Source "{self.transition.source}" not found for transition "{self.transition.qualified_name}"'
            )
        self.timer_event.source = source.qualified_name
        behavior = BehaviorElement(
            qualified_name=join(
                source.qualified_name, self.timer_event.name, str(len(model.members))
            ),
            kind=Kinds.Concurrent,
            operation=self.operation,
            scope=source.qualified_name,
            defer_events=True,
        )
        source.activity.append(behavior.qualified_name)
        model.set(behavior.qualified_name, behavior)


@dataclass
class PartialAfter(typing.Generic[TInstance], PartialElement):
    duration: Duration[TInstance] = field(default=noop_duration)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: after must be called within a TransitionElement"
            )
        trigger_name = getattr(self.duration, "__name__", "duration")
        qualified_name = join(
            transition.qualified_name, trigger_name, str(len(model.members))
        )
        timer_event = Event(
            name=qualified_name,
            qualified_name=qualified_name,
            kind=Kinds.TimeEvent,
            source=transition.source,
        )
        _register_event(model, timer_event)
        transition.events.append(timer_event.qualified_name)
        duration = self.duration

        async def run_after(
            ctx: Context, instance: Instance, event: Event[typing.Any]
        ) -> None:
            value = await _maybe_await(duration(ctx, instance, event))
            if not isinstance(value, timedelta):
                raise TypeError("After()/Every() duration must return timedelta")
            if value.total_seconds() <= 0:
                instance.dispatch(timer_event)
                return
            try:
                await instance.clock().Sleep(value)
            except asyncio.CancelledError:
                if _task_is_cancelling() or ctx.is_done():
                    return
                raise
            if ctx.is_done():
                return
            instance.dispatch(timer_event)

        run_after.__name__ = "run_after"
        model.add(
            PartialTimeExpression(
                traceback=self.traceback,
                transition=transition,
                timer_event=timer_event,
                operation=run_after,
            )
        )


@dataclass
class PartialAt(typing.Generic[TInstance], PartialElement):
    timepoint: Timepoint[TInstance] = field(default=noop_timepoint)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: at must be called within a TransitionElement"
            )
        trigger_name = getattr(self.timepoint, "__name__", "duration")
        qualified_name = join(
            transition.qualified_name, trigger_name, str(len(model.members))
        )
        timer_event = Event(
            name=qualified_name,
            qualified_name=qualified_name,
            kind=Kinds.TimeEvent,
            source=transition.source,
        )
        _register_event(model, timer_event)
        transition.events.append(timer_event.qualified_name)
        timepoint = self.timepoint

        async def run_at(
            ctx: Context, instance: Instance, event: Event[typing.Any]
        ) -> None:
            target = await _maybe_await(timepoint(ctx, instance, event))
            if not isinstance(target, datetime):
                raise TypeError("At() timepoint must return datetime")
            now = (
                datetime.now(target.tzinfo)
                if target.tzinfo is not None
                else datetime.now()
            )
            delta = target - now
            if delta.total_seconds() <= 0:
                instance.dispatch(timer_event)
                return
            try:
                await instance.clock().Sleep(delta)
            except asyncio.CancelledError:
                if _task_is_cancelling() or ctx.is_done():
                    return
                raise
            if ctx.is_done():
                return
            instance.dispatch(timer_event)

        run_at.__name__ = "run_at"
        model.add(
            PartialTimeExpression(
                traceback=self.traceback,
                transition=transition,
                timer_event=timer_event,
                operation=run_at,
            )
        )


@dataclass
class PartialEvery(typing.Generic[TInstance], PartialElement):
    duration: Duration[TInstance] = field(default=noop_duration)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: every must be called within a TransitionElement"
            )
        trigger_name = getattr(self.duration, "__name__", "duration")
        qualified_name = join(
            transition.qualified_name, trigger_name, str(len(model.members))
        )
        timer_event = Event(
            name=qualified_name,
            qualified_name=qualified_name,
            kind=Kinds.TimeEvent,
            source=transition.source,
        )
        _register_event(model, timer_event)
        transition.events.append(timer_event.qualified_name)
        duration = self.duration

        async def run_every(
            ctx: Context, instance: Instance, event: Event[typing.Any]
        ) -> None:
            while not ctx.is_done():
                value = await _maybe_await(duration(ctx, instance, event))
                if not isinstance(value, timedelta):
                    raise TypeError("After()/Every() duration must return timedelta")
                if value.total_seconds() < 0:
                    return
                if value.total_seconds() == 0:
                    instance.dispatch(timer_event)
                    continue
                try:
                    await instance.clock().Sleep(value)
                except asyncio.CancelledError:
                    if _task_is_cancelling() or ctx.is_done():
                        return
                    raise
                if ctx.is_done():
                    return
                instance.dispatch(timer_event)

        run_every.__name__ = "run_every"
        model.add(
            PartialTimeExpression(
                traceback=self.traceback,
                transition=transition,
                timer_event=timer_event,
                operation=run_every,
            )
        )


@dataclass
class PartialWhen(PartialOnSet, typing.Generic[TInstance]):
    expression: WhenExpression[TInstance] | None = None
    attribute: str | None = None

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: when must be called within a TransitionElement"
            )
        source = None
        if transition.source not in ("", "."):
            source = model.get(transition.source, StateElement)
        if source is None and transition.source in ("", "."):
            source = find(stack, StateElement)
        if source is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: when can only be used on transitions where the source is a StateElement"
            )
        if self.attribute is not None:
            attribute = PartialAttribute(
                qualified_name=self.attribute, implicit=True
            ).apply(model, stack)
            event = Event(
                name=attribute.declared_name,
                qualified_name=attribute.declared_name,
                kind=Kinds.ChangeEvent,
                source=attribute.declared_name,
                schema=AttributeChange,
            )
            _register_event(model, event)
            transition.events.append(event.qualified_name)

            def expression(ctx: Context, instance: TInstance, event: Event) -> bool:
                change = event.data
                return isinstance(change, AttributeChange) and bool(change.value)

            qualified_name = join(
                transition.qualified_name,
                f"when_{self.attribute}",
                str(len(model.members)),
            )
            guard = GuardElement(
                qualified_name=qualified_name,
                expression=expression,
                scope=source.qualified_name,
            )
            model.set(guard.qualified_name, guard)
            transition.when = guard.qualified_name
            transition.when_attribute = attribute.declared_name
            return

        if self.expression is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: when requires an expression or attribute"
            )
        _validate_synchronous_callback("when", self.expression, self.traceback)
        qualified_name = join(
            transition.qualified_name,
            getattr(self.expression, "__name__", "when"),
            str(len(model.members)),
        )
        guard = GuardElement(
            qualified_name=qualified_name,
            expression=self.expression,
            scope=source.qualified_name,
        )
        model.set(guard.qualified_name, guard)
        transition.when = guard.qualified_name


class _OperationAPI:
    def __getitem__(self, item: typing.Any) -> typing.Any:
        return OperationCallback[item]  # type: ignore[index]

    def __call__(
        self,
        name: str,
        callback: OperationImplementation | None = None,
    ) -> PartialOperationDeclaration:
        return PartialOperationDeclaration(qualified_name=name, callback=callback)


Operation = _OperationAPI()
operation = Operation


class _AfterWaiters:
    def __init__(self) -> None:
        self.dispatch: list[tuple[str, asyncio.Future[None]]] = []
        self.process: list[tuple[str | None, asyncio.Future[None]]] = []
        self.entry: list[tuple[str, asyncio.Future[None]]] = []
        self.exit: list[tuple[str, asyncio.Future[None]]] = []
        self.executed: list[tuple[str, asyncio.Future[None]]] = []

    def _cancel_all(self) -> None:
        for waiters in (
            self.dispatch,
            self.process,
            self.entry,
            self.exit,
            self.executed,
        ):
            for _, future in waiters:
                if not future.done():
                    future.cancel()
            waiters.clear()

    def _notify(
        self,
        waiters: list[tuple[typing.Any, asyncio.Future[None]]],
        predicate: typing.Callable[[typing.Any], bool],
    ) -> None:
        remaining: list[tuple[typing.Any, asyncio.Future[None]]] = []
        for expected, future in waiters:
            if future.done():
                continue
            if predicate(expected):
                future.set_result(None)
            else:
                remaining.append((expected, future))
        waiters[:] = remaining


def _default_attribute_values(model: Model) -> dict[str, typing.Any]:
    values: dict[str, typing.Any] = {}
    for name, attribute in model.attributes.items():
        values[name] = copy.deepcopy(attribute.default)
    return values


def _attribute_value_type(attribute: AttributeElement) -> type[typing.Any] | None:
    if attribute.dynamic:
        return None
    if attribute.value_type is not None:
        return attribute.value_type
    if attribute.default is not None:
        return type(attribute.default)
    return None


def _attribute_accepts_value(attribute: AttributeElement, value: typing.Any) -> bool:
    value_type = _attribute_value_type(attribute)
    if value_type is None:
        return True
    return type(value) is value_type


class Mutex:
    def __init__(self) -> None:
        self._locked = False
        self._waiters: collections.deque[asyncio.Future[None]] = collections.deque()
        self._signal: asyncio.Future[None] | None = None

    def _new_signal(self) -> None:
        self._signal = asyncio.get_running_loop().create_future()

    def wait(self) -> asyncio.Future[None]:
        if self._signal is None:
            self._signal = asyncio.get_running_loop().create_future()
            self._signal.set_result(None)
        return self._signal

    def try_acquire(self) -> bool:
        if self._locked:
            return False
        self._locked = True
        self._new_signal()
        return True

    def locked(self) -> bool:
        return self._locked

    async def acquire(self) -> None:
        if not self._locked:
            self._locked = True
            self._new_signal()
            return
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(future)
        try:
            await future
        except BaseException:
            if future.done() and not future.cancelled():
                self.release()
            else:
                future.cancel()
                try:
                    self._waiters.remove(future)
                except ValueError:
                    pass
            raise

    def release(self, error: BaseException | None = None) -> None:
        signal = self._signal
        if signal is not None and not signal.done():
            if error is None:
                signal.set_result(None)
            else:
                signal.set_exception(error)
        while self._waiters:
            future = self._waiters.popleft()
            if future.cancelled():
                continue
            self._new_signal()
            future.set_result(None)
            return
        self._locked = False


QueuePushResult = tuple[BaseException | None]
QueuePopResult = tuple[Event, bool, BaseException | None]
QueueLenResult = tuple[int, BaseException | None]


class Queue:
    """Regular-event FIFO backend for Queue(fifo=...)."""

    def __init__(self) -> None:
        self._items: collections.deque[Event] = collections.deque()

    def push(self, event: Event) -> QueuePushResult:
        self._items.append(event)
        return (None,)

    def pop(self) -> QueuePopResult:
        if not self._items:
            return (Event(), False, None)
        return (self._items.popleft(), True, None)

    def len(self) -> QueueLenResult:
        return (len(self._items), None)

    def clear(self) -> None:
        self._items.clear()


Fifo = Queue


class MultiQueue:
    def __init__(self, fifo: "Queue | None" = None) -> None:
        if fifo is not None:
            for hook in ("push", "pop", "len"):
                if not callable(getattr(fifo, hook, None)):
                    raise TypeError(f"Queue fifo backend requires callable {hook}")
        self._lock = threading.Lock()
        self._lifo: collections.deque[Event] = collections.deque()
        self._fifo: Queue = fifo or Queue()

    def push(self, event: Event[typing.Any]) -> QueuePushResult:
        if is_kind(event.kind, Kinds.CompletionEvent):
            with self._lock:
                self._lifo.appendleft(event)
            return (None,)
        try:
            (error,) = self._fifo.push(event)
        except BaseException as error:
            return (error,)
        return (error,)

    def pop(self) -> QueuePopResult:
        with self._lock:
            if self._lifo:
                return (self._lifo.popleft(), True, None)
        try:
            return self._fifo.pop()
        except BaseException as error:
            return (Event(), False, error)

    def len(self) -> QueueLenResult:
        with self._lock:
            completion_len = len(self._lifo)
        try:
            count, error = self._fifo.len()
        except BaseException as error:
            return (0, error)
        if error is not None:
            return (0, error)
        return (completion_len + count, None)

    def clear(self) -> None:
        with self._lock:
            self._lifo.clear()
            self._fifo.clear()


@dataclass
class ActiveBehavior:
    context: Context
    task: asyncio.Task[None]


@final
class _AbortTransition(Exception):
    def __init__(self, error: BaseException):
        super().__init__(str(error))
        self.error = error


def _clock_for_instance(instance: typing.Any) -> Clock:
    if isinstance(instance, Instance):
        return instance.clock()
    return DefaultClock.with_defaults()


class Instance:
    __hsm: typing.Optional["HSM[typing.Self]"] = None

    def dispatch(
        self, event: Event, source: "HSM[typing.Any] | Group | None" = None
    ) -> typing.Awaitable[None]:
        if self.__hsm is None:
            raise ValidationError("operation requires a started HSM")
        return self.__hsm.dispatch(event, source)

    def state(self) -> str:
        if self.__hsm is None:
            return ""
        return self.__hsm.state()

    def context(self) -> Context | None:
        if self.__hsm is None:
            return None
        return self.__hsm.context()

    def clock(self) -> Clock:
        if self.__hsm is None:
            return DefaultClock.with_defaults()
        return self.__hsm.clock()

    def get(self, name: str) -> tuple[typing.Any, bool]:
        if self.__hsm is None:
            return None, False
        return self.__hsm.get(name)

    def set(self, name: str, value: typing.Any) -> typing.Awaitable[None]:
        if self.__hsm is None:
            raise ValidationError("operation requires a started HSM")
        return self.__hsm.set(name, value)

    def call(self, name: str, *args: typing.Any) -> typing.Awaitable[typing.Any]:
        if self.__hsm is None:
            raise ValidationError("operation requires a started HSM")
        return self.__hsm.call(name, *args)

    def stop(self) -> typing.Awaitable[None]:
        if self.__hsm is None:
            return _completed_none()
        return self.__hsm.stop()

    def restart(self, data: typing.Any = None) -> typing.Awaitable[None]:
        if self.__hsm is None:
            raise ValidationError("operation requires a started HSM")
        return self.__hsm.restart(data)

    def take_snapshot(self) -> Snapshot:
        if self.__hsm is None:
            raise ValidationError("operation requires a started HSM")
        return self.__hsm.take_snapshot()

    Dispatch = dispatch
    StateElement = state
    Context = context
    Clock = clock
    Get = get
    Set = set
    Call = call
    Stop = stop
    Restart = restart
    TakeSnapshot = take_snapshot


class HSM(BehaviorElement[TInstance]):
    __hash__: typing.ClassVar[typing.Any] = object.__hash__

    def __init__(
        self,
        instance: TInstance,
        model: Model,
        ctx: Context | None = None,
        config: Config | None = None,
    ):
        config = config or Config()
        super().__init__(
            kind=Kinds.StateMachine,
            id=config.ID or _next_id(),
            qualified_name=config.Name or model.qualified_name,
        )
        self.model = model
        self._instance = instance
        self._processing = Mutex()
        self._queue = (
            MultiQueue(config.Queue) if config.Queue is not None else MultiQueue()
        )
        self._active: dict[str, ActiveBehavior] = {}
        self._after = _AfterWaiters()
        self._state: VertexElement = model
        self._awaitable: typing.Awaitable[None] = _future_done()
        self._attributes = _default_attribute_values(model)
        self._history_shallow: dict[str, str] = {}
        self._history_deep: dict[str, str] = {}
        self._context: Context = _WithRuntimeHSM(ctx or Context(), self)
        self._clock = (config.Clock or DefaultClock).with_defaults()
        if isinstance(
            instances := self._context.Value(Keys.Instances),
            collections.abc.MutableMapping,
        ):
            instances[self.id] = self
        setattr(self._instance, "_Instance__hsm", self)

        async def operation(ctx: Context, inst: TInstance, event: Event) -> None:
            self._state = await self._enter(self.model, event, True)
            await self._drain_queue([])

        self.operation = operation

    def state(self) -> str:
        return self._state.qualified_name

    def context(self) -> Context:
        return self._context

    def clock(self) -> Clock:
        return self._clock

    async def _start(self, data: typing.Any = None) -> None:
        await self._processing.acquire()
        try:
            if isinstance(
                instances := self._context.Value(Keys.Instances),
                collections.abc.MutableMapping,
            ):
                instances[self.id] = self
            initial_event = (
                InitialEvent.WithData(data) if data is not None else InitialEvent
            )
            await self._execute(self, initial_event)
        except BaseException as error:
            await self._cleanup_failed_start(
                reset_state=isinstance(error, asyncio.CancelledError)
            )
            raise
        finally:
            self._processing.release()

    async def _cleanup_failed_start(self, *, reset_state: bool = False) -> None:
        tasks: list[asyncio.Task[None]] = []
        for active in list(self._active.values()):
            active.context.cancel()
            if active.task is asyncio.current_task():
                continue
            active.task.cancel()
            tasks.append(active.task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._after._cancel_all()
        if isinstance(
            instances := self._context.Value(Keys.Instances),
            collections.abc.MutableMapping,
        ):
            for key, value in list(instances.items()):
                if value is self:
                    del instances[key]
        self._context.cancel()
        self._queue.clear()
        self._active.clear()
        self._attributes = _default_attribute_values(self.model)
        self._history_shallow.clear()
        self._history_deep.clear()
        if reset_state:
            self._state = self.model
        self._awaitable = _future_done()

    def _remember_history(self, leaf_name: str, skip_owner: str | None = None) -> None:
        current = leaf_name
        while current not in ("", "/", self.model.qualified_name):
            parent = _parent_path(current)
            if parent in ("", ".", "/") or parent == current:
                break
            if parent != skip_owner:
                self._history_shallow[parent] = current
            current = parent
        current = leaf_name
        while current not in ("", ".", "/"):
            parent = _parent_path(current)
            if parent in ("", ".", "/") or parent == current:
                break
            if parent != skip_owner:
                self._history_deep[parent] = leaf_name
            if parent == self.model.qualified_name:
                break
            current = parent

    def _queue_final_completion(self, vertex: VertexElement) -> None:
        if isinstance(vertex, FinalStateElement):
            self._queue_push(Event(name=FinalEvent.name, kind=Kinds.CompletionEvent))

    async def _enter(
        self,
        vertex: VertexElement,
        event: Event,
        default_entry: bool,
    ) -> VertexElement:
        if isinstance(vertex, (ShallowHistoryElement, DeepHistoryElement)):
            owner_name = vertex.owner()
            remembered = (
                self._history_shallow.get(owner_name)
                if isinstance(vertex, ShallowHistoryElement)
                else self._history_deep.get(owner_name)
            )
            if remembered:
                current: VertexElement = (
                    self.model.get(owner_name, VertexElement) or self.model
                )
                enter_path = self.model.history_paths.get((owner_name, remembered), ())
                for index, qualified_name in enumerate(enter_path):
                    vertex_to_enter = self.model.get(qualified_name, VertexElement)
                    if vertex_to_enter is None:
                        continue
                    default_entry = (
                        isinstance(vertex, ShallowHistoryElement)
                        and index == len(enter_path) - 1
                    )
                    current = await self._enter(vertex_to_enter, event, default_entry)
                return current
            for transition_name in vertex.transitions:
                transition = self.model.get(transition_name, TransitionElement)
                if transition is None:
                    continue
                guard = self.model.get(transition.guard or "", GuardElement)
                if guard is not None and not self._evaluate(guard, event):
                    continue
                return await self._transition(vertex, transition, event)
            owner_vertex = self.model.get(owner_name, VertexElement)
            return owner_vertex if owner_vertex is not None else self._state
        if isinstance(vertex, ChoiceElement):
            for transition_name in vertex.transitions:
                transition = self.model.get(transition_name, TransitionElement)
                if transition is None:
                    continue
                guard = self.model.get(transition.guard or "", GuardElement)
                if guard is not None and not self._evaluate(guard, event):
                    continue
                return await self._transition(vertex, transition, event)
            return self._state
        if isinstance(vertex, StateElement):
            previous_state = self._state
            if vertex.entry or vertex.activity:
                self._state = vertex
            for behavior_name in vertex.entry:
                behavior = self.model.get(behavior_name, BehaviorElement)
                if behavior is not None:
                    await self._execute(behavior, event)
            if self._after.entry:
                self._after._notify(
                    self._after.entry,
                    lambda expected: expected == vertex.qualified_name,
                )
            for behavior_name in vertex.activity:
                behavior = self.model.get(behavior_name, BehaviorElement)
                if behavior is not None:
                    await self._execute(behavior, event)
            if not default_entry or vertex.initial == "":
                self._queue_final_completion(vertex)
                return vertex
            initial = self.model.get(vertex.initial, VertexElement)
            if isinstance(initial, VertexElement) and initial.transitions:
                transition = self.model.get(initial.transitions[0], TransitionElement)
                if transition is not None:
                    self._state = previous_state
                    try:
                        return await self._transition(vertex, transition, event)
                    except BaseException:
                        if self._state is previous_state:
                            self._state = vertex
                        raise
        return vertex

    async def _exit(
        self,
        vertex: VertexElement,
        event: Event,
    ) -> VertexElement:
        if isinstance(vertex, StateElement):
            for behavior_name in vertex.activity:
                behavior = self.model.get(behavior_name, BehaviorElement)
                if behavior is not None:
                    await self._terminate(behavior)
            for behavior_name in vertex.exit:
                behavior = self.model.get(behavior_name, BehaviorElement)
                if behavior is not None:
                    await self._execute(behavior, event)
            if self._after.exit:
                self._after._notify(
                    self._after.exit, lambda expected: expected == vertex.qualified_name
                )
        return vertex

    def _evaluate(self, guard: GuardElement[TInstance], event: Event) -> bool:
        try:
            result = guard.expression(self._context, self._instance, event)
            return bool(result)
        except Exception as error:
            if is_kind(event.kind, Kinds.ErrorEvent):
                return False
            self._dispatch_error(error)
            raise _AbortTransition(error) from error

    def _run_sequential_behavior(
        self, behavior: BehaviorElement[TInstance], event: Event
    ) -> None:
        result = behavior.operation(self._context, self._instance, event)
        if inspect.isawaitable(result):
            _close_awaitable(result)
            raise RuntimeError("transition behavior returned awaitable")
        self._notify_executed(behavior)

    async def _execute(
        self, behavior: BehaviorElement[TInstance], event: Event
    ) -> None:
        try:
            if behavior.kind == Kinds.Concurrent:
                activity_ctx = Context(self._context)
                registered: list[ActiveBehavior | None] = [None]

                async def run_activity() -> None:
                    try:
                        await _maybe_await(
                            behavior.operation(activity_ctx, self._instance, event)
                        )
                    except asyncio.CancelledError as error:
                        was_done = activity_ctx.is_done()
                        activity_ctx.cancel()
                        if _task_is_cancelling() or was_done:
                            return
                        self._dispatch_error(error)
                    except Exception as error:
                        if activity_ctx.is_done():
                            return
                        activity_ctx.cancel()
                        self._dispatch_error(error)
                    finally:
                        self._notify_executed(behavior)
                        current = registered[0]
                        if (
                            current is not None
                            and self._active.get(behavior.qualified_name) is current
                        ):
                            self._active.pop(behavior.qualified_name, None)
                        if (
                            behavior.defer_events
                            and isinstance(
                                instances := self._context.Value(Keys.Instances),
                                collections.abc.Mapping,
                            )
                            and instances.get(self.id) is self
                            and (
                                self._state is not self.model
                                or self._processing.locked()
                            )
                            and not self._context.done
                            and self._queue_len() > 0
                            and self._processing.try_acquire()
                        ):
                            self._awaitable = asyncio.create_task(self._process())

                task = asyncio.create_task(run_activity(), name=behavior.qualified_name)
                registered[0] = ActiveBehavior(context=activity_ctx, task=task)
                self._active[behavior.qualified_name] = registered[0]
                return
            if behavior.kind == Kinds.StateMachine:
                await _maybe_await(
                    behavior.operation(self._context, self._instance, event)
                )
                return
            self._run_sequential_behavior(behavior, event)
        except Exception as error:
            if self._context.done or is_kind(event.kind, Kinds.ErrorEvent):
                return
            self._dispatch_error(error)
            raise _AbortTransition(error) from error

    def _dispatch_error(self, error: BaseException) -> None:
        if self._context.done:
            return
        try:
            self._dispatch_task(
                Event(name=ErrorEvent.name, data=error, kind=Kinds.ErrorEvent),
                observe_result=False,
            )
        except ValidationError:
            return

    def _notify_executed(self, behavior: BehaviorElement[TInstance]) -> None:
        if not self._after.executed:
            return
        names = {behavior.qualified_name}
        owner = behavior.owner()
        if owner:
            names.add(owner)
        current = owner
        while current not in ("", ".", "/"):
            element = self.model.get(current, StateElement)
            if element is not None:
                names.add(current)
                break
            current = _parent_path(current)
        self._after._notify(self._after.executed, lambda expected: expected in names)

    async def _terminate(self, behavior: BehaviorElement[TInstance]) -> None:
        active = self._active.pop(behavior.qualified_name, None)
        if active is None:
            return
        active.context.cancel()
        current_task = asyncio.current_task()
        if active.task is current_task:
            return
        active.task.cancel()
        try:
            await active.task
        except asyncio.CancelledError:
            pass

    def _enabled(self, source: StateElement, event: Event) -> TransitionElement | None:
        source_transitions = self.model.transition_map.get(source.qualified_name, {})
        ordered = [*source_transitions.get(event.qualified_name, [])]
        ordered.extend(source_transitions.get(AnyEvent.qualified_name, []))
        direct_deferred = self.model.direct_deferred_map.get(source.qualified_name)
        defers_event = direct_deferred is not None and (
            event.qualified_name in direct_deferred
        )
        for transition in ordered:
            if defers_event and transition.owner() != source.qualified_name:
                continue
            if transition.when is not None:
                maybe_when = self.model.get(transition.when, GuardElement)
                if maybe_when is not None and not self._evaluate(maybe_when, event):
                    continue
            if transition.guard is None:
                return transition
            maybe_guard = self.model.get(transition.guard, GuardElement)
            if maybe_guard is not None and self._evaluate(maybe_guard, event):
                return transition
        return None

    async def _process(
        self,
        first_event: Event | None = None,
        pending_error: BaseException | None = None,
    ) -> None:
        deferred: list[Event] = []
        error: BaseException | None = None
        try:
            await self._drain_queue(deferred, first_event, pending_error)
        except BaseException as exc:
            error = exc
        finally:
            for event in deferred:
                self._queue_push(event)
            self._processing.release(error)

    def _queue_push(self, event: Event) -> None:
        (error,) = self._queue.push(event)
        if error is not None and not is_kind(event.kind, Kinds.ErrorEvent):
            self._queue_push(
                Event(name=ErrorEvent.name, data=error, kind=Kinds.ErrorEvent)
            )

    def _queue_pop(self) -> Event | None:
        while True:
            event, ok, error = self._queue.pop()
            if error is not None:
                self._queue_push(
                    Event(name=ErrorEvent.name, data=error, kind=Kinds.ErrorEvent)
                )
                continue
            if not ok:
                return None
            return event

    def _queue_len(self) -> int:
        count, error = self._queue.len()
        if error is not None:
            self._queue_push(
                Event(name=ErrorEvent.name, data=error, kind=Kinds.ErrorEvent)
            )
            return 0
        return count

    def _deferred_boundary_active(
        self, event: Event, current_leaf: VertexElement | None = None
    ) -> bool:
        boundary = getattr(event, "_hsm_deferred_boundary", "")
        if not isinstance(boundary, str) or not boundary:
            return True
        state_name = (current_leaf or self._state).qualified_name
        return state_name == boundary or IsAncestor(boundary, state_name)

    async def _drain_queue(
        self,
        deferred: list[Event],
        first_event: Event | None = None,
        pending_error: BaseException | None = None,
    ) -> None:
        event = first_event if first_event is not None else self._queue_pop()
        pending_error_handled = False

        def add_deferred(event: Event, owner: str) -> None:
            owner = getattr(event, "_hsm_deferred_owner", owner)
            setattr(event, "_hsm_deferred_owner", owner)
            boundary = self.model.submachine_owner_map.get(owner)
            if boundary and boundary != owner:
                setattr(event, "_hsm_deferred_boundary", boundary)
            elif hasattr(event, "_hsm_deferred_boundary"):
                delattr(event, "_hsm_deferred_boundary")
            deferred.append(event)

        def release_deferred() -> None:
            if not deferred:
                return
            remaining: list[Event] = []
            for deferred_event in deferred:
                boundary = getattr(deferred_event, "_hsm_deferred_boundary", "")
                if (
                    isinstance(boundary, str)
                    and boundary
                    and self._deferred_boundary_active(deferred_event)
                ):
                    remaining.append(deferred_event)
                    continue
                self._queue_push(deferred_event)
            deferred[:] = remaining

        def requeue_deferred() -> None:
            if not deferred:
                return
            for deferred_event in deferred:
                self._queue_push(deferred_event)
            deferred.clear()

        while event is not None:
            event_qualified_name = event.qualified_name
            current_leaf = self._state
            was_deferred = hasattr(event, "_hsm_deferred_owner")
            if was_deferred and not self._deferred_boundary_active(event, current_leaf):
                deferred.append(event)
                event = self._queue_pop()
                continue
            if (
                event.kind == Kinds.TimeEvent
                and event.source
                and current_leaf.qualified_name != event.source
                and not IsAncestor(event.source, current_leaf.qualified_name)
            ):
                event = self._queue_pop()
                continue
            if was_deferred and event.qualified_name in self.model.deferred_map.get(
                current_leaf.qualified_name, {}
            ):
                owner = getattr(
                    event, "_hsm_deferred_owner", current_leaf.qualified_name
                )
                add_deferred(event, owner)
                if self._after.process:
                    self._after._notify(
                        self._after.process,
                        lambda expected: (
                            expected is None or expected == event_qualified_name
                        ),
                    )
                event = self._queue_pop()
                continue
            qualified_name = current_leaf.qualified_name
            event_handled = False
            event_aborted = False
            while qualified_name:
                source = self.model.get(qualified_name, StateElement)
                if source is None:
                    break
                try:
                    transition = self._enabled(source, event)
                    if transition is not None:
                        self._state = await self._transition(
                            current_leaf, transition, event
                        )
                        event_handled = True
                        if is_kind(event.kind, Kinds.ErrorEvent):
                            pending_error_handled = True
                        release_deferred()
                        break
                except _AbortTransition as abort:
                    event_aborted = True
                    if pending_error is None:
                        pending_error = abort.error
                    break
                owner = self.model.deferred_map.get(qualified_name, {}).get(
                    event.qualified_name
                )
                if owner is not None:
                    add_deferred(event, owner)
                    event_handled = True
                    break
                qualified_name = source.owner()
            if self._after.process:
                self._after._notify(
                    self._after.process,
                    lambda expected: (
                        expected is None or expected == event_qualified_name
                    ),
                )
            event = self._queue_pop()
        requeue_deferred()
        if pending_error is not None and not pending_error_handled:
            raise pending_error

    async def _transition(
        self, current_leaf: VertexElement, transition: TransitionElement, event: Event
    ) -> VertexElement:
        path = transition.paths.get(current_leaf.qualified_name)
        if path is None:
            return current_leaf
        if transition.kind != Kinds.Internal:
            self._remember_history(
                current_leaf.qualified_name, transition.history_target_owner
            )
        for exiting in path.exit:
            vertex = self.model.get(exiting, VertexElement)
            if vertex is not None:
                await self._exit(vertex, event)
        for index, effect_name in enumerate(transition.effect):
            effect = self.model.get(effect_name, BehaviorElement)
            if effect is None:
                continue
            try:
                await self._execute(effect, event)
            except BaseException:
                if (
                    path.effect_failure_state
                    and path.effect_failure_state_index >= 0
                    and index >= path.effect_failure_state_index
                ):
                    restored = self.model.get(path.effect_failure_state, VertexElement)
                    if restored is not None:
                        self._state = restored
                raise
        if transition.kind == Kinds.Internal:
            return current_leaf
        current: VertexElement = current_leaf
        for entering in path.enter:
            vertex = self.model.get(entering, VertexElement)
            if vertex is None:
                continue
            current = await self._enter(vertex, event, entering == path.target)
            if entering == path.target:
                return current
        target = self.model.get(path.target, VertexElement)
        return current if target is None else target

    def _dispatch_task(
        self, event: Event[typing.Any], observe_result: bool = True
    ) -> typing.Awaitable[None]:
        instances = self._context.Value(Keys.Instances)
        registered = (
            isinstance(instances, collections.abc.Mapping)
            and instances.get(self.id) is self
        )
        if not self._processing.locked() and not (
            registered and self._state is not self.model
        ):
            raise ValidationError("operation requires a started HSM")
        self._queue_push(event)
        if self._after.dispatch:
            self._after._notify(
                self._after.dispatch, lambda expected: expected == event.qualified_name
            )
        current_task = asyncio.current_task()
        active_behavior: BehaviorElement[typing.Any] | None = None
        if current_task is not None:
            for behavior_name, active in self._active.items():
                if active.task is current_task:
                    active_behavior = self.model.get(
                        behavior_name, BehaviorElement[typing.Any]
                    )
                    break
        if active_behavior is not None and (
            active_behavior.defer_events
            and event.kind == Kinds.ChangeEvent
            or (event.kind != Kinds.TimeEvent and _is_timer_activity(active_behavior))
        ):
            return _future_done()
        acquired = self._processing.try_acquire()
        if not acquired and asyncio.current_task() is self._awaitable:
            return _future_done()
        if (
            not acquired
            and isinstance(self._awaitable, asyncio.Future)
            and self._awaitable.done()
        ):
            return _future_done()
        if acquired:
            self._awaitable = asyncio.create_task(self._process())
        if not observe_result:
            return self._awaitable
        return self._processing.wait()

    def dispatch(
        self,
        event: Event[typing.Any],
        source: "HSM[typing.Any] | Group | None" = None,
    ) -> typing.Awaitable[None]:
        return self._dispatch_task(_clone_event_for_delivery(event, self, source))

    async def _stop_locked(self) -> None:
        final_event = Event(name=FinalEvent.name, kind=Kinds.CompletionEvent)
        self._context.cancel()
        while self._state.qualified_name != self.model.qualified_name:
            await self._exit(self._state, final_event)
            parent = self.model.get(
                _parent_path(self._state.qualified_name), VertexElement
            )
            if parent is None:
                break
            self._state = parent
        for active in list(self._active.values()):
            active.context.cancel()
            if active.task is asyncio.current_task():
                continue
            active.task.cancel()
            try:
                await active.task
            except asyncio.CancelledError:
                pass
        self._active.clear()
        self._after._cancel_all()
        if isinstance(
            instances := self._context.Value(Keys.Instances),
            collections.abc.MutableMapping,
        ):
            for key, value in list(instances.items()):
                if value is self:
                    del instances[key]
        self._queue.clear()
        self._attributes = _default_attribute_values(self.model)
        self._history_shallow.clear()
        self._history_deep.clear()
        self._state = self.model
        self._awaitable = _future_done()

    async def stop(self) -> None:
        await self._processing.acquire()
        try:
            await self._stop_locked()
        finally:
            self._processing.release()

    async def restart(self, data: typing.Any = None) -> None:
        instances = self._context.Value(Keys.Instances)
        registered = (
            isinstance(instances, collections.abc.Mapping)
            and instances.get(self.id) is self
        )
        if not (
            registered and (self._state is not self.model or self._processing.locked())
        ):
            raise ValidationError("operation requires a started HSM")
        await self._processing.acquire()
        try:
            await self._restart_locked(data)
        finally:
            self._processing.release()

    def _reset_for_restart(self) -> None:
        self._context = _WithRuntimeHSM(_runtime_context_parent(self._context), self)
        self._queue.clear()
        self._active.clear()
        self._attributes = _default_attribute_values(self.model)
        self._history_shallow.clear()
        self._history_deep.clear()
        self._state = self.model
        self._awaitable = _future_done()

    async def _restart_locked(self, data: typing.Any = None) -> None:
        await self._stop_locked()
        self._reset_for_restart()
        if isinstance(
            instances := self._context.Value(Keys.Instances),
            collections.abc.MutableMapping,
        ):
            instances[self.id] = self
        initial_event = (
            InitialEvent.WithData(data) if data is not None else InitialEvent
        )
        self._state = await self._enter(self.model, initial_event, True)
        await self._drain_queue([])

    def get(self, name: str) -> tuple[typing.Any, bool]:
        qualified_name = _resolve_model_path(self.model, name)
        if qualified_name in self._attributes:
            return copy.deepcopy(self._attributes[qualified_name]), True
        return None, False

    async def set(self, name: str, value: typing.Any) -> None:
        instances = self._context.Value(Keys.Instances)
        registered = (
            isinstance(instances, collections.abc.Mapping)
            and instances.get(self.id) is self
        )
        if not self._processing.locked() and not (
            registered and self._state is not self.model
        ):
            raise ValidationError("operation requires a started HSM")
        qualified_name = _resolve_model_path(self.model, name)
        attribute = self.model.attributes.get(qualified_name)
        if attribute is None:
            raise ValidationError(f'missing attribute "{name}"')
        if not _attribute_accepts_value(attribute, value):
            value_type = _attribute_value_type(attribute)
            expected = "unknown" if value_type is None else value_type.__name__
            actual = type(value).__name__
            raise ValidationError(
                f'attribute "{name}" requires value of type {expected}, got {actual}'
            )
        old_value = self._attributes.get(qualified_name)
        self._attributes[qualified_name] = value
        if old_value == value:
            return None
        event = Event(
            name=qualified_name,
            qualified_name=qualified_name,
            kind=Kinds.ChangeEvent,
            source=qualified_name,
            data=AttributeChange(name=qualified_name, value=value, old_value=old_value),
            schema=AttributeChange,
        )
        _register_event(self.model, event)
        await self.dispatch(event)
        return None

    async def call(self, name: str, *args: typing.Any) -> typing.Any:
        instances = self._context.Value(Keys.Instances)
        registered = (
            isinstance(instances, collections.abc.Mapping)
            and instances.get(self.id) is self
        )
        if not self._processing.locked() and not (
            registered and self._state is not self.model
        ):
            raise ValidationError("operation requires a started HSM")
        if not name:
            raise ValidationError("operation name cannot be empty")
        event_name = _oncall_event_name(name)
        call_event = self.model.events.get(event_name)
        operation = None
        if call_event is not None and call_event.kind == Kinds.CallEvent:
            operation = _resolve_operation(self.model, call_event.source, name)
        elif name in self.model.operations:
            operation = self.model.operations[name]
        if operation is None:
            raise ValidationError(f'missing operation "{name}"')
        callback = _operation_callback(operation, self._instance)
        event = Event(
            name=event_name,
            qualified_name=event_name,
            kind=Kinds.CallEvent,
            source=operation.qualified_name,
            data=CallData(name=operation.qualified_name, args=args),
            schema=CallData,
        )
        await self.dispatch(event)
        value = await _maybe_await(callback(self._context, self._instance, *args))
        return value

    def take_snapshot(self) -> Snapshot:
        current_name = self._state.qualified_name
        return Snapshot(
            ID=typing.cast(str, self.id),
            QualifiedName=self.qualified_name,
            StateElement=self._state.qualified_name,
            Attributes=copy.deepcopy(self._attributes),
            QueueLen=self._queue_len(),
            Events=self.model.snapshot_event_map.get(current_name, ()),
        )

    StateElement = state
    Context = context
    Clock = clock
    Stop = stop
    Restart = restart
    Get = get
    Set = set
    Call = call
    TakeSnapshot = take_snapshot


class Group:
    def __init__(self, *instances: typing.Union[str, Instance, "Group", None]):
        self.instances: list[Instance] = []
        self.id = _next_id()
        if instances and isinstance(instances[0], str):
            self.id = instances[0]
            instances = instances[1:]
        for instance in instances:
            if instance is None:
                continue
            if isinstance(instance, Group):
                self.instances.extend(instance.instances)
            elif isinstance(instance, Instance):
                self.instances.append(instance)

    def Instances(self) -> list[Instance]:
        return list(self.instances)

    def state(self) -> str:
        if not self.instances:
            return ""
        return self.instances[0].state()

    def context(self) -> Context | None:
        if not self.instances:
            return None
        return self.instances[0].context()

    def dispatch(
        self,
        event: Event,
        source: "HSM[typing.Any] | Group | None" = None,
    ) -> typing.Awaitable[None]:
        instances = [
            instance for instance in self.instances if _instance_is_started(instance)
        ]
        if not instances:
            raise ValidationError("operation requires a started HSM")
        return _await_all(instance.dispatch(event, source) for instance in instances)

    def stop(self) -> typing.Awaitable[None]:
        if self.instances:
            for instance in self.instances:
                if not _instance_is_started(instance):
                    raise ValidationError("operation requires a started HSM")
        return _await_all(instance.stop() for instance in self.instances)

    def restart(self, data: typing.Any = None) -> typing.Awaitable[None]:
        for instance in self.instances:
            if not _instance_is_started(instance):
                raise ValidationError("operation requires a started HSM")
        return _await_all(
            instance.restart(copy.deepcopy(data)) for instance in self.instances
        )

    def take_snapshot(self) -> Snapshot:
        snapshots = [TakeSnapshot(None, instance) for instance in self.instances]
        events: list[EventSnapshot] = []
        queue_len = 0
        ids: list[str] = []
        qualified_names: list[str] = []
        states: list[str] = []
        for snapshot in snapshots:
            ids.append(snapshot.ID)
            qualified_names.append(snapshot.QualifiedName)
            states.append(snapshot.StateElement)
            queue_len += snapshot.QueueLen
            events.extend(snapshot.Events)
        return Snapshot(
            ID=self.id if self.id else ",".join(ids),
            QualifiedName=",".join(qualified_names),
            StateElement=" | ".join(states),
            Attributes={},
            QueueLen=queue_len,
            Events=tuple(events),
        )


def NewGroup(*instances: typing.Union[str, Instance, Group, None]) -> Group:
    return Group(*instances)


MakeGroup = NewGroup


def _instance_is_started(instance: Instance) -> bool:
    try:
        snapshot = instance.take_snapshot()
    except ValidationError:
        return False
    return (
        bool(snapshot.QualifiedName) and snapshot.StateElement != snapshot.QualifiedName
    )


def _new_future() -> asyncio.Future[None]:
    return asyncio.get_running_loop().create_future()


def _after_future(
    waiters: list[tuple[typing.Any, asyncio.Future[None]]], expected: typing.Any
) -> asyncio.Future[None]:
    future = _new_future()
    waiter = (expected, future)
    waiters.append(waiter)

    def remove_waiter(done: asyncio.Future[None]) -> None:
        if not done.cancelled():
            return
        try:
            waiters.remove(waiter)
        except ValueError:
            pass

    future.add_done_callback(remove_waiter)
    return future


def _machine_from_instance_context(
    instance: Instance, *, require_started: bool
) -> "HSM[typing.Any] | None":
    machine, _ = FromContext(instance.context())
    if isinstance(machine, HSM):
        return machine
    if require_started:
        raise ValidationError("operation requires a started HSM")
    return None


def _machine_is_started(machine: "HSM[typing.Any]") -> bool:
    instances = machine._context.Value(Keys.Instances)
    registered = (
        isinstance(instances, collections.abc.Mapping)
        and instances.get(machine.id) is machine
    )
    return machine._processing.locked() or (
        registered and machine._state is not machine.model
    )


def _resolve_observable_machine(
    sm: typing.Union[HSM[TInstance], Instance],
) -> HSM[TInstance]:
    machine: HSM[TInstance]
    if isinstance(sm, HSM):
        machine = sm
    else:
        machine = typing.cast(
            HSM[TInstance],
            _machine_from_instance_context(sm, require_started=True),
        )
    if not _machine_is_started(machine):
        raise ValidationError("operation requires a started HSM")
    return machine


def AfterDispatch(
    ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], event: Event
) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(machine._after.dispatch, event.qualified_name)


def AfterProcess(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance],
    maybe_event: Event | None = None,
) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(
        machine._after.process,
        None if maybe_event is None else maybe_event.qualified_name,
    )


def AfterEntry(
    ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], state: str
) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(machine._after.entry, state)


def AfterExit(
    ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], state: str
) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(machine._after.exit, state)


def AfterExecuted(
    ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], state: str
) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(machine._after.executed, state)


def _event_from_name(
    event_or_name: str | Event, kind_value: int = Kinds.Event
) -> Event:
    if isinstance(event_or_name, Event):
        return event_or_name
    if event_or_name == AnyEvent.name:
        return AnyEvent
    return Event(name=event_or_name, kind=kind_value)


def _clone_event(event: Event[TData]) -> Event[TData]:
    schema = None if event.schema is None else copy.deepcopy(event.schema)
    return Event(
        name=event.name,
        data=event.data,
        kind=event.kind,
        id=event.id,
        source=event.source,
        target=event.target,
        qualified_name=event.qualified_name,
        schema=schema,
    )


def _machine_event_id(machine: "HSM[typing.Any] | Group | None") -> str:
    if isinstance(machine, HSM):
        return typing.cast(str, machine.id)
    return ""


def _context_machine(ctx: Context | None) -> "HSM[typing.Any] | Group | None":
    machine, ok = FromContext(ctx)
    return machine if ok else None


def _clone_event_for_delivery(
    event: Event[TData],
    target: "HSM[typing.Any]",
    source: "HSM[typing.Any] | Group | None" = None,
) -> Event[TData]:
    event_copy = _clone_event(event)
    source_id = _machine_event_id(source)
    if source_id and not event_copy.source:
        event_copy.source = source_id
    if not event_copy.target:
        event_copy.target = typing.cast(str, target.id)
    return event_copy


def _validate_model(model: Model) -> None:
    if not model.initial:
        raise ValidationError("initial state is required for state machine")
    if model.entry:
        raise ValidationError(
            "entry actions are not allowed on top level state machine"
        )
    if model.exit:
        raise ValidationError("exit actions are not allowed on top level state machine")
    _validate_synchronous_transition_hooks(model)
    _validate_submachine_transitions(model)
    _validate_transition_events(model)


def _finalize_model(model: Model) -> None:
    _finalize_operation_references(model)
    _finalize_entry_point_targets(model)
    _finalize_exit_point_targets(model)
    _finalize_entry_point_artifact_removal(model)
    _finalize_exit_point_artifact_removal(model)
    _finalize_when_transitions(model)
    _finalize_transition_paths(model)
    _finalize_history_target_owners(model)
    _finalize_history_paths(model)
    _finalize_transition_table(model)
    _finalize_snapshot_event_table(model)
    _finalize_deferred_table(model)


def _submachine_ancestor_for(
    model: Model, qualified_name: str
) -> SubmachineStateElement | None:
    current = qualified_name
    while current not in ("", ".", "/"):
        element = model.get(current, SubmachineStateElement)
        if element is not None:
            return element
        if current == model.qualified_name:
            return None
        current = _parent_path(current)
    return None


def _validate_synchronous_transition_hooks(model: Model) -> None:
    for element in model.members.values():
        if isinstance(element, GuardElement):
            if not _callable_is_synchronous(element.expression):
                raise ValidationError(
                    f'guard "{element.qualified_name}" must be a synchronous function'
                )
        elif isinstance(element, BehaviorElement) and element.kind not in (
            Kinds.Concurrent,
            Kinds.StateMachine,
        ):
            if not _callable_is_synchronous(element.operation):
                raise ValidationError(
                    f'behavior "{element.qualified_name}" must be a synchronous function'
                )


def _split_entry_point_target(model: Model, target: str) -> tuple[str, str] | None:
    target = posixpath.normpath(target)
    if not target or target == "/":
        return None
    if model.get(target, EntryPointElement) is None:
        return None
    return _parent_path(target), posixpath.basename(target)


def _validate_submachine_transitions(model: Model) -> None:
    for transition in list(model.members.values()):
        if not isinstance(transition, TransitionElement):
            continue
        validation_target = transition.target
        entry_point_target = _split_entry_point_target(model, transition.target)
        if entry_point_target is not None:
            target_name, entry_point_name = entry_point_target
            entry_point = model.get(
                join(target_name, entry_point_name), EntryPointElement
            )
            if entry_point is None:
                raise ValidationError(
                    f'EntryPoint "{entry_point_name}" can only target a SubmachineStateElement'
                )
            source = transition.source
            if source != target_name and source.startswith(target_name + "/"):
                raise ValidationError("entry point target cannot be internal")
            validation_target = target_name
        if validation_target:
            source_submachine = _submachine_ancestor_for(model, transition.source)
            target_submachine = _submachine_ancestor_for(model, validation_target)
            if (
                source_submachine is None
                and target_submachine is not None
                and validation_target != target_submachine.qualified_name
            ):
                raise ValidationError(
                    f'TransitionElement "{transition.qualified_name}" cannot target internal state "{validation_target}" of SubmachineStateElement "{target_submachine.qualified_name}"'
                )
        for event_name in transition.events:
            if not event_name.startswith("@exit:"):
                continue
            source_state = model.get(transition.source, SubmachineStateElement)
            if source_state is None:
                raise ValidationError(
                    "ExitPoint outcome can only be handled by a SubmachineStateElement"
                )
            exit_point_name = event_name.rsplit(":", 1)[-1]
            if (
                model.get(
                    join(source_state.qualified_name, exit_point_name),
                    ExitPointElement,
                )
                is None
            ):
                raise ValidationError(
                    f'SubmachineStateElement "{source_state.qualified_name}" has no exit point "{exit_point_name}"'
                )


def _finalize_entry_point_targets(model: Model) -> None:
    for transition in list(model.members.values()):
        if not isinstance(transition, TransitionElement):
            continue
        entry_point_target = _split_entry_point_target(model, transition.target)
        if entry_point_target is None:
            continue
        target_name, entry_point_name = entry_point_target
        entry_point = model.get(join(target_name, entry_point_name), EntryPointElement)
        if entry_point is None or not entry_point.transitions:
            continue
        entry_transition = model.get(entry_point.transitions[0], TransitionElement)
        if entry_transition is None:
            continue
        transition.effect_failure_state_index = len(transition.effect)
        transition.effect = [*transition.effect, *entry_transition.effect]
        transition.target = entry_transition.target
        transition.effect_failure_state = target_name


def _finalize_entry_point_artifact_removal(model: Model) -> None:
    for name, element in list(model.members.items()):
        if isinstance(element, EntryPointElement):
            for transition_name in element.transitions:
                model.members.pop(transition_name, None)
            model.members.pop(name, None)


def _finalize_exit_point_targets(model: Model) -> None:
    for transition in list(model.members.values()):
        if not isinstance(transition, TransitionElement):
            continue
        exit_point = model.get(transition.target, ExitPointElement)
        if exit_point is None:
            continue
        boundary = model.get(exit_point.owner(), SubmachineStateElement)
        if boundary is None:
            continue
        exit_transition = model.get(
            join(exit_point.qualified_name, "exit"), TransitionElement
        )
        exit_effects = (
            list(exit_transition.effect) if exit_transition is not None else []
        )
        transition.effect_failure_state_index = len(transition.effect)
        transition.effect = [*transition.effect, *exit_effects]
        transition.target = boundary.qualified_name
        transition.effect_failure_state = boundary.qualified_name
        completion_event = _exit_point_event_name(
            boundary.qualified_name, exit_point.name()
        )
        _register_event(
            model,
            Event(
                name=completion_event,
                qualified_name=completion_event,
                kind=Kinds.CompletionEvent,
            ),
        )


def _finalize_exit_point_artifact_removal(model: Model) -> None:
    for name, element in list(model.members.items()):
        if not isinstance(element, ExitPointElement):
            continue
        model.members.pop(join(element.qualified_name, "exit"), None)
        model.members.pop(name, None)


def _finalize_when_transitions(model: Model) -> None:
    attribute_events: list[str] = []
    for name in model.attributes:
        if model.get(name, Event) is None:
            _register_event(
                model,
                Event(
                    name=name,
                    qualified_name=name,
                    kind=Kinds.ChangeEvent,
                    source=name,
                    schema=AttributeChange,
                ),
            )
        attribute_events.append(name)
    for transition in list(model.members.values()):
        if not isinstance(transition, TransitionElement):
            continue
        if (
            transition.when is None
            and transition.generated_when is not None
            and attribute_events
        ):
            transition.when = transition.generated_when
        if transition.when is None:
            continue
        if transition.events:
            if transition.when_attribute is None:
                if attribute_events:
                    synthetic_events = {
                        event_name
                        for event_name in transition.events
                        if event_name.endswith("/.when")
                    }
                    if synthetic_events:
                        transition.events = [
                            event_name
                            for event_name in transition.events
                            if event_name not in synthetic_events
                        ]
                        source = model.get(transition.source, StateElement)
                        if source is not None:
                            source.activity = [
                                behavior_name
                                for behavior_name in source.activity
                                if not (
                                    (
                                        behavior := model.get(
                                            behavior_name, BehaviorElement[typing.Any]
                                        )
                                    )
                                    is not None
                                    and any(
                                        marker in behavior.qualified_name
                                        for marker in synthetic_events
                                    )
                                )
                            ]
                existing = builtins.set(transition.events)
                transition.events.extend(
                    event_name
                    for event_name in attribute_events
                    if event_name not in existing
                )
            continue
        if attribute_events:
            transition.events.extend(attribute_events)
            continue
        source = model.get(transition.source, StateElement)
        guard = model.get(transition.when, GuardElement)
        if source is None or guard is None:
            continue
        event = Event(
            name=join(transition.qualified_name, ".when"),
            qualified_name=join(transition.qualified_name, ".when"),
            kind=Kinds.ChangeEvent,
        )
        _register_event(model, event)
        transition.events.append(event.qualified_name)
        transition.generated_when = transition.when
        transition.when = None

        async def operation(
            ctx: Context,
            instance: Instance,
            current_event: Event,
            expression: Expression[typing.Any] = guard.expression,
            trigger_event: Event = event,
        ) -> None:
            try:
                result = await _maybe_await(expression(ctx, instance, current_event))
                await _normalize_waitable(result)
                if not ctx.is_done():
                    instance.dispatch(trigger_event)
            except asyncio.CancelledError:
                if _task_is_cancelling() or ctx.is_done():
                    return
                raise

        behavior = BehaviorElement(
            qualified_name=join(
                source.qualified_name, event.name, str(len(model.members))
            ),
            kind=Kinds.Concurrent,
            operation=operation,
            scope=source.qualified_name,
        )
        source.activity.append(behavior.qualified_name)
        model.set(behavior.qualified_name, behavior)


def _validate_transition_events(model: Model) -> None:
    for transition in model.members.values():
        if not isinstance(transition, TransitionElement):
            continue
        source = model.get(transition.source, VertexElement)
        if source is None or isinstance(source, PseudostateElement):
            continue
        if not transition.events and transition.when is None:
            raise ValidationError(
                f'TransitionElement "{transition.qualified_name}" has no events'
            )


def _finalize_transition_paths(model: Model) -> None:
    for element in model.members.values():
        if not isinstance(element, TransitionElement):
            continue
        element.paths.clear()
        ResolvePaths(transition=element).apply(model, [])
        if not element.effect_failure_state:
            continue
        for path in element.paths.values():
            path.effect_failure_state = element.effect_failure_state
            path.effect_failure_state_index = element.effect_failure_state_index


def _finalize_history_target_owners(model: Model) -> None:
    for element in model.members.values():
        if not isinstance(element, TransitionElement):
            continue
        target = model.get(element.target, VertexElement)
        element.history_target_owner = (
            target.owner()
            if isinstance(target, (ShallowHistoryElement, DeepHistoryElement))
            else None
        )


def _finalize_history_paths(model: Model) -> None:
    model.history_paths.clear()
    history_owners = {
        element.owner()
        for element in model.members.values()
        if isinstance(element, (ShallowHistoryElement, DeepHistoryElement))
    }
    for owner in history_owners:
        if not owner:
            continue
        for target, element in model.members.items():
            if not isinstance(element, VertexElement):
                continue
            if target == owner or not IsAncestor(owner, target):
                continue
            current = target
            enter: list[str] = []
            while current not in ("", owner):
                enter.insert(0, current)
                next_path = _parent_path(current)
                if next_path == current:
                    break
                current = next_path
            model.history_paths[(owner, target)] = tuple(enter)


def _finalize_transition_table(model: Model) -> None:
    model.transition_map.clear()
    for state_name, element in model.members.items():
        if not isinstance(element, StateElement):
            continue
        model.transition_map[state_name] = {}
        transitions_by_event: dict[str, list[tuple[TransitionElement, int]]] = {}
        for index, transition_name in enumerate(element.transitions):
            transition = model.get(transition_name, TransitionElement)
            if transition is None or not transition.events:
                continue
            for event_name in transition.events:
                transitions_by_event.setdefault(event_name, []).append(
                    (transition, index)
                )
        for event_name, transitions in transitions_by_event.items():
            if event_name.startswith("@exit:"):
                transitions.sort(
                    key=lambda item: (
                        item[0].guard is None,
                        item[1],
                    )
                )
            else:
                transitions.sort(key=lambda item: item[1])
            model.transition_map[state_name][event_name] = [
                item[0] for item in transitions
            ]


def _finalize_snapshot_event_table(model: Model) -> None:
    model.snapshot_event_map.clear()
    for state_name, element in model.members.items():
        if not isinstance(element, StateElement):
            continue
        snapshots: list[EventSnapshot] = []
        current_path = state_name
        while current_path:
            source = model.get(current_path, StateElement)
            if source is None:
                break
            for transition_name in source.transitions:
                transition = model.get(transition_name, TransitionElement)
                if transition is None:
                    continue
                if transition.paths and state_name not in transition.paths:
                    continue
                for event_name in transition.events:
                    event = model.events.get(event_name)
                    if event is None:
                        continue
                    if event.kind == Kinds.CompletionEvent and not isinstance(
                        element, FinalStateElement
                    ):
                        continue
                    guard = (
                        model.get(transition.guard, GuardElement)
                        if transition.guard is not None
                        else None
                    )
                    snapshots.append(
                        EventSnapshot(
                            Name=event_name,
                            Kind=event.kind,
                            Target=transition.target or None,
                            GuardElement=(
                                bool(
                                    getattr(
                                        guard.expression, "_hsm_snapshot_guard", True
                                    )
                                )
                                if guard is not None
                                else False
                            ),
                            Schema=event.schema,
                        )
                    )
            if current_path in ("", "/", model.qualified_name):
                if current_path == model.qualified_name:
                    current_path = _parent_path(current_path)
                else:
                    break
            current_path = source.owner()
        model.snapshot_event_map[state_name] = tuple(snapshots)


def _finalize_deferred_table(model: Model) -> None:
    model.deferred_map.clear()
    model.direct_deferred_map.clear()
    model.submachine_owner_map.clear()
    for state_name, element in model.members.items():
        if not isinstance(element, StateElement):
            continue
        current_owner = state_name
        while current_owner:
            owner_state = model.members.get(current_owner)
            if isinstance(owner_state, SubmachineStateElement):
                model.submachine_owner_map[state_name] = current_owner
                break
            if current_owner in ("", "/", model.qualified_name):
                break
            current_owner = _parent_path(current_owner)
        model.deferred_map[state_name] = {}
        model.direct_deferred_map[state_name] = builtins.set(element.deferred)
        current_path = state_name
        while current_path:
            current_state = model.members.get(current_path)
            if isinstance(current_state, StateElement):
                for deferred_event in current_state.deferred:
                    model.deferred_map[state_name].setdefault(
                        deferred_event, current_path
                    )
            if current_path in ("", "/", model.qualified_name):
                if current_path == model.qualified_name:
                    current_path = _parent_path(current_path)
                else:
                    break
            current_path = _parent_path(current_path)


def Define(name: str, *elements: NamedElement) -> Model:
    _validate_slashless_name("model", name)
    qualified_name = join("/", name)
    model = Model(qualified_name=qualified_name)
    model.set(qualified_name, model)
    apply(model, model, [], list(elements))
    while model.owned_elements:
        partial = model.owned_elements.pop()
        if isinstance(partial, PartialElement):
            partial.apply(model, [])
    model.owned_elements = list(elements)
    _validate_model(model)
    _finalize_model(model)
    return model


def State(name: str, *elements: NamedElement) -> PartialState:
    return PartialState(qualified_name=name, owned_elements=list(elements))


def SubmachineState(
    name: str, machine: Model, *elements: NamedElement
) -> PartialSubmachineState:
    return PartialSubmachineState(
        qualified_name=name,
        machine=machine,
        owned_elements=list(elements),
    )


def Initial(
    name_or_element: str | NamedElement, *elements: NamedElement
) -> PartialInitial:
    name = ".initial"
    owned_elements = list(elements)
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.insert(0, name_or_element)
    return PartialInitial(qualified_name=name, owned_elements=owned_elements)


def Transition(
    name_or_element: str | PartialElement, *elements: NamedElement
) -> PartialTransition:
    name = ""
    owned_elements = list(elements)
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.insert(0, name_or_element)
    return PartialTransition(qualified_name=name, owned_elements=owned_elements)


def Source(name_or_element: str | NamedElement) -> PartialSource:
    if isinstance(name_or_element, str):
        return PartialSource(qualified_name=name_or_element)
    return PartialSource(owned_elements=[name_or_element])


def Target(name_or_element: str | NamedElement) -> PartialTarget:
    if isinstance(name_or_element, str):
        return PartialTarget(qualified_name=name_or_element)
    return PartialTarget(owned_elements=[name_or_element])


def Entry(*operations: BehaviorArgument[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(
        operations=list(operations), type=StateElement, qualified_name="entry"
    )


def Exit(*operations: BehaviorArgument[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(
        operations=list(operations), type=StateElement, qualified_name="exit"
    )


def Activity(*operations: BehaviorArgument[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(
        operations=list(operations),
        type=StateElement,
        concurrent=True,
        qualified_name="activity",
    )


def Effect(*operations: BehaviorArgument[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(
        operations=list(operations), type=TransitionElement, qualified_name="effect"
    )


def Guard(expression: ExpressionArgument[TInstance]) -> PartialGuard[TInstance]:
    return PartialGuard(
        qualified_name=expression
        if isinstance(expression, str)
        else getattr(expression, "__name__", "guard"),
        expression=expression,
    )


def On(*events: str | Event) -> PartialTrigger:
    return PartialTrigger(events=[_event_from_name(event) for event in events])


def OnSet(name: str) -> PartialOnSet:
    return PartialOnSet(qualified_name=name)


def OnCall(name: str) -> PartialOnCall:
    return PartialOnCall(qualified_name=name)


def _duration_attribute(name: str) -> Duration[typing.Any]:
    async def duration(ctx: Context, instance: Instance, event: Event) -> timedelta:
        value, _ = Get(ctx, instance, name)
        return typing.cast(timedelta, value)

    duration.__name__ = f"attribute_{name}"
    return duration


def _timepoint_attribute(name: str) -> Timepoint[typing.Any]:
    async def timepoint(ctx: Context, instance: Instance, event: Event) -> datetime:
        value, _ = Get(ctx, instance, name)
        return typing.cast(datetime, value)

    timepoint.__name__ = f"attribute_{name}"
    return timepoint


def After(duration: str | Duration[TInstance]) -> PartialAfter[TInstance]:
    if isinstance(duration, str):
        return PartialAfter(duration=_duration_attribute(duration))
    return PartialAfter(duration=duration)


def At(timepoint: str | Timepoint[TInstance]) -> PartialAt[TInstance]:
    if isinstance(timepoint, str):
        return PartialAt(timepoint=_timepoint_attribute(timepoint))
    return PartialAt(timepoint=timepoint)


def Every(duration: str | Duration[TInstance]) -> PartialEvery[TInstance]:
    if isinstance(duration, str):
        return PartialEvery(duration=_duration_attribute(duration))
    return PartialEvery(duration=duration)


def When(
    expression: str | WhenExpression[TInstance],
) -> PartialOnSet | PartialWhen[TInstance]:
    if isinstance(expression, str):
        return PartialWhen(attribute=expression)
    return PartialWhen(expression=expression)


def Defer(*events: str | Event) -> PartialDefer:
    return PartialDefer(events=[_event_from_name(event) for event in events])


def Choice(
    element_or_name: str | PartialTransition,
    *transitions: PartialTransition,
) -> PartialChoice:
    name = ""
    owned_elements: list[NamedElement] = list(transitions)
    if isinstance(element_or_name, str):
        name = element_or_name
    else:
        owned_elements.insert(0, element_or_name)
    return PartialChoice(qualified_name=name, owned_elements=owned_elements)


def ShallowHistory(
    element_or_name: str | PartialTransition,
    *partials: NamedElement,
) -> PartialHistory:
    name = ""
    owned_elements = list(partials)
    if isinstance(element_or_name, str):
        name = element_or_name
    else:
        owned_elements.insert(0, element_or_name)
    return PartialHistory(
        qualified_name=name,
        owned_elements=owned_elements,
        history_type=ShallowHistoryElement,
    )


def DeepHistory(
    element_or_name: str | PartialTransition,
    *partials: NamedElement,
) -> PartialHistory:
    name = ""
    owned_elements = list(partials)
    if isinstance(element_or_name, str):
        name = element_or_name
    else:
        owned_elements.insert(0, element_or_name)
    return PartialHistory(
        qualified_name=name,
        owned_elements=owned_elements,
        history_type=DeepHistoryElement,
    )


def Final(name_or_element: str | NamedElement) -> PartialFinal:
    if isinstance(name_or_element, str):
        return PartialFinal(qualified_name=name_or_element)
    return PartialFinal(owned_elements=[name_or_element])


def EntryPoint(name: str, *partials: NamedElement) -> PartialEntryPoint:
    return PartialEntryPoint(qualified_name=name, owned_elements=list(partials))


def ExitPoint(name: str, *partials: NamedElement) -> PartialExitPoint:
    return PartialExitPoint(qualified_name=name, owned_elements=list(partials))


_ATTRIBUTE_DEFAULT_UNSET = object()


def Attribute(
    name: str,
    maybe_type_or_default: typing.Any = _ATTRIBUTE_DEFAULT_UNSET,
    maybe_default: typing.Any = _ATTRIBUTE_DEFAULT_UNSET,
) -> PartialAttribute:
    if maybe_default is not _ATTRIBUTE_DEFAULT_UNSET:
        type_was_provided = isinstance(maybe_type_or_default, type)
        value_type = maybe_type_or_default if type_was_provided else None
        return PartialAttribute(
            qualified_name=name,
            default=maybe_default,
            value_type=value_type,
            dynamic=not type_was_provided,
        )
    if isinstance(maybe_type_or_default, type):
        return PartialAttribute(
            qualified_name=name,
            default=None,
            value_type=maybe_type_or_default,
        )
    default = (
        None
        if maybe_type_or_default is _ATTRIBUTE_DEFAULT_UNSET
        else maybe_type_or_default
    )
    value_type = None if default is None else type(default)
    return PartialAttribute(qualified_name=name, default=default, value_type=value_type)


def New(
    instance: TInstance, model: Model, maybe_config: Config | None = None
) -> HSM[TInstance]:
    return HSM(instance=instance, model=model, config=maybe_config)


async def Start(
    ctx: Context | None,
    instance: TInstance | HSM[TInstance],
    model: Model | typing.Any | None = None,
    data: typing.Any = None,
) -> HSM[TInstance]:
    if isinstance(instance, HSM):
        sm = instance
        if sm.state() != sm.model.qualified_name or sm._processing.locked():
            raise ValidationError("Start() called on an already started HSM")
        if isinstance(
            instances := sm.context().Value(Keys.Instances),
            collections.abc.MutableMapping,
        ):
            for key, value in list(instances.items()):
                if value is sm:
                    del instances[key]
        sm._context = _WithRuntimeHSM(ctx or Context(), sm)
        sm._reset_for_restart()
        start_data = model
    else:
        if not isinstance(model, Model):
            raise ValidationError("Start() requires a model when starting an instance")
        state = instance.state()
        if instance.context() is not None and state and state != model.qualified_name:
            raise ValidationError(
                "Start() called on an instance that already has a running HSM"
            )
        sm = HSM(instance=instance, model=model, ctx=ctx)
        start_data = data
    await sm._start(start_data)
    return sm


def Started(
    ctx: Context | None,
    instance: TInstance,
    model: Model,
    maybe_config: Config | None = None,
) -> typing.Awaitable[HSM[TInstance]]:
    if instance.context() is not None and instance.state():
        raise ValidationError(
            "Start() called on an instance that already has a running HSM"
        )
    data = maybe_config.Data if maybe_config is not None else None
    return Start(ctx, New(instance, model, maybe_config), data)


def Stop(sm: typing.Union[HSM[TInstance], Instance, Group]) -> typing.Awaitable[None]:
    if isinstance(sm, Group):
        return sm.stop()
    return sm.stop()


def Restart(
    sm: typing.Union[HSM[TInstance], Instance, Group],
    data: typing.Any = None,
) -> typing.Awaitable[None]:
    if isinstance(sm, Group):
        return sm.restart(data)
    return sm.restart(data)


def Dispatch(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
    event: Event,
) -> typing.Awaitable[None]:
    source = _context_machine(ctx)
    if isinstance(hsm, Group):
        return hsm.dispatch(event, source)
    return hsm.dispatch(event, source)


def DispatchAll(ctx: Context | None, event: Event) -> typing.Awaitable[None]:
    if ctx is None or ctx.done:
        return _completed_none()
    machines = [
        machine
        for machine in InstancesFromContext(ctx)[0]
        if isinstance(machine, HSM)
        and isinstance(
            (instances := machine._context.Value(Keys.Instances)),
            collections.abc.Mapping,
        )
        and instances.get(machine.id) is machine
        and (machine._state is not machine.model or machine._processing.locked())
    ]
    source = _context_machine(ctx)
    return _dispatch_machines(
        (
            (machine, _clone_event_for_delivery(event, machine, source))
            for machine in machines
        )
    )


def DispatchTo(
    ctx: Context | None, event: Event, *maybe_ids: str
) -> typing.Awaitable[None]:
    if ctx is None or ctx.done:
        return _completed_none()
    machines = [
        machine
        for machine in InstancesFromContext(ctx)[0]
        if isinstance(machine, HSM)
        and isinstance(
            (instances := machine._context.Value(Keys.Instances)),
            collections.abc.Mapping,
        )
        and instances.get(machine.id) is machine
        and (machine._state is not machine.model or machine._processing.locked())
    ]
    if maybe_ids:
        selected = []
        seen: builtins.set[int] = builtins.set()
        for maybe_id in maybe_ids:
            for machine in machines:
                if builtins.id(machine) in seen or not Match(
                    machine.take_snapshot().ID, maybe_id
                ):
                    continue
                selected.append(machine)
                seen.add(builtins.id(machine))
    else:
        selected = machines
    source = _context_machine(ctx)
    return _dispatch_machines(
        (
            (machine, _clone_event_for_delivery(event, machine, source))
            for machine in selected
        )
    )


def Get(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance],
    name: str,
) -> tuple[typing.Any, bool]:
    return hsm.get(name)


def Set(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance],
    name: str,
    value: typing.Any,
) -> typing.Awaitable[None]:
    return hsm.set(name, value)


def Call(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance],
    name: str,
    *args: typing.Any,
) -> typing.Awaitable[typing.Any]:
    return hsm.call(name, *args)


def TakeSnapshot(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
) -> Snapshot:
    if isinstance(hsm, Group):
        return hsm.take_snapshot()
    return hsm.take_snapshot()


define = Define
element = Element
validation_error = ValidationError
behavior = BehaviorElement
model = Model
instance = Instance
state = StateElement
submachine_state = SubmachineStateElement
final_state = FinalStateElement
initial = InitialElement
transition = TransitionElement
choice = ChoiceElement
source = Source
target = Target
entry = Entry
exit = Exit
activity = Activity
effect = Effect
guard = GuardElement
on = On
after = After
at = At
every = Every
when = When
defer = Defer
final = Final
entry_point = EntryPoint
exit_point = ExitPoint
new = New
start = Start
started = Started
stop = Stop
restart = Restart
dispatch = Dispatch
dispatch_all = DispatchAll
dispatch_to = DispatchTo
get = Get
set = Set
call = Call
take_snapshot = TakeSnapshot
attribute = Attribute
operation = Operation
on_set = OnSet
onset = OnSet
on_call = OnCall
shallow_history = ShallowHistory
deep_history = DeepHistory
new_group = NewGroup
make_group = MakeGroup
after_dispatch = AfterDispatch
after_process = AfterProcess
after_entry = AfterEntry
after_exit = AfterExit
after_executed = AfterExecuted
lca = LCA


__all__ = [
    "Activity",
    "After",
    "AfterDispatch",
    "AfterEntry",
    "AfterExecuted",
    "AfterExit",
    "AfterProcess",
    "At",
    "AnyEvent",
    "Attribute",
    "AttributeChange",
    "AttributeKind",
    "BehaviorElement",
    "BehaviorKind",
    "Call",
    "CallData",
    "CallEventKind",
    "ChangeEventKind",
    "ChoiceElement",
    "ChoiceKind",
    "Clock",
    "CompletionEvent",
    "CompletionEventKind",
    "Config",
    "ConcurrentKind",
    "ConstraintKind",
    "Context",
    "ContextKey",
    "ContextKeys",
    "DeepHistory",
    "DeepHistoryKind",
    "Defer",
    "Define",
    "State",
    "Initial",
    "Transition",
    "Choice",
    "SubmachineState",
    "DefaultClock",
    "Dispatch",
    "DispatchAll",
    "DispatchTo",
    "Effect",
    "Guard",
    "Element",
    "ElementKind",
    "Entry",
    "EntryPoint",
    "EntryPointElement",
    "EntryPointKind",
    "ErrorEvent",
    "ErrorEventKind",
    "Event",
    "EventKind",
    "EventSnapshot",
    "Every",
    "Exit",
    "ExitPoint",
    "ExitPointKind",
    "Expression",
    "ExternalKind",
    "Final",
    "FinalEvent",
    "FinalStateElement",
    "FinalStateKind",
    "FromContext",
    "Get",
    "Group",
    "GuardElement",
    "HSM",
    "ID",
    "InitialElement",
    "InitialEvent",
    "InitialKind",
    "InfiniteDuration",
    "Instance",
    "InstancesFromContext",
    "InternalKind",
    "IsAncestor",
    "IsKind",
    "Kinds",
    "Keys",
    "LCA",
    "LocalKind",
    "Match",
    "MakeGroup",
    "MakeKind",
    "Model",
    "Name",
    "NamedElementKind",
    "NamespaceKind",
    "New",
    "NewGroup",
    "NullKind",
    "On",
    "OnCall",
    "OnSet",
    "Operation",
    "OperationKind",
    "PartialKind",
    "PseudostateKind",
    "Fifo",
    "Queue",
    "QueueLenResult",
    "QueuePopResult",
    "QueuePushResult",
    "QualifiedName",
    "Restart",
    "SelfKind",
    "SequentialKind",
    "Set",
    "ShallowHistory",
    "ShallowHistoryKind",
    "Snapshot",
    "Source",
    "Start",
    "Started",
    "StateElement",
    "StateKind",
    "StateMachineKind",
    "SubmachineStateElement",
    "SubmachineStateKind",
    "Stop",
    "TakeSnapshot",
    "Target",
    "TimeEventKind",
    "TransitionElement",
    "TransitionKind",
    "ValidationError",
    "VertexElement",
    "VertexKind",
    "PseudostateElement",
    "When",
    "activity",
    "after",
    "after_dispatch",
    "after_entry",
    "after_executed",
    "after_exit",
    "after_process",
    "at",
    "attribute",
    "attribute_kind",
    "any_event",
    "attribute_change",
    "behavior_kind",
    "behavior",
    "call",
    "call_data",
    "call_event_kind",
    "change_event_kind",
    "choice",
    "choice_kind",
    "clock",
    "completion_event",
    "completion_event_kind",
    "concurrent_kind",
    "config",
    "constraint_kind",
    "context_key",
    "deep_history_kind",
    "deep_history",
    "default_clock",
    "define",
    "defer",
    "dispatch",
    "dispatch_all",
    "dispatch_to",
    "effect",
    "element",
    "element_kind",
    "entry",
    "entry_point",
    "error_event",
    "error_event_kind",
    "event",
    "event_snapshot",
    "event_kind",
    "every",
    "exit",
    "exit_point",
    "exit_point_kind",
    "expression",
    "external_kind",
    "final",
    "final_event",
    "final_state",
    "final_state_kind",
    "from_context",
    "get",
    "guard",
    "id",
    "initial",
    "initial_event",
    "initial_kind",
    "infinite_duration",
    "instance",
    "instances_from_context",
    "internal_kind",
    "is_ancestor",
    "is_kind",
    "kinds",
    "keys",
    "lca",
    "local_kind",
    "match",
    "make_kind",
    "make_group",
    "model",
    "name",
    "named_element_kind",
    "namespace_kind",
    "new",
    "new_group",
    "null_kind",
    "on",
    "on_call",
    "on_set",
    "onset",
    "operation",
    "operation_kind",
    "partial_kind",
    "pseudostate_kind",
    "qualified_name",
    "restart",
    "self_kind",
    "sequential_kind",
    "set",
    "shallow_history",
    "shallow_history_kind",
    "snapshot",
    "source",
    "start",
    "started",
    "state",
    "state_kind",
    "state_machine_kind",
    "submachine_state",
    "submachine_state_kind",
    "stop",
    "take_snapshot",
    "target",
    "time_event_kind",
    "transition",
    "transition_kind",
    "validation_error",
    "vertex_kind",
    "when",
]


def ID(hsm: typing.Union[HSM[TInstance], Instance, Group]) -> str:
    return TakeSnapshot(None, hsm).ID


def QualifiedName(hsm: typing.Union[HSM[TInstance], Instance, Group]) -> str:
    return TakeSnapshot(None, hsm).QualifiedName


def Name(hsm: typing.Union[HSM[TInstance], Instance, Group]) -> str:
    return posixpath.basename(QualifiedName(hsm))


id = ID
qualified_name = QualifiedName
name = Name


def _install_snake_case_aliases() -> None:
    for exported_name in list(__all__):
        if exported_name == "HSM" or not exported_name[:1].isupper():
            continue
        alias = _to_snake_case(exported_name)
        if alias == exported_name:
            continue
        globals().setdefault(alias, globals()[exported_name])
        if alias not in __all__:
            __all__.append(alias)


_install_snake_case_aliases()


if __name__ == "__main__":
    model = Define(
        "root", StateElement("s1"), StateElement("s2"), InitialElement(Target("s1"))
    )
    print(model.members)
