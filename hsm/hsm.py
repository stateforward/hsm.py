from __future__ import annotations

import asyncio
import builtins
import collections
import collections.abc
import contextvars
import copy
import fnmatch
import functools
import inspect
import posixpath
import re
import sys
import threading
import typing
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
from types import MappingProxyType, SimpleNamespace

from .kind import IsKind, MakeKind, is_kind, make_kind

TElement = typing.TypeVar("TElement", bound="Element")
TInstance = typing.TypeVar("TInstance", bound="Instance")
TData = typing.TypeVar("TData", default=typing.Any)
TNewData = typing.TypeVar("TNewData")
_next_id_counter = 0
_execution_scopes: contextvars.ContextVar[dict[int, str]] = contextvars.ContextVar(
    "hsm_execution_scopes",
    default={},
)

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
    return posixpath.normpath(posixpath.join(path, *paths))


@functools.lru_cache(maxsize=None)
def _parent_path(path: str) -> str:
    return posixpath.dirname(path)


def _future_done() -> asyncio.Future[None]:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.set_result(None)
    return future


async def _maybe_await(value: typing.Any) -> typing.Any:
    if inspect.isawaitable(value):
        return await typing.cast(typing.Awaitable[typing.Any], value)
    return value


async def _completed_none() -> None:
    return None


async def _await_all(awaitables: collections.abc.Iterable[typing.Awaitable[typing.Any]]) -> None:
    await asyncio.gather(*awaitables)


async def _await_all_shielded(awaitables: collections.abc.Iterable[typing.Awaitable[typing.Any]]) -> None:
    await asyncio.gather(*(asyncio.shield(awaitable) for awaitable in awaitables))


def _await_all_started(awaitables: collections.abc.Iterable[typing.Awaitable[typing.Any]]) -> typing.Awaitable[None]:
    pending = list(awaitables)
    if not pending:
        return _completed_none()
    return asyncio.create_task(_await_all(pending))


def _await_all_shielded_started(awaitables: collections.abc.Iterable[typing.Awaitable[typing.Any]]) -> typing.Awaitable[None]:
    pending = list(awaitables)
    if not pending:
        return _completed_none()
    return asyncio.create_task(_await_all_shielded(pending))


async def _dispatch_machines_sequential(
    dispatches: collections.abc.Iterable[tuple["HSM[typing.Any]", Event]]
) -> None:
    for machine, event in dispatches:
        await machine.dispatch(event)


def _dispatch_machines_sequential_started(
    dispatches: collections.abc.Iterable[tuple["HSM[typing.Any]", Event]]
) -> typing.Awaitable[None]:
    pending = list(dispatches)
    if not pending:
        return _completed_none()
    return asyncio.create_task(_dispatch_machines_sequential(pending))


async def _dispatch_machines_ordered_started(
    dispatches: collections.abc.Iterable[tuple["HSM[typing.Any]", Event]]
) -> None:
    waiters: list[typing.Awaitable[None]] = []
    for machine, event in dispatches:
        waiters.append(machine._dispatch_task(event))
    await _await_all_shielded(waiters)


def _dispatch_machines_ordered_task(
    dispatches: collections.abc.Iterable[tuple["HSM[typing.Any]", Event]]
) -> typing.Awaitable[None]:
    pending = list(dispatches)
    if not pending:
        return _completed_none()
    return asyncio.create_task(_dispatch_machines_ordered_started(pending))


def _start_later_dispatches(
    dispatches: collections.abc.Sequence[tuple["HSM[typing.Any]", int, typing.Awaitable[None]]]
) -> asyncio.Future[None]:
    loop = asyncio.get_running_loop()
    done: asyncio.Future[None] = loop.create_future()

    def start_at(index: int) -> None:
        if done.done():
            return
        if index >= len(dispatches):
            done.set_result(None)
            return
        machine, event_kind, waiter = dispatches[index]
        try:
            machine._start_scheduled_processing(event_kind)
        except BaseException as error:
            done.set_exception(error)
            return
        asyncio.ensure_future(waiter).add_done_callback(
            lambda completed, next_index=index + 1: _continue_later_dispatches(done, completed, next_index, start_at)
        )

    loop.call_soon(start_at, 0)
    return done


def _continue_later_dispatches(
    done: asyncio.Future[None],
    completed: asyncio.Future[typing.Any],
    next_index: int,
    start_at: typing.Callable[[int], None],
) -> None:
    if done.done():
        return
    try:
        completed.result()
    except BaseException as error:
        done.set_exception(error)
        return
    asyncio.get_running_loop().call_soon(start_at, next_index)


def _queue_dispatches_for_later(
    dispatches: collections.abc.Iterable[tuple["HSM[typing.Any]", Event]]
) -> typing.Awaitable[None]:
    scheduled: list[tuple[HSM[typing.Any], int, typing.Awaitable[None]]] = []
    waiters: list[typing.Awaitable[None]] = []
    current_task = asyncio.current_task()
    for machine, event in dispatches:
        machine._ensure_accepting_events()
        machine._queue_push(_clone_event(event))
        if machine._after.dispatch:
            machine._after._notify(machine._after.dispatch, lambda expected: expected == event.qualified_name)
        if (
            (event.kind != Kinds.TimeEvent and current_task in machine._active_timer_tasks)
            or (event.kind == Kinds.ChangeEvent and current_task in machine._active_tasks)
        ):
            if event.kind != Kinds.TimeEvent and current_task in machine._active_timer_tasks:
                machine._timer_task_pending_dispatch = True
            waiters.append(_future_done())
            continue
        acquired = machine._processing.try_acquire()
        if not acquired and asyncio.current_task() is machine._awaitable:
            waiters.append(_future_done())
            continue
        if not acquired and isinstance(machine._awaitable, asyncio.Future) and machine._awaitable.done():
            waiters.append(_future_done())
            continue
        waiter = machine._processing.wait()
        waiters.append(waiter)
        if acquired:
            scheduled.append((machine, event.kind, waiter))
    if scheduled:
        return _start_later_dispatches(scheduled)
    return _await_all_shielded_started(waiters)


def _dispatch_from_processing(source: "HSM[typing.Any] | Group | None") -> bool:
    return isinstance(source, HSM) and asyncio.current_task() is source._awaitable


def _raise_async_required(value: typing.Any = None) -> typing.NoReturn:
    _close_awaitable(value)
    raise _AsyncRequired()


def _close_awaitable(value: typing.Any) -> None:
    close = getattr(value, "close", None)
    if callable(close):
        close()


def _is_context_parameter(parameter: inspect.Parameter) -> bool:
    if parameter.name in {"ctx", "context"}:
        return True
    annotation = parameter.annotation
    return annotation is Context or annotation == "Context"


def _is_instance_parameter(parameter: inspect.Parameter) -> bool:
    if parameter.name in {"inst", "instance"}:
        return True
    annotation = parameter.annotation
    if annotation is Instance or annotation == "Instance":
        return True
    return inspect.isclass(annotation) and issubclass(annotation, Instance)


def _operation_argument_candidates(
    callback: OperationImplementation,
    ctx: "Context",
    instance: "Instance",
    args: tuple[typing.Any, ...],
) -> list[tuple[typing.Any, ...]]:
    base_candidates = [
        (ctx, instance, *args),
        (ctx, *args),
        (instance, *args),
        args,
    ]
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return base_candidates
    positional = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    if not positional:
        return base_candidates
    first = positional[0]
    if _is_context_parameter(first):
        return base_candidates
    if _is_instance_parameter(first):
        return [
            (instance, *args),
            (ctx, instance, *args),
            args,
            (ctx, *args),
        ]
    return base_candidates


def _invoke_operation_callback(
    callback: OperationImplementation,
    ctx: "Context",
    instance: "Instance",
    args: tuple[typing.Any, ...],
) -> typing.Any:
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(*args)
    for candidate in _operation_argument_candidates(callback, ctx, instance, args):
        try:
            signature.bind(*candidate)
        except TypeError:
            continue
        return callback(*candidate)
    return callback(*args)


def _operation_callback(
    model: "Model",
    instance: "Instance",
    name: str,
    scope: str = "",
) -> OperationImplementation:
    operation_name = _resolve_operation_name(model, scope, name)
    operation = model.operations.get(operation_name)
    if operation is None:
        raise ValidationError(f'missing operation "{name}"')
    callback = operation.callback
    if callback is None:
        callback = getattr(instance, name, None)
    if callback is None:
        raise ValidationError(f'missing operation "{name}"')
    return callback


def _operation_behavior_reference(
    model: "Model",
    name: str,
    scope: str = "",
) -> OperationCallback[typing.Any]:
    async def operation_reference(ctx: "Context", instance: "Instance", event: "Event") -> None:
        machine = getattr(instance, "_Instance__hsm", None)
        runtime_scope = scope
        if isinstance(machine, HSM):
            runtime_scope = machine._current_execution_scope() or scope
        callback = _operation_callback(model, instance, name, runtime_scope)
        result = _invoke_operation_callback(callback, ctx, instance, (event,))
        await _maybe_await(result)

    operation_reference.__name__ = name
    return operation_reference


def _operation_guard_reference(
    model: "Model",
    name: str,
    scope: str = "",
) -> Expression[typing.Any]:
    async def operation_reference(ctx: "Context", instance: "Instance", event: "Event") -> bool:
        machine = getattr(instance, "_Instance__hsm", None)
        runtime_scope = scope
        if isinstance(machine, HSM):
            runtime_scope = machine._current_execution_scope() or scope
        callback = _operation_callback(model, instance, name, runtime_scope)
        result = _invoke_operation_callback(callback, ctx, instance, (event,))
        value = await _maybe_await(result)
        return bool(value)

    operation_reference.__name__ = name
    return operation_reference


def _task_is_cancelling() -> bool:
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _current_task_or_none() -> asyncio.Task[typing.Any] | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


async def _normalize_waitable(value: typing.Any) -> None:
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
    await asyncio.sleep(duration.total_seconds())


def _next_id() -> str:
    global _next_id_counter
    _next_id_counter += 1
    return f"hsm-{_next_id_counter}"


def _qualify_model_name(model_qualified_name: str, name: str) -> str:
    if name == "":
        return ""
    if posixpath.isabs(name):
        qualified = posixpath.normpath(name)
        if IsAncestor(model_qualified_name, qualified) or qualified == model_qualified_name:
            return qualified
        return join(model_qualified_name, qualified.lstrip("/"))
    return join(model_qualified_name, name)


def _validate_slashless_name(kind: str, name: str, traceback_info: tuple[str, int] | None = None) -> None:
    if "/" not in name:
        return
    location = "" if traceback_info is None else f"{traceback_info[0]}:{traceback_info[1]}: "
    raise ValidationError(f'{location}{kind} name "{name}" cannot contain "/"')


def Match(value: str, *patterns: str) -> bool:
    if not patterns:
        return False
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


match = Match


def _to_snake_case(name: str) -> str:
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


class Context:
    def __init__(
        self,
        parent: "Context | None" = None,
        values: "collections.abc.Mapping[typing.Hashable, typing.Any] | None" = None,
    ):
        self._done = False
        self._parent = parent
        self._listeners: list[typing.Callable[[], None]] = []
        self._done_future: asyncio.Future[None] | None = None
        context_values = dict(values or {})
        if parent is None:
            context_values.setdefault(Keys.Instances, weakref.WeakSet())
        self._values = MappingProxyType(context_values)

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

    def register(self, machine: typing.Any) -> None:
        instances = self.Value(Keys.Instances)
        if isinstance(instances, weakref.WeakSet):
            instances.add(machine)

    def unregister(self, machine: typing.Any) -> None:
        instances = self.Value(Keys.Instances)
        if not isinstance(instances, weakref.WeakSet):
            return
        try:
            instances.remove(machine)
        except KeyError:
            pass

    def machines(self) -> list[typing.Any]:
        instances, ok = InstancesFromContext(self)
        return instances if ok else []

    def machine(self) -> "HSM[typing.Any] | Group | None":
        machine, ok = FromContext(self)
        return machine if ok else None

    def owner(self) -> "HSM[typing.Any] | Group | None":
        owner = self.Value(Keys.Owner)
        return owner if isinstance(owner, (HSM, Group)) else None

    def Value(self, key: typing.Hashable) -> typing.Any:
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


context = Context
context_key = ContextKey
keys = Keys


def FromContext(ctx: Context | None) -> tuple["HSM[typing.Any] | Group | None", bool]:
    if ctx is None:
        return None, False
    machine = ctx.Value(Keys.HSM)
    if isinstance(machine, (HSM, Group)):
        return machine, True
    return None, False


def InstancesFromContext(ctx: Context | None) -> tuple[list[typing.Any], bool]:
    if ctx is None:
        return [], False
    instances = ctx.Value(Keys.Instances)
    if isinstance(instances, weakref.WeakSet):
        return list(instances), True
    if isinstance(instances, collections.abc.Iterable) and not isinstance(instances, (str, bytes)):
        return list(instances), True
    return [], False


from_context = FromContext
instances_from_context = InstancesFromContext


def _WithRuntimeHSM(ctx: Context, machine: "HSM[typing.Any] | Group") -> Context:
    instances = ctx.Value(Keys.Instances)
    if not isinstance(instances, weakref.WeakSet):
        instances = weakref.WeakSet()
    return (
        ctx.WithValue(Keys.Instances, instances)
        .WithValue(Keys.Owner, ctx.Value(Keys.HSM))
        .WithValue(Keys.HSM, machine)
    )


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
class Namespace(Element):
    kind: int = Kinds.Namespace
    members: dict[str, typing.Union["Element", "Event[typing.Any]"]] = field(default_factory=dict)


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


@dataclass
class Behavior(typing.Generic[TInstance], NamedElement, Namespace):
    kind: int = Kinds.Behavior
    operation: OperationCallback[TInstance] = field(default=noop_operation)
    scope: str = field(default_factory=str)
    defer_events: bool = False
    timer_event: Event[typing.Any] | None = None
    timer_duration: Duration[typing.Any] | None = None
    timer_timepoint: Timepoint[typing.Any] | None = None
    timer_repeating: bool = False
    generated_when_event: str = ""


@dataclass
class StateMachine(Behavior[TInstance]):
    kind: int = Kinds.StateMachine


@dataclass
class Vertex(NamedElement):
    kind: int = Kinds.Vertex
    transitions: list[str] = field(default_factory=list)


@dataclass
class State(Vertex, Namespace):
    kind: int = Kinds.State
    initial: str = field(default_factory=str)
    entry: list[str] = field(default_factory=list)
    exit: list[str] = field(default_factory=list)
    activity: list[str] = field(default_factory=list)
    deferred: list[str] = field(default_factory=list)


@dataclass
class EntryPointDef(NamedElement):
    target: str = field(default_factory=str)
    effect: list[str] = field(default_factory=list)


@dataclass
class SubmachineState(State):
    kind: int = Kinds.SubmachineState
    machine: "Model | None" = None
    entry_points: dict[str, EntryPointDef] = field(default_factory=dict)
    exit_points: dict[str, str] = field(default_factory=dict)


@dataclass
class AttributeDef(NamedElement):
    kind: int = Kinds.Attribute
    declared_name: str = ""
    default: typing.Any = None
    value_type: type[typing.Any] | None = None
    dynamic: bool = False
    implicit: bool = False


@dataclass
class OperationDef(NamedElement):
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
class Model(State):
    events: dict[str, "Event[typing.Any]"] = field(default_factory=dict)
    attributes: dict[str, AttributeDef] = field(default_factory=dict)
    attribute_aliases: dict[str, str] = field(default_factory=dict)
    attribute_scope_aliases: dict[tuple[str, str], str] = field(default_factory=dict)
    operations: dict[str, OperationDef] = field(default_factory=dict)
    operation_aliases: dict[tuple[str, str], str] = field(default_factory=dict)
    operation_name_aliases: dict[str, str] = field(default_factory=dict)
    entry_points: dict[str, EntryPointDef] = field(default_factory=dict)
    exit_points: dict[str, "ExitPointVertex"] = field(default_factory=dict)
    transition_map: dict[str, dict[str, list[typing.Any]]] = field(default_factory=dict)
    snapshot_event_map: dict[str, tuple["EventSnapshot", ...]] = field(default_factory=dict)
    deferred_map: dict[str, dict[str, bool]] = field(default_factory=dict)
    deferred_owner_map: dict[str, dict[str, str]] = field(default_factory=dict)
    direct_deferred_map: dict[str, builtins.set[str]] = field(default_factory=dict)
    submachine_owner_map: dict[str, str] = field(default_factory=dict)
    timer_activity_map: dict[str, dict[str, str]] = field(default_factory=dict)
    timer_triggers: dict[tuple[str, bool, bool, int], str] = field(default_factory=dict)
    timer_event_order: dict[str, int] = field(default_factory=dict)
    pending_oncall: list[tuple[str, str]] = field(default_factory=list)
    pending_operations: list[tuple[str, str]] = field(default_factory=list)

    def add(self, partial: PartialElement) -> None:
        self.owned_elements.append(partial)

    @typing.overload
    def get(self, name: str) -> typing.Union[Element, "Event[typing.Any]", None]: ...

    @typing.overload
    def get(self, name: str, kind: typing.Type["Event[TData]"]) -> "Event[TData] | None": ...

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
            base = getattr(all_kinds[0], "__origin__", all_kinds[0])
            if base is Event:
                return self.events.get(name)
            if base is AttributeDef:
                return self.attributes.get(name)
            if base is OperationDef:
                return self.operations.get(name)
        element = self.members.get(name)
        if element is None:
            return None
        if not all_kinds:
            return element
        if len(all_kinds) == 1:
            if not isinstance(element, base):
                return None
            return element
        bases = tuple(getattr(kind_value, "__origin__", kind_value) for kind_value in all_kinds)
        if not isinstance(element, bases):
            return None
        return element

    def set(self, qualified_name: str, element: typing.Union[Element, "Event[typing.Any]"]) -> None:
        if isinstance(element, Event):
            self.events[qualified_name] = element
            existing = self.members.get(qualified_name)
            if existing is None or isinstance(existing, Event):
                self.members[qualified_name] = element
        elif isinstance(element, AttributeDef):
            self.attributes[element.declared_name or qualified_name] = element
            existing = self.members.get(qualified_name)
            if existing is None or isinstance(existing, AttributeDef):
                self.members[qualified_name] = element
        elif isinstance(element, OperationDef):
            self.operations[qualified_name] = element
            if element.declared_name:
                self.operations.setdefault(element.declared_name, element)
            existing = self.members.get(qualified_name)
            if existing is None or isinstance(existing, OperationDef):
                self.members[qualified_name] = element
        else:
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
        return MappingProxyType({
            copy.deepcopy(key): _readonly_snapshot_value(item)
            for key, item in value.items()
        })
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
    Guard: bool
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
        return self.Guard

    @property
    def schema(self) -> typing.Any:
        return self.Schema


@dataclass(frozen=True)
class Snapshot:
    ID: str = ""
    QualifiedName: str = ""
    State: str = ""
    Attributes: typing.Mapping[str, typing.Any] | None = None
    QueueLen: int = 0
    Events: tuple[EventSnapshot, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        attributes = (
            None if self.Attributes is None else _readonly_snapshot_value(self.Attributes)
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
        return self.State

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
class Pseudostate(Vertex):
    kind: int = Kinds.Pseudostate


@dataclass
class Initial(Pseudostate):
    kind: int = Kinds.Initial


@dataclass
class Choice(Pseudostate):
    kind: int = Kinds.Choice


@dataclass
class ShallowHistoryVertex(Pseudostate):
    kind: int = Kinds.ShallowHistory


@dataclass
class DeepHistoryVertex(Pseudostate):
    kind: int = Kinds.DeepHistory


@dataclass
class ExitPointVertex(Pseudostate):
    kind: int = Kinds.ExitPoint
    effect: list[str] = field(default_factory=list)
    public_name: str = field(default_factory=str)


@dataclass
class FinalState(State):
    kind: int = Kinds.FinalState


@dataclass
class TransitionPath:
    target: str = field(default_factory=str)
    enter: list[str] = field(default_factory=list)
    exit: list[str] = field(default_factory=list)
    effect_failure_state_index: int = -1
    effect_failure_state: str = field(default_factory=str)
    synchronous: bool = False


@dataclass
class Transition(NamedElement):
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
    paths: dict[str, TransitionPath] = field(default_factory=dict)


BehaviorNode = Behavior
VertexNode = Vertex
StateNode = State
SubmachineStateNode = SubmachineState
InitialNode = Initial
ChoiceNode = Choice
PseudostateNode = Pseudostate
ShallowHistoryNode = ShallowHistoryVertex
DeepHistoryNode = DeepHistoryVertex
ExitPointNode = ExitPointVertex
FinalStateNode = FinalState
TransitionNode = Transition


def transition_has_wildcard_event(transition: TransitionNode) -> bool:
    return any(event_name == AnyEvent.qualified_name for event_name in transition.events)


@dataclass
class SortTransitions(PartialElement):
    vertex: VertexNode = field(default_factory=VertexNode)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        self.vertex.transitions.sort(
            key=lambda name: (
                transition := model.get(name, TransitionNode)
            ) is not None and not transition_has_wildcard_event(transition)
        )


@dataclass
class PartialState(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> StateNode:
        _validate_slashless_name("state", self.qualified_name, self.traceback)
        namespace = find(stack, StateNode)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: state must be called within Define() or State()"
            )
        state = StateNode(qualified_name=join(namespace.qualified_name, self.qualified_name))
        model.set(state.qualified_name, state)
        apply(state, model, stack, self.owned_elements)
        model.add(SortTransitions(vertex=state, traceback=self.traceback))
        return state


def _remap_submachine_path(path: str, child_root: str, parent_root: str) -> str:
    if path == "":
        return ""
    if path == child_root:
        return parent_root
    if IsAncestor(child_root, path):
        return join(parent_root, path[len(child_root):].lstrip("/"))
    return path


def _remap_submachine_event_name(name: str, child_root: str, parent_root: str) -> str:
    if not name.startswith("@exit:"):
        return _remap_submachine_path(name, child_root, parent_root)
    source, separator, exit_name = name[len("@exit:"):].rpartition(":")
    if not separator:
        return name
    return f"@exit:{_remap_submachine_path(source, child_root, parent_root)}:{exit_name}"


def _clone_event_for_submachine(
    event: Event[typing.Any],
    child_root: str,
    parent_root: str,
    attribute_remap: dict[str, str] | None = None,
) -> Event[typing.Any]:
    attribute_remap = attribute_remap or {}
    qualified_name = attribute_remap.get(
        event.qualified_name,
        _remap_submachine_event_name(event.qualified_name, child_root, parent_root),
    )
    return Event(
        name=attribute_remap.get(event.name, _remap_submachine_event_name(event.name, child_root, parent_root)),
        data=copy.deepcopy(event.data),
        kind=event.kind,
        id=event.id,
        source=attribute_remap.get(event.source, _remap_submachine_path(event.source, child_root, parent_root)),
        target=_remap_submachine_path(event.target, child_root, parent_root),
        qualified_name=qualified_name,
        schema=copy.deepcopy(event.schema),
    )


def _clone_behavior_for_submachine(
    behavior: BehaviorNode[typing.Any],
    child_root: str,
    parent_root: str,
) -> BehaviorNode[typing.Any]:
    timer_event = (
        None
        if behavior.timer_event is None
        else _clone_event_for_submachine(behavior.timer_event, child_root, parent_root)
    )
    operation = behavior.operation
    if timer_event is not None and behavior.timer_duration is not None:
        operation = _make_timed_operation(
            timer_event,
            behavior.timer_duration,
            behavior.timer_timepoint,
            behavior.timer_repeating,
        )
    return BehaviorNode(
        kind=behavior.kind,
        qualified_name=_remap_submachine_path(behavior.qualified_name, child_root, parent_root),
        operation=operation,
        scope=_remap_submachine_path(behavior.scope or behavior.owner(), child_root, parent_root),
        defer_events=behavior.defer_events,
        timer_event=timer_event,
        timer_duration=behavior.timer_duration,
        timer_timepoint=behavior.timer_timepoint,
        timer_repeating=behavior.timer_repeating,
        generated_when_event=_remap_submachine_event_name(
            behavior.generated_when_event,
            child_root,
            parent_root,
        ) if behavior.generated_when_event else "",
    )


def _clone_guard_for_submachine(
    guard: GuardNode[typing.Any],
    child_root: str,
    parent_root: str,
) -> GuardNode[typing.Any]:
    return GuardNode(
        qualified_name=_remap_submachine_path(guard.qualified_name, child_root, parent_root),
        expression=guard.expression,
        scope=_remap_submachine_path(guard.scope or guard.owner(), child_root, parent_root),
    )


def _clone_transition_for_submachine(
    transition: TransitionNode,
    child_root: str,
    parent_root: str,
    attribute_remap: dict[str, str] | None = None,
) -> TransitionNode:
    attribute_remap = attribute_remap or {}
    return TransitionNode(
        kind=transition.kind,
        qualified_name=_remap_submachine_path(transition.qualified_name, child_root, parent_root),
        source=_remap_submachine_path(transition.source, child_root, parent_root),
        target=_remap_submachine_path(transition.target, child_root, parent_root),
        when=(
            None
            if transition.when is None
            else _remap_submachine_path(transition.when, child_root, parent_root)
        ),
        generated_when=(
            None
            if transition.generated_when is None
            else _remap_submachine_path(transition.generated_when, child_root, parent_root)
        ),
        when_attribute=(
            None
            if transition.when_attribute is None
            else attribute_remap.get(
                transition.when_attribute,
                _remap_submachine_path(transition.when_attribute, child_root, parent_root),
            )
        ),
        guard=(
            None
            if transition.guard is None
            else _remap_submachine_path(transition.guard, child_root, parent_root)
        ),
        effect=[
            _remap_submachine_path(effect_name, child_root, parent_root)
            for effect_name in transition.effect
        ],
        events=[
            attribute_remap.get(
                event_name,
                _remap_submachine_event_name(event_name, child_root, parent_root),
            )
            for event_name in transition.events
        ],
    )


def _clone_vertex_for_submachine(
    vertex: VertexNode,
    child_root: str,
    parent_root: str,
) -> VertexNode:
    qualified_name = _remap_submachine_path(vertex.qualified_name, child_root, parent_root)
    if isinstance(vertex, ExitPointNode):
        return ExitPointNode(
            kind=vertex.kind,
            qualified_name=qualified_name,
            transitions=[
                _remap_submachine_path(name, child_root, parent_root)
                for name in vertex.transitions
            ],
            effect=[
                _remap_submachine_path(name, child_root, parent_root)
                for name in vertex.effect
            ],
            public_name=vertex.public_name,
        )
    if isinstance(vertex, SubmachineStateNode):
        clone = SubmachineStateNode(
            qualified_name=qualified_name,
            machine=vertex.machine,
            initial=_remap_submachine_path(vertex.initial, child_root, parent_root),
            entry=[
                _remap_submachine_path(name, child_root, parent_root)
                for name in vertex.entry
            ],
            exit=[
                _remap_submachine_path(name, child_root, parent_root)
                for name in vertex.exit
            ],
            activity=[
                _remap_submachine_path(name, child_root, parent_root)
                for name in vertex.activity
            ],
            deferred=list(vertex.deferred),
            entry_points={
                name: EntryPointDef(
                    qualified_name=_remap_submachine_path(entry_point.qualified_name, child_root, parent_root),
                    target=_remap_submachine_path(entry_point.target, child_root, parent_root),
                    effect=[
                        _remap_submachine_path(effect_name, child_root, parent_root)
                        for effect_name in entry_point.effect
                    ],
                )
                for name, entry_point in vertex.entry_points.items()
            },
            exit_points={
                name: _remap_submachine_path(exit_point, child_root, parent_root)
                for name, exit_point in vertex.exit_points.items()
            },
        )
    elif isinstance(vertex, FinalStateNode):
        clone = FinalStateNode(qualified_name=qualified_name)
    elif isinstance(vertex, InitialNode):
        clone = InitialNode(qualified_name=qualified_name)
    elif isinstance(vertex, ChoiceNode):
        clone = ChoiceNode(qualified_name=qualified_name)
    elif isinstance(vertex, ShallowHistoryNode):
        clone = ShallowHistoryNode(qualified_name=qualified_name)
    elif isinstance(vertex, DeepHistoryNode):
        clone = DeepHistoryNode(qualified_name=qualified_name)
    elif isinstance(vertex, StateNode):
        clone = StateNode(
            qualified_name=qualified_name,
            initial=_remap_submachine_path(vertex.initial, child_root, parent_root),
            entry=[
                _remap_submachine_path(name, child_root, parent_root)
                for name in vertex.entry
            ],
            exit=[
                _remap_submachine_path(name, child_root, parent_root)
                for name in vertex.exit
            ],
            activity=[
                _remap_submachine_path(name, child_root, parent_root)
                for name in vertex.activity
            ],
            deferred=list(vertex.deferred),
        )
    else:
        clone = VertexNode(qualified_name=qualified_name)
    clone.transitions = [
        _remap_submachine_path(name, child_root, parent_root)
        for name in vertex.transitions
    ]
    return clone


def _clone_child_model_into_submachine(
    parent_model: Model,
    submachine: SubmachineStateNode,
    child_model: Model,
) -> None:
    child_root = child_model.qualified_name
    parent_root = submachine.qualified_name
    attribute_remap: dict[str, str] = {}
    for child_name, attribute in child_model.attributes.items():
        parent_attribute_name = _qualify_model_name(parent_model.qualified_name, posixpath.basename(child_name))
        parent_attribute = parent_model.attributes.get(parent_attribute_name)
        if attribute.implicit and parent_attribute is not None and not parent_attribute.implicit:
            attribute_remap[child_name] = parent_attribute_name
            attribute_remap[attribute.qualified_name] = parent_attribute_name
            if attribute.declared_name:
                attribute_remap[attribute.declared_name] = parent_attribute_name
    submachine.machine = child_model
    submachine.initial = _remap_submachine_path(child_model.initial, child_root, parent_root)

    for name, event in child_model.events.items():
        parent_model.set(
            attribute_remap.get(name, _remap_submachine_event_name(name, child_root, parent_root)),
            _clone_event_for_submachine(event, child_root, parent_root, attribute_remap),
        )

    for name, element in child_model.attributes.items():
        if name in attribute_remap or element.qualified_name in attribute_remap:
            continue
        parent_model.set(
            _remap_submachine_path(element.qualified_name, child_root, parent_root),
            AttributeDef(
                qualified_name=_remap_submachine_path(element.qualified_name, child_root, parent_root),
                declared_name=_remap_submachine_path(element.declared_name or element.qualified_name, child_root, parent_root),
                default=copy.deepcopy(element.default),
                value_type=element.value_type,
                dynamic=element.dynamic,
                implicit=element.implicit,
            ),
        )

    for name, element in child_model.members.items():
        if name == child_root:
            continue
        if isinstance(element, Event):
            continue
        if isinstance(element, AttributeDef):
            continue
        elif isinstance(element, OperationDef):
            parent_model.set(
                _remap_submachine_path(name, child_root, parent_root),
                OperationDef(
                    qualified_name=_remap_submachine_path(element.qualified_name, child_root, parent_root),
                    declared_name=element.declared_name,
                    callback=element.callback,
                ),
            )
        elif isinstance(element, BehaviorNode):
            parent_model.set(
                _remap_submachine_path(name, child_root, parent_root),
                _clone_behavior_for_submachine(element, child_root, parent_root),
            )
        elif isinstance(element, GuardNode):
            parent_model.set(
                _remap_submachine_path(name, child_root, parent_root),
                _clone_guard_for_submachine(element, child_root, parent_root),
            )
        elif isinstance(element, TransitionNode):
            cloned_transition = _clone_transition_for_submachine(element, child_root, parent_root, attribute_remap)
            parent_model.set(
                _remap_submachine_path(name, child_root, parent_root),
                cloned_transition,
            )
            if element.source == child_root:
                submachine.transitions.append(cloned_transition.qualified_name)
            parent_model.add(ResolvePaths(transition=cloned_transition))
        elif isinstance(element, VertexNode):
            cloned_vertex = _clone_vertex_for_submachine(element, child_root, parent_root)
            parent_model.set(cloned_vertex.qualified_name, cloned_vertex)
            if isinstance(cloned_vertex, ExitPointNode):
                submachine.exit_points[cloned_vertex.public_name] = cloned_vertex.qualified_name

    for name, entry_point in child_model.entry_points.items():
        submachine.entry_points[name] = EntryPointDef(
            qualified_name=_remap_submachine_path(entry_point.qualified_name, child_root, parent_root),
            target=_remap_submachine_path(entry_point.target, child_root, parent_root),
            effect=[
                _remap_submachine_path(effect_name, child_root, parent_root)
                for effect_name in entry_point.effect
            ],
        )

    for name, exit_point in child_model.exit_points.items():
        submachine.exit_points[name] = _remap_submachine_path(
            exit_point.qualified_name,
            child_root,
            parent_root,
        )


@dataclass
class PartialSubmachineState(PartialElement):
    machine: Model | None = None

    def apply(self, model: Model, stack: list[NamedElement]) -> StateNode:
        _validate_slashless_name("submachine state", self.qualified_name, self.traceback)
        namespace = find(stack, StateNode)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: SubmachineState must be called within Define() or State()"
            )
        if self.machine is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: SubmachineState requires a model"
            )
        state = SubmachineStateNode(
            qualified_name=join(namespace.qualified_name, self.qualified_name),
            machine=self.machine,
        )
        model.set(state.qualified_name, state)
        _clone_child_model_into_submachine(model, state, self.machine)
        apply(state, model, stack, self.owned_elements)
        if any(
            isinstance(partial, (PartialState, PartialInitial, PartialFinal, PartialChoice, PartialHistory))
            for partial in self.owned_elements
        ):
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: SubmachineState cannot contain nested states, initial, final, or pseudostates"
            )
        model.add(SortTransitions(vertex=state, traceback=self.traceback))
        return state


@dataclass
class PartialInitial(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> InitialNode:
        state = find(stack, StateNode)
        if state is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: initial must be called within a State"
            )
        initial = InitialNode(qualified_name=join(state.qualified_name, self.qualified_name))
        model.set(initial.qualified_name, initial)
        if state.initial:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: State {state.qualified_name} already has an initial state {state.initial}"
            )
        state.initial = initial.qualified_name
        initial_transition = TransitionNode(
            source=initial.qualified_name,
            qualified_name=join(initial.qualified_name, "initial"),
        )
        model.set(initial_transition.qualified_name, initial_transition)
        scoped_stack = [*stack, initial, initial_transition]
        initial_transition.events.append(InitialEvent.qualified_name)
        model.set(InitialEvent.qualified_name, InitialEvent)
        for partial in self.owned_elements:
            if isinstance(partial, PartialElement):
                partial.apply(model, scoped_stack)
        if initial_transition.guard is not None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Initial transition {initial_transition.qualified_name} cannot have a guard"
            )
        if not initial_transition.target:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: initial state is required for state machine"
            )
        if not is_ancestor(state.qualified_name, initial_transition.target) and state.qualified_name != _parent_path(initial_transition.target):
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Initial transition {initial_transition.qualified_name} must target a nested state not {initial_transition.target}"
            )
        initial.transitions.append(initial_transition.qualified_name)
        model.add(ResolvePaths(transition=initial_transition, traceback=self.traceback))
        return initial


@dataclass
class PartialHistory(PartialElement):
    history_type: typing.Type[PseudostateNode] = ShallowHistoryNode

    def apply(self, model: Model, stack: list[NamedElement]) -> PseudostateNode:
        history_name = self.history_type.__name__.replace("Vertex", "")
        _validate_slashless_name(history_name, self.qualified_name, self.traceback)
        owner_state = find(stack, StateNode)
        if owner_state is None or owner_state is model:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: you must call {history_name}() within a nested State"
            )
        if not self.owned_elements:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: {history_name} requires a default transition"
            )
        history = self.history_type(
            qualified_name=join(owner_state.qualified_name, self.qualified_name)
        )
        model.set(history.qualified_name, history)
        if self.owned_elements:
            default_transition = TransitionNode(
                source=history.qualified_name,
                qualified_name=join(history.qualified_name, "default"),
            )
            model.set(default_transition.qualified_name, default_transition)
            apply(default_transition, model, [*stack, history], self.owned_elements)
            history.transitions.append(default_transition.qualified_name)
            model.add(ResolvePaths(transition=default_transition, traceback=self.traceback))
        return history


@dataclass
class ResolvePaths(PartialElement):
    transition: TransitionNode = field(default_factory=TransitionNode)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        if self.transition.kind == Kinds.Internal:
            for name in model.members:
                if name.startswith(self.transition.source):
                    self.transition.paths[name] = TransitionPath(
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
        source_element = model.get(self.transition.source, VertexNode)
        if isinstance(source_element, InitialNode):
            self.transition.paths[_parent_path(self.transition.source)] = TransitionPath(
                target=self.transition.target,
                enter=enter,
                exit=[],
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
            if not isinstance(element, (StateMachine, VertexNode)):
                continue
            if not qualified_name.startswith(self.transition.source):
                continue
            exit_path: list[str] = []
            if self.transition.kind != Kinds.Internal:
                exiting = qualified_name
                while exiting not in ("", lca):
                    exit_path.append(exiting)
                    exiting = _parent_path(exiting)
            self.transition.paths[qualified_name] = TransitionPath(
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
    return posixpath.commonpath([parent_abs]) == posixpath.commonpath([parent_abs, child_abs])


@dataclass
class ValidateVertex(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        if model.get(self.qualified_name, VertexNode) is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Vertex "{self.qualified_name}" not found'
            )


@dataclass
class PartialTransition(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionNode:
        vertex = find(stack, VertexNode)
        if vertex is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: transition must be called within a State() or Define()"
            )
        name = self.qualified_name or f"transition_{len(model.members)}"
        transition = TransitionNode(qualified_name=join(vertex.qualified_name, name), source=".")
        model.set(transition.qualified_name, transition)
        apply(transition, model, stack, self.owned_elements)
        if transition.source in ("", "."):
            transition.source = vertex.qualified_name
        source_element = model.get(transition.source, VertexNode)
        if source_element is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Source "{transition.source}" not found for transition "{transition.qualified_name}"'
            )
        source_element.transitions.append(transition.qualified_name)
        if (
            not transition.events
            and transition.when is None
            and not isinstance(source_element, PseudostateNode)
        ):
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Transition "{transition.qualified_name}" has no events'
            )
        classification_target = transition.target
        entry_point_target = _split_entry_point_target(transition.target)
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
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionNode:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: hsm.Source() must be called within a hsm.Transition()"
            )
        if transition.source not in ("", "."):
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Transition "{transition.qualified_name}" already has a source "{transition.source}"'
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
            self.qualified_name = source.qualified_name
        elif not posixpath.isabs(self.qualified_name):
            state = find(stack, StateNode)
            if state is not None and not self.qualified_name.startswith(state.qualified_name):
                self.qualified_name = join(state.qualified_name, self.qualified_name)
            model.add(ValidateVertex(qualified_name=self.qualified_name, traceback=self.traceback))
        elif not is_path_in_path(self.qualified_name, model.qualified_name):
            self.qualified_name = join(model.qualified_name, self.qualified_name[1:])
        transition.source = self.qualified_name
        return transition


@dataclass
class PartialTarget(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionNode:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Target() must be called within Transition()"
            )
        if transition.target:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Transition "{transition.qualified_name}" already has a target "{transition.target}"'
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
            self.qualified_name = target.qualified_name
        elif not posixpath.isabs(self.qualified_name):
            state = find(stack, StateNode)
            if state is not None and not self.qualified_name.startswith(state.qualified_name):
                self.qualified_name = join(state.qualified_name, self.qualified_name)
            model.add(ValidateVertex(qualified_name=self.qualified_name, traceback=self.traceback))
        elif not is_path_in_path(self.qualified_name, model.qualified_name):
            self.qualified_name = join(model.qualified_name, self.qualified_name[1:])
        transition.target = self.qualified_name
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
                model.pending_operations.append((operation, element.qualified_name))
                callback = _operation_behavior_reference(model, operation, element.qualified_name)
                operation_name = operation
            else:
                callback = operation
                operation_name = getattr(operation, "__name__", "anonymous")
            behavior = BehaviorNode(
                qualified_name=join(
                    element.qualified_name,
                    self.qualified_name,
                    operation_name,
                    str(len(behaviors)),
                ),
                operation=callback,
                kind=Kinds.Concurrent if self.concurrent else Kinds.Sequential,
                scope=element.qualified_name,
                defer_events=self.concurrent,
            )
            behaviors.append(behavior.qualified_name)
            model.set(behavior.qualified_name, behavior)
        return element


async def noop_expression(ctx: Context, instance: "Instance", event: Event) -> bool:
    return True


@dataclass
class Guard(typing.Generic[TInstance], NamedElement):
    kind: int = Kinds.Constraint
    expression: Expression[TInstance] = field(default=noop_expression)
    scope: str = field(default_factory=str)


GuardNode = Guard


@dataclass
class PartialGuard(typing.Generic[TInstance], PartialElement):
    expression: ExpressionArgument[TInstance] = field(default=noop_expression)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: guard must be called within a Transition"
            )
        expression = self.expression
        source = find(stack, VertexNode)
        if isinstance(expression, str):
            scope = "" if source is None else source.qualified_name
            model.pending_operations.append((expression, scope))
            expression = _operation_guard_reference(model, expression, scope)
        guard = GuardNode(
            qualified_name=join(transition.qualified_name, self.qualified_name),
            expression=expression,
            scope="" if source is None else source.qualified_name,
        )
        model.set(guard.qualified_name, guard)
        transition.guard = guard.qualified_name


@dataclass
class PartialTrigger(PartialElement):
    events: list[Event] = field(default_factory=list)

    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionNode:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: trigger must be called within a Transition"
            )
        for event in self.events:
            model.set(event.qualified_name, event)
            transition.events.append(event.qualified_name)
        return transition


@dataclass
class PartialDefer(PartialElement):
    events: list[Event] = field(default_factory=list)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        state = find(stack, StateNode)
        if state is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: defer must be called within a state"
            )
        for event in self.events:
            model.set(event.qualified_name, event)
            state.deferred.append(event.qualified_name)


@dataclass
class PartialChoice(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> ChoiceNode:
        state_or_transition = find(stack, StateNode, TransitionNode)
        if state_or_transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: choice must be called within a state or transition"
            )
        if isinstance(state_or_transition, TransitionNode):
            source_name = state_or_transition.source
            if source_name in ("", "."):
                source_vertex = find(stack, VertexNode)
                if source_vertex is None:
                    raise ValidationError(
                        f"{self.traceback[0]}:{self.traceback[1]}: choice must be called within a state"
                    )
                source_name = source_vertex.qualified_name
            if isinstance(model.get(source_name, PseudostateNode), PseudostateNode):
                state_or_transition = find(stack, StateNode)
                if state_or_transition is None:
                    raise ValidationError(
                        f"{self.traceback[0]}:{self.traceback[1]}: choice must be called within a state"
                    )
        qualified_name = join(state_or_transition.qualified_name, self.qualified_name or f"choice_{len(model.members)}")
        choice = ChoiceNode(qualified_name=qualified_name)
        model.set(choice.qualified_name, choice)
        apply(choice, model, stack, self.owned_elements)
        if not choice.transitions:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: choice \"{choice.qualified_name}\" has no transitions"
            )
        default_transition = model.get(choice.transitions[-1], TransitionNode)
        if default_transition is not None and default_transition.guard is not None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: the last transition of choice state "{choice.qualified_name}" cannot have a guard'
            )
        return choice


@dataclass
class ValidateFinalState(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        final_state = model.get(self.qualified_name, FinalStateNode)
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
        namespace = find(stack, StateNode)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Final must be called within a namespace"
            )
        final_state = FinalStateNode(
            qualified_name=join(namespace.qualified_name, self.qualified_name)
        )
        model.set(final_state.qualified_name, final_state)
        model.add(ValidateFinalState(qualified_name=final_state.qualified_name))


@dataclass
class PartialEntryPoint(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> NamedElement:
        _validate_slashless_name("entry point", self.qualified_name, self.traceback)
        transition = find(stack, TransitionNode)
        if transition is not None and not self.owned_elements:
            if _split_entry_point_target(transition.target) is not None:
                raise ValidationError(
                    f'{self.traceback[0]}:{self.traceback[1]}: Transition "{transition.qualified_name}" already has an entry point target "{transition.target}"'
                )
            boundary = transition.target or "."
            transition.target = join(boundary, ".entry", self.qualified_name)
            return transition
        namespace = find(stack, Model)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: EntryPoint must be declared within Define() or used within Transition()"
            )
        entry_point = EntryPointDef(
            qualified_name=join(namespace.qualified_name, ".entry", self.qualified_name)
        )
        synthetic = TransitionNode(
            source=entry_point.qualified_name,
            qualified_name=join(entry_point.qualified_name, "entry"),
        )
        model.set(synthetic.qualified_name, synthetic)
        apply(synthetic, model, [*stack, entry_point, synthetic], self.owned_elements)
        if not synthetic.target:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: EntryPoint {self.qualified_name} requires a target"
            )
        entry_point.target = synthetic.target
        entry_point.effect = list(synthetic.effect)
        namespace.entry_points[self.qualified_name] = entry_point
        model.set(entry_point.qualified_name, entry_point)
        return entry_point


@dataclass
class PartialExitPoint(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> NamedElement:
        _validate_slashless_name("exit point", self.qualified_name, self.traceback)
        transition = find(stack, TransitionNode)
        source_state = find(stack, StateNode)
        if transition is not None and not self.owned_elements:
            source_name = transition.source
            if source_name in ("", "."):
                source_name = "" if source_state is None else source_state.qualified_name
            if not source_name:
                raise ValidationError(
                    f"{self.traceback[0]}:{self.traceback[1]}: ExitPoint outcome must be used within a state transition"
                )
            event = Event(
                name=_exit_point_event_name(source_name, self.qualified_name),
                qualified_name=_exit_point_event_name(source_name, self.qualified_name),
                kind=Kinds.CompletionEvent,
            )
            model.set(event.qualified_name, event)
            transition.events.append(event.qualified_name)
            return transition
        namespace = find(stack, Model)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: ExitPoint must be declared within Define() or used within Transition()"
            )
        exit_point = ExitPointNode(
            qualified_name=join(namespace.qualified_name, self.qualified_name),
            public_name=self.qualified_name,
        )
        model.set(exit_point.qualified_name, exit_point)
        synthetic = TransitionNode(
            source=exit_point.qualified_name,
            qualified_name=join(exit_point.qualified_name, "exit"),
        )
        model.set(synthetic.qualified_name, synthetic)
        apply(synthetic, model, [*stack, exit_point, synthetic], self.owned_elements)
        exit_point.effect = list(synthetic.effect)
        namespace.exit_points[self.qualified_name] = exit_point
        return exit_point


@dataclass
class PartialAttribute(PartialElement):
    default: typing.Any = None
    value_type: type[typing.Any] | None = None
    dynamic: bool = False
    implicit: bool = False

    def apply(self, model: Model, stack: list[NamedElement]) -> AttributeDef:
        _validate_slashless_name("attribute", self.qualified_name, self.traceback)
        if self.qualified_name == "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: attribute name cannot be empty"
            )
        qualified_name = _qualify_model_name(model.qualified_name, self.qualified_name)
        if qualified_name in model.attributes and not self.implicit:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: duplicate attribute {self.qualified_name}"
            )
        existing = model.attributes.get(qualified_name)
        if existing is not None:
            return existing
        attribute = AttributeDef(
            qualified_name=qualified_name,
            declared_name=qualified_name,
            default=self.default,
            value_type=self.value_type,
            dynamic=self.dynamic,
            implicit=self.implicit,
        )
        model.set(attribute.qualified_name, attribute)
        return attribute


@dataclass
class PartialOperationDeclaration(PartialElement):
    callback: OperationImplementation | None = None

    def apply(self, model: Model, stack: list[NamedElement]) -> OperationDef:
        _validate_slashless_name("operation", self.qualified_name, self.traceback)
        if self.qualified_name == "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: operation name cannot be empty"
            )
        if self.qualified_name in model.operations:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: duplicate operation {self.qualified_name}"
            )
        namespace = find(stack, StateNode)
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: operation must be called within Define() or State()"
            )
        operation = OperationDef(
            qualified_name=join(namespace.qualified_name, f".operation.{self.qualified_name}"),
            declared_name=self.qualified_name,
            callback=self.callback,
        )
        model.set(operation.qualified_name, operation)
        return operation


def _oncall_event_name(name: str) -> str:
    return f"@call:{name}"


def _exit_point_event_name(submachine_state: str, name: str) -> str:
    return f"@exit:{submachine_state}:{name}"


def _exit_point_event_source(event_name: str) -> str:
    if not event_name.startswith("@exit:"):
        return ""
    source, separator, _exit_name = event_name[len("@exit:"):].rpartition(":")
    return source if separator else ""


def _exit_point_event_name_for_source(event_name: str, source_name: str) -> str:
    if not event_name.startswith("@exit:"):
        return event_name
    source, separator, exit_name = event_name[len("@exit:"):].rpartition(":")
    if not separator:
        return event_name
    if source == source_name or IsAncestor(source_name, source):
        return _exit_point_event_name(source_name, exit_name)
    return event_name


@dataclass
class PartialOnSet(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionNode:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: OnSet() must be called within a Transition"
            )
        if self.qualified_name == "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: OnSet() requires a non-empty attribute name"
            )
        attribute = PartialAttribute(qualified_name=self.qualified_name, implicit=True).apply(model, stack)
        event = Event(
            name=attribute.declared_name,
            qualified_name=attribute.declared_name,
            kind=Kinds.ChangeEvent,
            source=attribute.declared_name,
            schema=AttributeChange,
        )
        model.set(event.qualified_name, event)
        transition.events.append(event.qualified_name)
        return transition


@dataclass
class PartialOnCall(PartialElement):
    def apply(self, model: Model, stack: list[NamedElement]) -> TransitionNode:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: OnCall() must be called within a Transition"
            )
        if self.qualified_name == "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: OnCall() requires a non-empty operation name"
            )
        source = find(stack, VertexNode)
        scope = "" if source is None else source.qualified_name
        model.pending_oncall.append((self.qualified_name, scope))
        event = Event(
            name=_oncall_event_name(self.qualified_name),
            qualified_name=_oncall_event_name(self.qualified_name),
            kind=Kinds.CallEvent,
            schema=CallData,
        )
        model.set(event.qualified_name, event)
        transition.events.append(event.qualified_name)
        return transition


async def noop_duration(ctx: Context, instance: "Instance", event: Event) -> timedelta:
    return timedelta(seconds=0)


async def noop_timepoint(ctx: Context, instance: "Instance", event: Event) -> datetime:
    return datetime.now()


def _make_timed_operation(
    timer_event: Event[typing.Any],
    duration: Duration[typing.Any],
    timepoint: Timepoint[typing.Any] | None,
    repeating: bool,
) -> OperationCallback[typing.Any]:
    async def operation(ctx: Context, instance: Instance, event: Event) -> None:
        while not ctx.is_done():
            if timepoint is None:
                delta = await _maybe_await(duration(ctx, instance, event))
            else:
                target = await _maybe_await(timepoint(ctx, instance, event))
                if not isinstance(target, datetime):
                    raise TypeError("At() timepoint must return datetime")
                now = (
                    datetime.now(target.tzinfo)
                    if target.tzinfo is not None
                    else datetime.now()
                )
                delta = target - now
            if not isinstance(delta, timedelta):
                raise TypeError("After()/Every() duration must return timedelta")
            if delta.total_seconds() <= 0:
                if not repeating:
                    await instance.dispatch(timer_event)
                return
            machine = getattr(instance, "_Instance__hsm", None)
            if (
                isinstance(machine, HSM)
                and asyncio.current_task() in machine._active_timer_tasks
                and machine._queue_len() > 0
            ):
                if machine._processing.try_acquire():
                    await machine._process()
                    if ctx.is_done():
                        return
                else:
                    machine._timer_task_pending_dispatch = True
                    return
            try:
                await _clock_for_instance(instance).Sleep(delta)
            except asyncio.CancelledError:
                if _task_is_cancelling() or ctx.is_done():
                    return
                raise
            if ctx.is_done():
                return
            dispatched = instance.dispatch(timer_event)
            if not repeating:
                return
            await dispatched

    return operation


@dataclass
class TimedBehavior(typing.Generic[TInstance], PartialElement):
    event: Event[typing.Any] = field(default_factory=Event)
    duration: Duration[TInstance] = field(default=noop_duration)
    timepoint: Timepoint[TInstance] | None = None
    transition: TransitionNode = field(default_factory=TransitionNode)
    repeating: bool = False

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        source = model.get(self.transition.source, StateNode)
        if source is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Source "{self.transition.source}" not found for transition "{self.transition.qualified_name}"'
            )
        self.event.source = source.qualified_name

        behavior = BehaviorNode(
            qualified_name=join(source.qualified_name, self.event.name, str(len(model.members))),
            kind=Kinds.Concurrent,
            operation=_make_timed_operation(self.event, self.duration, self.timepoint, self.repeating),
            scope=source.qualified_name,
            defer_events=True,
            timer_event=self.event,
            timer_duration=self.duration,
            timer_timepoint=self.timepoint,
            timer_repeating=self.repeating,
        )
        source.activity.append(behavior.qualified_name)
        model.set(behavior.qualified_name, behavior)


@dataclass
class PartialAfter(typing.Generic[TInstance], PartialElement):
    duration: Duration[TInstance] = field(default=noop_duration)
    timepoint: Timepoint[TInstance] | None = None
    repeating: bool = False

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: after must be called within a Transition"
            )
        trigger = self.timepoint or self.duration
        trigger_key = (
            transition.source,
            self.repeating,
            self.timepoint is not None,
            builtins.id(trigger),
        )
        existing_event_name = model.timer_triggers.get(trigger_key)
        if existing_event_name is not None:
            transition.events.append(existing_event_name)
            event = model.get(existing_event_name, Event)
            if event is None:
                raise ValidationError(f'Timer event "{existing_event_name}" not found')
        else:
            qualified_name = join(
                transition.qualified_name,
                getattr(trigger, "__name__", "duration"),
                str(len(model.members)),
            )
            event = Event(
                name=qualified_name,
                qualified_name=qualified_name,
                kind=Kinds.TimeEvent,
                source=transition.source,
            )
            model.set(event.qualified_name, event)
            model.timer_triggers[trigger_key] = event.qualified_name
            transition.events.append(event.qualified_name)
        model.add(
            TimedBehavior(
                event=event,
                transition=transition,
                duration=self.duration,
                timepoint=self.timepoint,
                repeating=self.repeating,
                traceback=self.traceback,
            )
        )


@dataclass
class PartialWhen(PartialOnSet, typing.Generic[TInstance]):
    expression: WhenExpression[TInstance] | None = None
    attribute: str | None = None

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: when must be called within a Transition"
            )
        source = None
        if transition.source not in ("", "."):
            source = model.get(transition.source, StateNode)
        if source is None and transition.source in ("", "."):
            source = find(stack, StateNode)
        if source is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: when can only be used on transitions where the source is a State"
            )
        if self.attribute is not None:
            attribute = PartialAttribute(qualified_name=self.attribute, implicit=True).apply(model, stack)
            event = Event(
                name=attribute.declared_name,
                qualified_name=attribute.declared_name,
                kind=Kinds.ChangeEvent,
                source=attribute.declared_name,
                schema=AttributeChange,
            )
            model.set(event.qualified_name, event)
            transition.events.append(event.qualified_name)

            def expression(ctx: Context, instance: TInstance, event: Event) -> bool:
                change = event.data
                return isinstance(change, AttributeChange) and bool(change.value)

            qualified_name = join(
                transition.qualified_name,
                f"when_{self.attribute}",
                str(len(model.members)),
            )
            guard = GuardNode(
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
        qualified_name = join(
            transition.qualified_name,
            getattr(self.expression, "__name__", "when"),
            str(len(model.members)),
        )
        guard = GuardNode(
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
        for waiters in (self.dispatch, self.process, self.entry, self.exit, self.executed):
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


def _attribute_value_type(attribute: AttributeDef) -> type[typing.Any] | None:
    if attribute.dynamic:
        return None
    if attribute.value_type is not None:
        return attribute.value_type
    if attribute.default is not None:
        return type(attribute.default)
    return None


def _attribute_accepts_value(attribute: AttributeDef, value: typing.Any) -> bool:
    value_type = _attribute_value_type(attribute)
    if value_type is None:
        return True
    return type(value) is value_type


def _segments_between(owner: str, target: str) -> list[str]:
    if owner == target:
        return []
    current = target
    segments: list[str] = []
    while current not in ("", owner):
        segments.insert(0, current)
        current = _parent_path(current)
    return segments


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


class Queue:
    def __init__(
        self,
        fifo: "Queue | None" = None,
        *,
        Push: typing.Callable[[Event], BaseException | None] | None = None,
        Pop: typing.Callable[[], Event | BaseException | None] | None = None,
        Len: typing.Callable[[], int | BaseException] | None = None,
        push: typing.Callable[[Event], BaseException | None] | None = None,
        pop: typing.Callable[[], Event | BaseException | None] | None = None,
        len: typing.Callable[[], int | BaseException] | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._fifo = self._make_fifo(fifo, Push, Pop, Len, push, pop, len)
        self._completion_events: collections.deque[Event] = collections.deque()
        self._regular_events: collections.deque[Event] = collections.deque()
        self._time_events: collections.deque[Event] = collections.deque()

    @staticmethod
    def _make_fifo(
        fifo: "Queue | None",
        Push: typing.Callable[[Event], BaseException | None] | None,
        Pop: typing.Callable[[], Event | BaseException | None] | None,
        Len: typing.Callable[[], int | BaseException] | None,
        push: typing.Callable[[Event], BaseException | None] | None,
        pop: typing.Callable[[], Event | BaseException | None] | None,
        len: typing.Callable[[], int | BaseException] | None,
    ) -> typing.Any:
        canonical = (Push, Pop, Len)
        native = (push, pop, len)
        has_canonical = any(hook is not None for hook in canonical)
        has_native = any(hook is not None for hook in native)
        if fifo is not None and (has_canonical or has_native):
            raise TypeError("Queue accepts either fifo or Push/Pop/Len hooks, not both")
        if has_canonical and has_native:
            raise TypeError("Queue accepts either Push/Pop/Len or push/pop/len hooks, not both")
        if has_canonical:
            if not all(hook is not None for hook in canonical):
                raise TypeError("Queue requires complete Push/Pop/Len or push/pop/len hooks")
            return SimpleNamespace(push=Push, pop=Pop, len=Len)
        if has_native:
            if not all(hook is not None for hook in native):
                raise TypeError("Queue requires complete Push/Pop/Len or push/pop/len hooks")
            return SimpleNamespace(push=push, pop=pop, len=len)
        if fifo is not None:
            missing = [
                name for name in ("push", "pop", "len")
                if not callable(getattr(fifo, name, None))
            ]
            if missing:
                raise TypeError("Queue requires complete Push/Pop/Len or push/pop/len hooks")
        return fifo

    def push(self, event: Event[typing.Any]) -> BaseException | None:
        if is_kind(event.kind, Kinds.CompletionEvent):
            with self._lock:
                self._completion_events.appendleft(event)
            return None
        if self._fifo is not None:
            try:
                maybe_error = self._fifo.push(event)
            except BaseException as error:
                return error
            if inspect.isawaitable(maybe_error):
                _close_awaitable(maybe_error)
                return TypeError("Queue Push must return synchronously")
            return maybe_error if isinstance(maybe_error, BaseException) else None
        with self._lock:
            if event.kind == Kinds.TimeEvent:
                order = getattr(event, "_hsm_time_order", None)
                if isinstance(order, int):
                    for index, queued in enumerate(self._time_events):
                        queued_order = getattr(queued, "_hsm_time_order", None)
                        if not isinstance(queued_order, int) or order < queued_order:
                            self._time_events.insert(index, event)
                            break
                    else:
                        self._time_events.append(event)
                else:
                    self._time_events.append(event)
            else:
                self._regular_events.append(event)
        return None

    def pop(self) -> Event | BaseException | None:
        with self._lock:
            if self._completion_events:
                return self._completion_events.popleft()
        if self._fifo is not None:
            try:
                maybe_event = self._fifo.pop()
            except BaseException as error:
                return error
            if inspect.isawaitable(maybe_event):
                _close_awaitable(maybe_event)
                return TypeError("Queue Pop must return synchronously")
            return maybe_event
        with self._lock:
            if self._regular_events:
                return self._regular_events.popleft()
            if self._time_events:
                return self._time_events.popleft()
            return None

    def len(self) -> int | BaseException:
        with self._lock:
            completion_len = len(self._completion_events)
            regular_len = 0 if self._fifo is not None else len(self._regular_events) + len(self._time_events)
        if self._fifo is not None:
            try:
                maybe_len = self._fifo.len()
            except BaseException as error:
                return error
            if inspect.isawaitable(maybe_len):
                _close_awaitable(maybe_len)
                return TypeError("Queue Len must return synchronously")
            if isinstance(maybe_len, BaseException):
                return maybe_len
            regular_len = maybe_len
        return completion_len + regular_len


@dataclass
class ActiveBehavior:
    context: Context
    task: asyncio.Task[None]


class _AsyncRequired(Exception):
    pass


class _AbortTransition(Exception):
    def __init__(self, error: BaseException):
        super().__init__(str(error))
        self.error = error


def _clock_for_instance(instance: typing.Any) -> Clock:
    machine = getattr(instance, "_Instance__hsm", None)
    if isinstance(machine, HSM):
        return machine.clock()
    return DefaultClock.with_defaults()


class Instance:
    __hsm: typing.Optional["HSM[typing.Self]"] = None

    def dispatch(self, event: Event) -> typing.Awaitable[None]:
        if self.__hsm is None:
            raise ValidationError("operation requires a started HSM")
        return self.__hsm.dispatch(event)

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

    def stop(self) -> typing.Awaitable[None]:
        if self.__hsm is None:
            return _completed_none()
        return self.__hsm.stop()

    def restart(self, data: typing.Any = None) -> typing.Awaitable[None]:
        if self.__hsm is None:
            raise ValidationError("operation requires a started HSM")
        return self.__hsm.restart(data)

    Dispatch = dispatch
    State = state
    Context = context
    Clock = clock
    Get = get
    Set = set
    Stop = stop
    Restart = restart


class HSM(Behavior[TInstance]):
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
            qualified_name=config.Name or model.qualified_name,
        )
        self.model = model
        self._instance = instance
        self._config = config
        self._processing = Mutex()
        self._queue = Queue(config.Queue) if config.Queue is not None else Queue()
        self._deferred_events: list[Event] = []
        self._deferred_queued: list[bool] = []
        self._active: dict[str, ActiveBehavior] = {}
        self._active_tasks: builtins.set[asyncio.Task[None]] = builtins.set()
        self._active_timer_tasks: builtins.set[asyncio.Task[None]] = builtins.set()
        self._timer_task_pending_dispatch = False
        self._after = _AfterWaiters()
        self._state: VertexNode = model
        self._awaitable: typing.Awaitable[None] = _future_done()
        self._attributes = _default_attribute_values(model)
        self._history_shallow: dict[str, str] = {}
        self._history_deep: dict[str, str] = {}
        self._id = config.ID or _next_id()
        self._qualified_name = config.Name or model.qualified_name
        base_context = ctx or Context()
        self._base_context = base_context
        self._root_context = _WithRuntimeHSM(base_context, self)
        self._clock = (config.Clock or DefaultClock).with_defaults()
        self._started = False
        self._stopping = False
        self._stop_requested = False
        self._restart_requested: tuple[typing.Any] | None = None
        self._execution_scope: str | None = None
        self._root_context.register(self)
        setattr(self._instance, "_Instance__hsm", self)

        async def operation(ctx: Context, inst: TInstance, event: Event) -> None:
            self._state = await self._enter(self.model, event, True)
            startup_deferred: list[Event] = []
            try:
                startup_deferred_queued: list[bool] = []
                await self._drain_queue(startup_deferred, startup_deferred_queued)
            finally:
                self._deferred_events.extend(startup_deferred)
                self._deferred_queued.extend(startup_deferred_queued)

        self.operation = operation

    def state(self) -> str:
        return self._state.qualified_name

    def context(self) -> Context:
        return self._root_context

    def id(self) -> str:
        return self._id

    def qualified_name(self) -> str:
        return self._qualified_name

    def clock(self) -> Clock:
        return self._clock

    def _current_execution_scope(self) -> str | None:
        return _execution_scopes.get().get(builtins.id(self))

    def _set_execution_scope(self, scope: str) -> contextvars.Token[dict[int, str]]:
        scopes = dict(_execution_scopes.get())
        scopes[builtins.id(self)] = scope
        self._execution_scope = scope
        return _execution_scopes.set(scopes)

    def _reset_execution_scope(self, token: contextvars.Token[dict[int, str]]) -> None:
        _execution_scopes.reset(token)
        self._execution_scope = self._current_execution_scope()

    def _ensure_accepting_events(self) -> None:
        if not self._started and not self._processing.locked():
            raise ValidationError("operation requires a started HSM")

    async def _start(self, data: typing.Any = None) -> None:
        await self._processing.acquire()
        try:
            await self._start_locked(data)
        except BaseException as error:
            await self._cleanup_failed_start(reset_state=isinstance(error, asyncio.CancelledError))
            raise
        finally:
            self._processing.release()

    async def _start_locked(self, data: typing.Any = None) -> None:
        initial_event = InitialEvent.WithData(data) if data is not None else InitialEvent
        await self._execute(self, initial_event)
        self._started = True

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
        self._root_context.unregister(self)
        self._root_context.cancel()
        self._queue = Queue(self._config.Queue) if self._config.Queue is not None else Queue()
        self._deferred_events.clear()
        self._deferred_queued.clear()
        self._active.clear()
        self._active_tasks.clear()
        self._active_timer_tasks.clear()
        self._timer_task_pending_dispatch = False
        self._attributes = _default_attribute_values(self.model)
        self._history_shallow.clear()
        self._history_deep.clear()
        if reset_state:
            self._state = self.model
        self._started = False
        self._stopping = False
        self._stop_requested = False
        self._restart_requested = None
        self._execution_scope = None
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

    def _queue_final_completion(self, vertex: VertexNode) -> None:
        if isinstance(vertex, FinalStateNode):
            self._queue_push(Event(name=FinalEvent.name, kind=Kinds.CompletionEvent))

    async def _follow_from_owner(self, owner_name: str, target_name: str, event: Event) -> VertexNode:
        current: VertexNode = self.model.get(owner_name, VertexNode) or self.model
        segments = _segments_between(owner_name, target_name)
        if not segments:
            target = self.model.get(target_name, VertexNode)
            if target is None:
                return current
            return await self._enter(target, event, True)
        for index, qualified_name in enumerate(segments):
            vertex = self.model.get(qualified_name, VertexNode)
            if vertex is None:
                continue
            current = await self._enter(vertex, event, index == len(segments) - 1)
        return current

    def _follow_from_owner_sync(self, owner_name: str, target_name: str, event: Event) -> VertexNode:
        current: VertexNode = self.model.get(owner_name, VertexNode) or self.model
        segments = _segments_between(owner_name, target_name)
        if not segments:
            target = self.model.get(target_name, VertexNode)
            if target is None:
                return current
            return self._enter_sync(target, event, True)
        for index, qualified_name in enumerate(segments):
            vertex = self.model.get(qualified_name, VertexNode)
            if vertex is None:
                continue
            current = self._enter_sync(vertex, event, index == len(segments) - 1)
        return current

    async def _enter(
        self,
        vertex: VertexNode,
        event: Event,
        default_entry: bool,
        preserved_activities: builtins.set[str] | None = None,
    ) -> VertexNode:
        if isinstance(vertex, ExitPointNode):
            owner_vertex = self.model.get(vertex.owner(), VertexNode)
            restore_state = self._state.qualified_name
            if isinstance(owner_vertex, SubmachineStateNode):
                self._state = owner_vertex
            for effect_name in vertex.effect:
                effect = self.model.get(effect_name, BehaviorNode[TInstance])
                if effect is not None:
                    await self._execute(effect, event)
            if isinstance(owner_vertex, SubmachineStateNode):
                event_name = _exit_point_event_name(owner_vertex.qualified_name, vertex.public_name)
                exit_event = Event(
                    name=event_name,
                    qualified_name=event_name,
                    kind=Kinds.CompletionEvent,
                    data=copy.deepcopy(event.data),
                    schema=event.schema,
                )
                setattr(exit_event, "_hsm_exit_point_restore_state", restore_state)
                setattr(exit_event, "_hsm_exit_point_name", vertex.public_name)
                self._queue_push(exit_event)
                return owner_vertex
            return owner_vertex if owner_vertex is not None else self._state
        if isinstance(vertex, (ShallowHistoryNode, DeepHistoryNode)):
            owner_name = vertex.owner()
            remembered = (
                self._history_shallow.get(owner_name)
                if isinstance(vertex, ShallowHistoryNode)
                else self._history_deep.get(owner_name)
            )
            if remembered:
                return await self._follow_from_owner(owner_name, remembered, event)
            for transition_name in vertex.transitions:
                transition = self.model.get(transition_name, TransitionNode)
                if transition is None:
                    continue
                guard = self.model.get(transition.guard or "", GuardNode[TInstance])
                if guard is not None and not await self._evaluate(guard, event):
                    continue
                return await self._transition(vertex, transition, event)
            owner_vertex = self.model.get(owner_name, VertexNode)
            return owner_vertex if owner_vertex is not None else self._state
        if isinstance(vertex, ChoiceNode):
            for transition_name in vertex.transitions:
                transition = self.model.get(transition_name, TransitionNode)
                if transition is None:
                    continue
                guard = self.model.get(transition.guard or "", GuardNode[TInstance])
                if guard is not None and not await self._evaluate(guard, event):
                    continue
                return await self._transition(vertex, transition, event)
            return self._state
        if isinstance(vertex, StateNode):
            previous_state = self._state
            if vertex.entry or vertex.activity:
                self._state = vertex
            for behavior_name in vertex.entry:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    await self._execute(behavior, event)
            if self._after.entry:
                self._after._notify(self._after.entry, lambda expected: expected == vertex.qualified_name)
            for behavior_name in vertex.activity:
                if preserved_activities is not None and behavior_name in preserved_activities:
                    continue
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    await self._execute(behavior, event)
            if not default_entry or vertex.initial == "":
                self._queue_final_completion(vertex)
                return vertex
            initial = self.model.get(vertex.initial, VertexNode)
            if isinstance(initial, VertexNode) and initial.transitions:
                transition = self.model.get(initial.transitions[0], TransitionNode)
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
        vertex: VertexNode,
        event: Event,
        preserved_activities: builtins.set[str] | None = None,
    ) -> VertexNode:
        if isinstance(vertex, StateNode):
            for behavior_name in vertex.activity:
                if preserved_activities is not None and behavior_name in preserved_activities:
                    continue
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    await self._terminate(behavior)
            for behavior_name in vertex.exit:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    await self._execute(behavior, event)
            if self._after.exit:
                self._after._notify(self._after.exit, lambda expected: expected == vertex.qualified_name)
        return vertex

    async def _evaluate(self, guard: GuardNode[TInstance], event: Event) -> bool:
        token = self._set_execution_scope(guard.scope or guard.owner())
        try:
            result = await _maybe_await(guard.expression(self._root_context, self._instance, event))
            return bool(result)
        except asyncio.CancelledError:
            if _task_is_cancelling():
                raise
            return False
        except Exception as error:
            if is_kind(event.kind, Kinds.ErrorEvent):
                return False
            self._dispatch_error(error)
            raise _AbortTransition(error) from error
        finally:
            self._reset_execution_scope(token)

    async def _execute(self, behavior: BehaviorNode[TInstance], event: Event) -> None:
        def behavior_scope() -> str:
            if behavior.scope:
                return behavior.scope
            qualified_name = getattr(behavior, "qualified_name", "")
            if isinstance(qualified_name, str) and qualified_name:
                return behavior.owner()
            return self.model.qualified_name

        try:
            if behavior.kind == Kinds.Concurrent:
                activity_ctx = Context(self._root_context)
                registered: list[ActiveBehavior | None] = [None]

                async def run_activity() -> None:
                    token = self._set_execution_scope(behavior_scope())
                    try:
                        await _maybe_await(behavior.operation(activity_ctx, self._instance, event))
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
                        self._reset_execution_scope(token)
                        self._notify_executed(behavior)
                        current = registered[0]
                        if current is not None and self._active.get(behavior.qualified_name) is current:
                            self._active.pop(behavior.qualified_name, None)
                        task = asyncio.current_task()
                        if behavior.defer_events and isinstance(task, asyncio.Task):
                            self._active_tasks.discard(task)
                            if behavior.timer_event is not None:
                                self._active_timer_tasks.discard(task)
                            if behavior.timer_event is not None:
                                self._active_timer_tasks.discard(task)
                        if (
                            behavior.defer_events
                            and self._started
                            and not self._stopping
                            and self._queue_len() > 0
                            and self._processing.try_acquire()
                        ):
                            self._awaitable = asyncio.create_task(self._process())

                task = asyncio.create_task(run_activity(), name=behavior.qualified_name)
                registered[0] = ActiveBehavior(context=activity_ctx, task=task)
                self._active[behavior.qualified_name] = registered[0]
                if behavior.defer_events:
                    self._active_tasks.add(task)
                    if behavior.timer_event is not None:
                        self._active_timer_tasks.add(task)
                return
            token = self._set_execution_scope(behavior_scope())
            try:
                await _maybe_await(behavior.operation(self._root_context, self._instance, event))
                self._notify_executed(behavior)
            finally:
                self._reset_execution_scope(token)
        except asyncio.CancelledError as error:
            if _task_is_cancelling():
                raise
            if self._stopping or is_kind(event.kind, Kinds.ErrorEvent):
                return
            self._dispatch_error(error)
            raise _AbortTransition(error) from error
        except Exception as error:
            if self._stopping or is_kind(event.kind, Kinds.ErrorEvent):
                return
            self._dispatch_error(error)
            raise _AbortTransition(error) from error

    def _dispatch_error(self, error: BaseException) -> None:
        if self._stopping:
            return
        try:
            self._dispatch_task(Event(name=ErrorEvent.name, data=error, kind=Kinds.ErrorEvent), observe_result=False)
        except ValidationError:
            return

    def _notify_executed(self, behavior: BehaviorNode[TInstance]) -> None:
        if not self._after.executed:
            return
        names = {behavior.qualified_name}
        owner = behavior.owner()
        if owner:
            names.add(owner)
        current = owner
        while current not in ("", ".", "/"):
            element = self.model.get(current, StateNode)
            if element is not None:
                names.add(current)
                break
            current = _parent_path(current)
        self._after._notify(self._after.executed, lambda expected: expected in names)

    async def _terminate(self, behavior: BehaviorNode[TInstance]) -> None:
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

    async def _enabled(self, source: StateNode, event: Event) -> TransitionNode | None:
        source_transitions = self.model.transition_map.get(source.qualified_name, {})
        ordered = [*source_transitions.get(event.qualified_name, [])]
        source_exit_event_name = event.qualified_name
        if event.qualified_name.startswith("@exit:"):
            source_exit_event_name = _exit_point_event_name_for_source(event.qualified_name, source.qualified_name)
            if source_exit_event_name != event.qualified_name:
                ordered.extend(source_transitions.get(source_exit_event_name, []))
        ordered.extend(source_transitions.get(AnyEvent.qualified_name, []))
        direct_deferred = self.model.direct_deferred_map.get(source.qualified_name)
        defers_event = direct_deferred is not None and (
            event.qualified_name in direct_deferred or source_exit_event_name in direct_deferred
        )
        for transition in ordered:
            if defers_event and transition.owner() != source.qualified_name:
                continue
            if transition.when is not None:
                maybe_when = self.model.get(transition.when, GuardNode[TInstance])
                if maybe_when is not None and not await self._evaluate(maybe_when, event):
                    continue
            if transition.guard is None:
                return transition
            maybe_guard = self.model.get(transition.guard, GuardNode[TInstance])
            if maybe_guard is not None and await self._evaluate(maybe_guard, event):
                return transition
        return None

    async def _should_retry_deferred(self, event: Event) -> bool:
        return not self.model.deferred_map.get(self._state.qualified_name, {}).get(event.qualified_name, False)

    async def _process(
        self,
        first_event: Event | None = None,
        pending_error: BaseException | None = None,
    ) -> None:
        deferred: list[Event] = list(self._deferred_events)
        deferred_queued: list[bool] = list(self._deferred_queued)
        if len(deferred_queued) < len(deferred):
            deferred_queued.extend(False for _ in range(len(deferred) - len(deferred_queued)))
        self._deferred_events.clear()
        self._deferred_queued.clear()
        error: BaseException | None = None
        try:
            await self._drain_queue(deferred, deferred_queued, first_event, pending_error)
        except BaseException as exc:
            error = exc
        finally:
            try:
                try:
                    while self._restart_requested is not None or self._stop_requested:
                        if self._restart_requested is not None:
                            (restart_data,) = self._restart_requested
                            self._restart_requested = None
                            self._stop_requested = False
                            await self._restart_locked(restart_data)
                            continue
                        self._stop_requested = False
                        await self._stop_locked()
                    if self._timer_task_pending_dispatch and self._queue_len() > 0:
                        self._timer_task_pending_dispatch = False
                        await self._drain_queue(deferred, deferred_queued)
                except BaseException as exc:
                    if error is None:
                        error = exc
            finally:
                self._deferred_events.extend(deferred)
                self._deferred_queued.extend(deferred_queued)
                self._processing.release(error)

    def _queue_push(self, event: Event) -> None:
        if event.kind == Kinds.TimeEvent:
            order = self.model.timer_event_order.get(event.qualified_name)
            if order is not None:
                setattr(event, "_hsm_time_order", order)
        error = self._queue.push(event)
        if error is not None and not is_kind(event.kind, Kinds.ErrorEvent):
            self._queue.push(Event(name=ErrorEvent.name, data=error, kind=Kinds.ErrorEvent))

    def _queue_pop(self) -> Event | None:
        while True:
            event_or_error = self._queue.pop()
            if not isinstance(event_or_error, BaseException):
                return event_or_error
            self._queue.push(Event(name=ErrorEvent.name, data=event_or_error, kind=Kinds.ErrorEvent))

    def _queue_len(self) -> int:
        len_or_error = self._queue.len()
        if isinstance(len_or_error, BaseException):
            self._queue.push(Event(name=ErrorEvent.name, data=len_or_error, kind=Kinds.ErrorEvent))
            return 0
        return len_or_error

    def _deferred_boundary_active(self, event: Event, current_leaf: VertexNode | None = None) -> bool:
        boundary = getattr(event, "_hsm_deferred_boundary", "")
        if not isinstance(boundary, str) or not boundary:
            return True
        state_name = (current_leaf or self._state).qualified_name
        return state_name == boundary or IsAncestor(boundary, state_name)

    def _promote_deferred_after_exit_point(self, deferred: list[Event], event: Event) -> None:
        boundary = _exit_point_event_source(event.qualified_name)
        if not boundary:
            return
        for deferred_event in deferred:
            if getattr(deferred_event, "_hsm_deferred_boundary", "") == boundary:
                delattr(deferred_event, "_hsm_deferred_boundary")

    async def _drain_queue(
        self,
        deferred: list[Event],
        deferred_queued: list[bool] | None = None,
        first_event: Event | None = None,
        pending_error: BaseException | None = None,
    ) -> None:
        if deferred_queued is None:
            deferred_queued = [False for _ in deferred]
        elif len(deferred_queued) < len(deferred):
            deferred_queued.extend(False for _ in range(len(deferred) - len(deferred_queued)))
        local_events: collections.deque[Event] = collections.deque()
        event = first_event if first_event is not None else self._queue_pop()
        pending_error_handled = False

        def add_deferred(event: Event, owner: str, *, queued: bool = False) -> None:
            owner = getattr(event, "_hsm_deferred_owner", owner)
            setattr(event, "_hsm_deferred_owner", owner)
            boundary = self.model.submachine_owner_map.get(owner)
            if boundary and boundary != owner:
                setattr(event, "_hsm_deferred_boundary", boundary)
            elif hasattr(event, "_hsm_deferred_boundary"):
                delattr(event, "_hsm_deferred_boundary")
            deferred.append(event)
            deferred_queued.append(queued)

        def remove_queued_deferred(event: Event) -> bool:
            for index, deferred_event in enumerate(deferred):
                if not deferred_queued[index]:
                    continue
                if deferred_event is event or deferred_event.qualified_name == event.qualified_name:
                    deferred.pop(index)
                    deferred_queued.pop(index)
                    return True
            return False

        async def retry_or_requeue_deferred() -> Event | None:
            if not deferred:
                return None
            next_deferred: list[Event] = []
            next_deferred_queued: list[bool] = []
            local_retry: list[Event] = []
            has_queued_retry = False
            for deferred_event, queued in zip(deferred, deferred_queued):
                if not self._deferred_boundary_active(deferred_event):
                    continue
                if await self._should_retry_deferred(deferred_event):
                    if queued:
                        has_queued_retry = True
                    else:
                        local_retry.append(deferred_event)
                    continue
                if not queued:
                    self._queue_push(deferred_event)
                next_deferred.append(deferred_event)
                next_deferred_queued.append(True)
            deferred[:] = next_deferred
            deferred_queued[:] = next_deferred_queued
            if has_queued_retry:
                local_events.extend(local_retry)
                return self._queue_pop()
            if local_retry:
                local_events.extend(local_retry[1:])
                return local_retry[0]
            return None

        if event is None:
            event = await retry_or_requeue_deferred()
        while event is not None:
            was_deferred = remove_queued_deferred(event)
            event_qualified_name = event.qualified_name
            current_leaf = self._state
            if was_deferred and not self._deferred_boundary_active(event, current_leaf):
                if local_events:
                    event = local_events.popleft()
                else:
                    event = self._queue_pop()
                if event is None:
                    event = await retry_or_requeue_deferred()
                continue
            if (
                event.kind == Kinds.TimeEvent
                and event.source
                and current_leaf.qualified_name != event.source
                and not IsAncestor(event.source, current_leaf.qualified_name)
            ):
                if local_events:
                    event = local_events.popleft()
                else:
                    event = self._queue_pop()
                continue
            if (
                was_deferred
                and self.model.deferred_map.get(current_leaf.qualified_name, {}).get(event.qualified_name, False)
            ):
                owner = getattr(event, "_hsm_deferred_owner", current_leaf.qualified_name)
                add_deferred(event, owner)
                if self._after.process:
                    self._after._notify(
                        self._after.process,
                        lambda expected: expected is None or expected == event_qualified_name,
                    )
                if local_events:
                    event = local_events.popleft()
                else:
                    event = self._queue_pop()
                if event is None:
                    event = await retry_or_requeue_deferred()
                continue
            qualified_name = current_leaf.qualified_name
            event_handled = False
            event_aborted = False
            while qualified_name:
                source = self.model.get(qualified_name, StateNode)
                if source is None:
                    break
                try:
                    transition = await self._enabled(source, event)
                    if transition is not None:
                        self._state = await self._transition(current_leaf, transition, event)
                        event_handled = True
                        if event.qualified_name.startswith("@exit:"):
                            self._promote_deferred_after_exit_point(deferred, event)
                        if is_kind(event.kind, Kinds.ErrorEvent):
                            pending_error_handled = True
                        break
                except _AbortTransition as abort:
                    event_aborted = True
                    if pending_error is None:
                        pending_error = abort.error
                    break
                if self.model.deferred_map.get(qualified_name, {}).get(event.qualified_name, False):
                    owner = self.model.deferred_owner_map.get(qualified_name, {}).get(event.qualified_name, qualified_name)
                    add_deferred(event, owner)
                    event_handled = True
                    break
                qualified_name = source.owner()
            if (
                not event_handled
                and not event_aborted
                and is_kind(event.kind, Kinds.CompletionEvent)
                and isinstance(event.qualified_name, str)
                and event.qualified_name.startswith("@exit:")
            ):
                restore_state = getattr(event, "_hsm_exit_point_restore_state", "")
                restored = self.model.get(restore_state, VertexNode)
                if restored is not None:
                    self._state = restored
                exit_name = getattr(event, "_hsm_exit_point_name", event.qualified_name)
                pending_error = RuntimeError(f'unhandled exit point "{exit_name}"')
            if self._after.process:
                self._after._notify(
                    self._after.process,
                    lambda expected: expected is None or expected == event_qualified_name,
                )
            if local_events:
                event = local_events.popleft()
            else:
                event = self._queue_pop()
            if event is None:
                event = await retry_or_requeue_deferred()
        if pending_error is not None and not pending_error_handled:
            raise pending_error

    def _evaluate_sync(self, guard: GuardNode[TInstance], event: Event) -> bool:
        token = self._set_execution_scope(guard.scope or guard.owner())
        try:
            result = guard.expression(self._root_context, self._instance, event)
            if inspect.isawaitable(result):
                _raise_async_required(result)
            return bool(result)
        except _AsyncRequired:
            raise
        except Exception as error:
            if is_kind(event.kind, Kinds.ErrorEvent):
                return False
            self._dispatch_error(error)
            raise _AbortTransition(error) from error
        finally:
            self._reset_execution_scope(token)

    def _execute_sync(self, behavior: BehaviorNode[TInstance], event: Event) -> None:
        def behavior_scope() -> str:
            if behavior.scope:
                return behavior.scope
            qualified_name = getattr(behavior, "qualified_name", "")
            if isinstance(qualified_name, str) and qualified_name:
                return behavior.owner()
            return self.model.qualified_name

        try:
            if behavior.kind == Kinds.Concurrent:
                activity_ctx = Context(self._root_context)
                registered: list[ActiveBehavior | None] = [None]

                async def run_activity() -> None:
                    token = self._set_execution_scope(behavior_scope())
                    try:
                        await _maybe_await(behavior.operation(activity_ctx, self._instance, event))
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
                        self._reset_execution_scope(token)
                        self._notify_executed(behavior)
                        current = registered[0]
                        if current is not None and self._active.get(behavior.qualified_name) is current:
                            self._active.pop(behavior.qualified_name, None)
                        task = asyncio.current_task()
                        if behavior.defer_events and isinstance(task, asyncio.Task):
                            self._active_tasks.discard(task)
                        if (
                            behavior.defer_events
                            and self._started
                            and not self._stopping
                            and self._queue_len() > 0
                            and self._processing.try_acquire()
                        ):
                            self._awaitable = asyncio.create_task(self._process())

                task = asyncio.create_task(run_activity(), name=behavior.qualified_name)
                registered[0] = ActiveBehavior(context=activity_ctx, task=task)
                self._active[behavior.qualified_name] = registered[0]
                if behavior.defer_events:
                    self._active_tasks.add(task)
                    if behavior.timer_event is not None:
                        self._active_timer_tasks.add(task)
                return
            token = self._set_execution_scope(behavior_scope())
            try:
                result = behavior.operation(self._root_context, self._instance, event)
                if inspect.isawaitable(result):
                    _raise_async_required(result)
                self._notify_executed(behavior)
            finally:
                self._reset_execution_scope(token)
        except _AsyncRequired:
            raise
        except Exception as error:
            if self._stopping or is_kind(event.kind, Kinds.ErrorEvent):
                return
            self._dispatch_error(error)
            raise _AbortTransition(error) from error

    def _enter_sync(self, vertex: VertexNode, event: Event, default_entry: bool) -> VertexNode:
        if isinstance(vertex, ExitPointNode):
            owner_vertex = self.model.get(vertex.owner(), VertexNode)
            restore_state = self._state.qualified_name
            if isinstance(owner_vertex, SubmachineStateNode):
                self._state = owner_vertex
            for effect_name in vertex.effect:
                effect = self.model.get(effect_name, BehaviorNode[TInstance])
                if effect is not None:
                    self._execute_sync(effect, event)
            if isinstance(owner_vertex, SubmachineStateNode):
                event_name = _exit_point_event_name(owner_vertex.qualified_name, vertex.public_name)
                exit_event = Event(
                    name=event_name,
                    qualified_name=event_name,
                    kind=Kinds.CompletionEvent,
                    data=copy.deepcopy(event.data),
                    schema=event.schema,
                )
                setattr(exit_event, "_hsm_exit_point_restore_state", restore_state)
                setattr(exit_event, "_hsm_exit_point_name", vertex.public_name)
                self._queue_push(exit_event)
                return owner_vertex
            return owner_vertex if owner_vertex is not None else self._state
        if isinstance(vertex, (ShallowHistoryNode, DeepHistoryNode)):
            raise _AsyncRequired()
        if isinstance(vertex, ChoiceNode):
            for transition_name in vertex.transitions:
                transition = self.model.get(transition_name, TransitionNode)
                if transition is None:
                    continue
                guard = self.model.get(transition.guard or "", GuardNode[TInstance])
                if guard is not None and not self._evaluate_sync(guard, event):
                    continue
                return self._transition_sync(vertex, transition, event)
            return self._state
        if isinstance(vertex, StateNode):
            previous_state = self._state
            if vertex.entry or vertex.activity:
                self._state = vertex
            for behavior_name in vertex.entry:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    self._execute_sync(behavior, event)
            if self._after.entry:
                self._after._notify(self._after.entry, lambda expected: expected == vertex.qualified_name)
            for behavior_name in vertex.activity:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    self._execute_sync(behavior, event)
            if not default_entry or vertex.initial == "":
                self._queue_final_completion(vertex)
                return vertex
            initial = self.model.get(vertex.initial, VertexNode)
            if isinstance(initial, VertexNode) and initial.transitions:
                transition = self.model.get(initial.transitions[0], TransitionNode)
                if transition is not None:
                    self._state = previous_state
                    try:
                        return self._transition_sync(vertex, transition, event)
                    except BaseException:
                        if self._state is previous_state:
                            self._state = vertex
                        raise
        return vertex

    def _exit_sync(self, vertex: VertexNode, event: Event) -> VertexNode:
        if isinstance(vertex, StateNode):
            for behavior_name in vertex.activity:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    self._terminate_sync(behavior)
            for behavior_name in vertex.exit:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    self._execute_sync(behavior, event)
            if self._after.exit:
                self._after._notify(self._after.exit, lambda expected: expected == vertex.qualified_name)
        return vertex

    def _terminate_sync(self, behavior: BehaviorNode[TInstance]) -> None:
        active = self._active.pop(behavior.qualified_name, None)
        if active is None:
            return
        active.context.cancel()
        if active.task is not asyncio.current_task():
            active.task.cancel()

    def _enabled_sync(self, source: StateNode, event: Event) -> TransitionNode | None:
        source_transitions = self.model.transition_map.get(source.qualified_name, {})
        ordered = [*source_transitions.get(event.qualified_name, [])]
        source_exit_event_name = event.qualified_name
        if event.qualified_name.startswith("@exit:"):
            source_exit_event_name = _exit_point_event_name_for_source(event.qualified_name, source.qualified_name)
            if source_exit_event_name != event.qualified_name:
                ordered.extend(source_transitions.get(source_exit_event_name, []))
        ordered.extend(source_transitions.get(AnyEvent.qualified_name, []))
        direct_deferred = self.model.direct_deferred_map.get(source.qualified_name)
        defers_event = direct_deferred is not None and (
            event.qualified_name in direct_deferred or source_exit_event_name in direct_deferred
        )
        for transition in ordered:
            if defers_event and transition.owner() != source.qualified_name:
                continue
            if transition.when is not None:
                maybe_when = self.model.get(transition.when, GuardNode[TInstance])
                if maybe_when is not None and not self._evaluate_sync(maybe_when, event):
                    continue
            if transition.guard is None:
                return transition
            maybe_guard = self.model.get(transition.guard, GuardNode[TInstance])
            if maybe_guard is not None and self._evaluate_sync(maybe_guard, event):
                return transition
        return None

    def _transition_sync(self, current_leaf: VertexNode, transition: TransitionNode, event: Event) -> VertexNode:
        path = transition.paths.get(current_leaf.qualified_name)
        if path is None:
            return current_leaf
        if not path.synchronous:
            raise _AsyncRequired()
        if self._path_touches_activity(path):
            raise _AsyncRequired()
        if transition.kind != Kinds.Internal:
            self._remember_history(current_leaf.qualified_name, transition.history_target_owner)
        for exiting in path.exit:
            vertex = self.model.get(exiting, VertexNode)
            if vertex is not None:
                self._exit_sync(vertex, event)
        for index, effect_name in enumerate(transition.effect):
            effect = self.model.get(effect_name, BehaviorNode[TInstance])
            if effect is None:
                continue
            try:
                self._execute_sync(effect, event)
            except BaseException:
                if (
                    path.effect_failure_state
                    and path.effect_failure_state_index >= 0
                    and index >= path.effect_failure_state_index
                ):
                    restored = self.model.get(path.effect_failure_state, VertexNode)
                    if restored is not None:
                        self._state = restored
                raise
        if transition.kind == Kinds.Internal:
            return current_leaf
        current: VertexNode = current_leaf
        for entering in path.enter:
            vertex = self.model.get(entering, VertexNode)
            if vertex is None:
                continue
            current = self._enter_sync(vertex, event, entering == path.target)
            if entering == path.target:
                return current
        target = self.model.get(path.target, VertexNode)
        return current if target is None else target

    def _path_touches_activity(self, path: TransitionPath) -> bool:
        for name in (*path.exit, *path.enter):
            vertex = self.model.get(name, StateNode)
            if vertex is not None and vertex.activity:
                return True
        return False

    def _event_can_process_sync(self, event: Event) -> bool:
        qualified_name = self._state.qualified_name
        while qualified_name:
            source = self.model.get(qualified_name, StateNode)
            if source is None:
                break
            source_transitions = self.model.transition_map.get(source.qualified_name, {})
            ordered = [*source_transitions.get(event.qualified_name, []), *source_transitions.get(AnyEvent.qualified_name, [])]
            for transition in ordered:
                path = transition.paths.get(self._state.qualified_name)
                if path is None or not path.synchronous or self._path_touches_activity(path):
                    return False
            if self.model.deferred_map.get(qualified_name, {}).get(event.qualified_name, False):
                return False
            qualified_name = source.owner()
        return True

    def _process_event_sync(self, event: Event) -> None:
        event_qualified_name = event.qualified_name
        current_leaf = self._state
        qualified_name = current_leaf.qualified_name
        event_handled = False
        while qualified_name:
            source = self.model.get(qualified_name, StateNode)
            if source is None:
                break
            transition = self._enabled_sync(source, event)
            if transition is not None:
                self._state = self._transition_sync(current_leaf, transition, event)
                event_handled = True
                if event.qualified_name.startswith("@exit:"):
                    self._promote_deferred_after_exit_point(self._deferred_events, event)
                break
            qualified_name = source.owner()
        if (
            not event_handled
            and is_kind(event.kind, Kinds.CompletionEvent)
            and isinstance(event.qualified_name, str)
            and event.qualified_name.startswith("@exit:")
        ):
            restore_state = getattr(event, "_hsm_exit_point_restore_state", "")
            restored = self.model.get(restore_state, VertexNode)
            if restored is not None:
                self._state = restored
            exit_name = getattr(event, "_hsm_exit_point_name", event.qualified_name)
            raise RuntimeError(f'unhandled exit point "{exit_name}"')
        if self._after.process:
            self._after._notify(
                self._after.process,
                lambda expected: expected is None or expected == event_qualified_name,
            )

    def _process_sync_or_task(self) -> typing.Awaitable[None]:
        error: BaseException | None = None
        try:
            event = self._queue_pop()
            while event is not None:
                if (
                    event.kind == Kinds.TimeEvent
                    and event.source
                    and self._state.qualified_name != event.source
                    and not IsAncestor(event.source, self._state.qualified_name)
                ):
                    event = self._queue_pop()
                    continue
                if not self._event_can_process_sync(event):
                    return asyncio.create_task(self._process(event))
                self._process_event_sync(event)
                if self._restart_requested is not None or self._stop_requested:
                    return asyncio.create_task(self._process())
                event = self._queue_pop()
            if self._deferred_events:
                return asyncio.create_task(self._process())
            self._processing.release()
            return _future_done()
        except _AsyncRequired:
            error = RuntimeError("transition path marked synchronous returned awaitable")
            self._processing.release(error)
            future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            future.set_exception(error)
            return future
        except _AbortTransition as abort:
            return asyncio.create_task(self._process(pending_error=abort.error))
        except BaseException as exc:
            error = exc
            self._processing.release(error)
            raise

    async def _transition(self, current_leaf: VertexNode, transition: TransitionNode, event: Event) -> VertexNode:
        path = transition.paths.get(current_leaf.qualified_name)
        if path is None:
            return current_leaf
        preserved_activities: builtins.set[str] = builtins.set()
        if transition.kind == Kinds.Self and event.kind == Kinds.TimeEvent:
            for exiting in path.exit:
                behavior_name = self.model.timer_activity_map.get(exiting, {}).get(event.qualified_name)
                if behavior_name:
                    preserved_activities.add(behavior_name)
        if transition.kind != Kinds.Internal:
            self._remember_history(current_leaf.qualified_name, transition.history_target_owner)
        for exiting in path.exit:
            vertex = self.model.get(exiting, VertexNode)
            if vertex is not None:
                await self._exit(vertex, event, preserved_activities)
        for index, effect_name in enumerate(transition.effect):
            effect = self.model.get(effect_name, BehaviorNode[TInstance])
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
                    restored = self.model.get(path.effect_failure_state, VertexNode)
                    if restored is not None:
                        self._state = restored
                raise
        if transition.kind == Kinds.Internal:
            return current_leaf
        current: VertexNode = current_leaf
        for entering in path.enter:
            vertex = self.model.get(entering, VertexNode)
            if vertex is None:
                continue
            current = await self._enter(vertex, event, entering == path.target, preserved_activities)
            if entering == path.target:
                return current
        target = self.model.get(path.target, VertexNode)
        return current if target is None else target

    def _dispatch_task(self, event: Event[typing.Any], observe_result: bool = True) -> typing.Awaitable[None]:
        self._ensure_accepting_events()
        self._queue_push(event)
        if self._after.dispatch:
            self._after._notify(self._after.dispatch, lambda expected: expected == event.qualified_name)
        current_task = asyncio.current_task()
        if (
            (event.kind != Kinds.TimeEvent and current_task in self._active_timer_tasks)
            or (event.kind == Kinds.ChangeEvent and current_task in self._active_tasks)
        ):
            if event.kind != Kinds.TimeEvent and current_task in self._active_timer_tasks:
                self._timer_task_pending_dispatch = True
            return _future_done()
        acquired = self._processing.try_acquire()
        if not acquired and asyncio.current_task() is self._awaitable:
            return _future_done()
        if not acquired and isinstance(self._awaitable, asyncio.Future) and self._awaitable.done():
            return _future_done()
        if acquired:
            if event.kind == Kinds.TimeEvent:
                self._awaitable = asyncio.create_task(self._process())
            else:
                self._awaitable = self._process_sync_or_task()
        if not observe_result:
            return self._awaitable
        return self._processing.wait()

    def dispatch(self, event: Event[typing.Any]) -> typing.Awaitable[None]:
        return self._dispatch_task(_clone_event(event))

    def dispatch_later(self, event: Event[typing.Any]) -> typing.Awaitable[None]:
        self._ensure_accepting_events()
        self._queue_push(_clone_event(event))
        if self._after.dispatch:
            self._after._notify(self._after.dispatch, lambda expected: expected == event.qualified_name)
        current_task = asyncio.current_task()
        if (
            (event.kind != Kinds.TimeEvent and current_task in self._active_timer_tasks)
            or (event.kind == Kinds.ChangeEvent and current_task in self._active_tasks)
        ):
            if event.kind != Kinds.TimeEvent and current_task in self._active_timer_tasks:
                self._timer_task_pending_dispatch = True
            return _future_done()
        acquired = self._processing.try_acquire()
        if not acquired and asyncio.current_task() is self._awaitable:
            return _future_done()
        if not acquired and isinstance(self._awaitable, asyncio.Future) and self._awaitable.done():
            return _future_done()
        if acquired:
            loop = asyncio.get_running_loop()
            loop.call_soon(self._start_scheduled_processing, event.kind)
        return self._processing.wait()

    def _start_scheduled_processing(self, event_kind: int) -> None:
        if event_kind == Kinds.TimeEvent:
            self._awaitable = asyncio.create_task(self._process())
        else:
            self._awaitable = self._process_sync_or_task()

    async def _stop_locked(self) -> None:
        final_event = Event(name=FinalEvent.name, kind=Kinds.CompletionEvent)
        self._stopping = True
        try:
            while self._state.qualified_name != self.model.qualified_name:
                await self._exit(self._state, final_event)
                parent = self.model.get(_parent_path(self._state.qualified_name), VertexNode)
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
        finally:
            self._stopping = False
        self._active.clear()
        self._active_tasks.clear()
        self._active_timer_tasks.clear()
        self._timer_task_pending_dispatch = False
        self._after._cancel_all()
        self._root_context.unregister(self)
        self._root_context.cancel()
        self._queue = Queue(self._config.Queue) if self._config.Queue is not None else Queue()
        self._deferred_events.clear()
        self._deferred_queued.clear()
        self._attributes = _default_attribute_values(self.model)
        self._history_shallow.clear()
        self._history_deep.clear()
        self._state = self.model
        self._started = False
        self._stop_requested = False
        self._restart_requested = None
        self._execution_scope = None
        self._awaitable = _future_done()

    async def stop(self) -> None:
        if asyncio.current_task() is self._awaitable:
            self._stop_requested = True
            return
        await self._processing.acquire()
        try:
            await self._stop_locked()
        finally:
            self._processing.release()

    async def restart(self, data: typing.Any = None) -> None:
        if asyncio.current_task() is self._awaitable:
            self._restart_requested = (data,)
            return
        if not self._started:
            raise ValidationError("operation requires a started HSM")
        await self.stop()
        self._reset_for_restart()
        await self._start(self._config.Data if data is None else data)

    def _reset_for_restart(self) -> None:
        self._root_context = _WithRuntimeHSM(self._base_context, self)
        self._queue = Queue(self._config.Queue) if self._config.Queue is not None else Queue()
        self._deferred_events.clear()
        self._deferred_queued.clear()
        self._active.clear()
        self._active_tasks.clear()
        self._active_timer_tasks.clear()
        self._timer_task_pending_dispatch = False
        self._attributes = _default_attribute_values(self.model)
        self._history_shallow.clear()
        self._history_deep.clear()
        self._state = self.model
        self._started = False
        self._stopping = False
        self._stop_requested = False
        self._restart_requested = None
        self._execution_scope = None
        self._awaitable = _future_done()
        self._root_context.register(self)

    async def _restart_locked(self, data: typing.Any = None) -> None:
        await self._stop_locked()
        self._reset_for_restart()
        restart_data = self._config.Data if data is None else data
        initial_event = InitialEvent.WithData(restart_data) if restart_data is not None else InitialEvent
        self._state = await self._enter(self.model, initial_event, True)
        self._started = True
        startup_deferred: list[Event] = []
        try:
            startup_deferred_queued: list[bool] = []
            await self._drain_queue(startup_deferred, startup_deferred_queued)
        finally:
            self._deferred_events.extend(startup_deferred)
            self._deferred_queued.extend(startup_deferred_queued)

    def _qualify_attribute_name(self, name: str) -> str:
        if name == "" or posixpath.isabs(name):
            return _qualify_model_name(self.model.qualified_name, name)
        scope = self._current_execution_scope()
        if scope not in (None, "", ".", "/"):
            qualified_name = self.model.attribute_scope_aliases.get((scope, name))
            if qualified_name is not None:
                return qualified_name
        fallback_name = _qualify_model_name(self.model.qualified_name, name)
        if fallback_name in self.model.attributes:
            return fallback_name
        current_state = getattr(self, "_state", None)
        if current_state is not None:
            qualified_name = self.model.attribute_scope_aliases.get((current_state.qualified_name, name))
            if qualified_name is not None:
                return qualified_name
        return self.model.attribute_aliases.get(name, fallback_name)

    def get(self, name: str) -> tuple[typing.Any, bool]:
        qualified_name = self._qualify_attribute_name(name)
        if qualified_name in self._attributes:
            return copy.deepcopy(self._attributes[qualified_name]), True
        return None, False

    async def set(self, name: str, value: typing.Any) -> None:
        self._ensure_accepting_events()
        qualified_name = self._qualify_attribute_name(name)
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
        self.model.set(event.qualified_name, event)
        await self.dispatch(event)
        return None

    async def call(self, name: str, *args: typing.Any) -> typing.Any:
        self._ensure_accepting_events()
        if not name:
            raise ValidationError("operation name cannot be empty")
        operation_scope = self._current_execution_scope() or self.model.qualified_name
        operation_name = _resolve_operation_name(self.model, operation_scope, name)
        operation = self.model.operations.get(operation_name)
        if operation is None:
            raise ValidationError(f'missing operation "{name}" for OnCall()')
        callback = operation.callback
        if callback is None:
            callback = getattr(self._instance, name, None)
        if callback is None:
            raise ValidationError(f'missing operation "{name}" for OnCall()')
        event = Event(
            name=_oncall_event_name(name),
            qualified_name=_oncall_event_name(name),
            kind=Kinds.CallEvent,
            data=CallData(name=name, args=args),
            schema=CallData,
        )
        self.model.set(event.qualified_name, event)
        await self.dispatch(event)
        result = _invoke_operation_callback(
            callback,
            self._root_context,
            self._instance,
            args,
        )
        value = await _maybe_await(result)
        return value

    def take_snapshot(self) -> Snapshot:
        current_name = self._state.qualified_name
        return Snapshot(
            ID=self._id,
            QualifiedName=self._qualified_name,
            State=self._state.qualified_name,
            Attributes=copy.deepcopy(self._attributes),
            QueueLen=self._queue_len() + sum(not queued for queued in self._deferred_queued),
            Events=self.model.snapshot_event_map.get(current_name, ()),
        )

    State = state
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

    def _ensure_accepting_events(self) -> None:
        for instance in self.instances:
            machine = getattr(instance, "_Instance__hsm", None)
            if not isinstance(machine, HSM):
                raise ValidationError("operation requires a started HSM")
            machine._ensure_accepting_events()

    def dispatch(
        self,
        event: Event,
        source: "HSM[typing.Any] | Group | None" = None,
    ) -> typing.Awaitable[None]:
        machines = self._started_machines()
        if not machines:
            raise ValidationError("operation requires a started HSM")
        if any(machine._deferred_events for machine in machines):
            return _dispatch_machines_sequential_started(
                (machine, _clone_event_for_delivery(event, machine, source)) for machine in machines
            )
        if _dispatch_from_processing(source):
            return _queue_dispatches_for_later(
                (machine, _clone_event_for_delivery(event, machine, source)) for machine in machines
            )
        return _dispatch_machines_ordered_task(
            (machine, _clone_event_for_delivery(event, machine, source)) for machine in machines
        )

    def _started_machines(self) -> list["HSM[typing.Any]"]:
        return [
            machine
            for instance in self.instances
            if isinstance((machine := getattr(instance, "_Instance__hsm", None)), HSM)
            and machine._started
        ]

    def stop(self) -> typing.Awaitable[None]:
        if self.instances:
            self._ensure_accepting_events()
        return _await_all(instance.stop() for instance in self.instances if instance is not None)

    def restart(self, data: typing.Any = None) -> typing.Awaitable[None]:
        self._ensure_accepting_events()
        return _await_all(
            instance.restart(copy.deepcopy(data))
            for instance in self.instances
            if instance is not None
        )

    def get(self, name: str) -> tuple[typing.Any, bool]:
        if not self.instances:
            return None, False
        return Get(self.context(), self.instances[0], name)

    def set(self, ctx: Context | None, name: str, value: typing.Any) -> typing.Awaitable[None]:
        self._ensure_accepting_events()
        return _await_all(
            Set(ctx, instance, name, copy.deepcopy(value))
            for instance in self.instances
            if instance is not None
        )

    def call(self, ctx: Context | None, name: str, *args: typing.Any) -> typing.Awaitable[typing.Any]:
        if not self.instances:
            raise ValidationError("missing hsm")
        return Call(ctx, self.instances[0], name, *args)

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
            states.append(snapshot.State)
            queue_len += snapshot.QueueLen
            events.extend(snapshot.Events)
        return Snapshot(
            ID=self.id if self.id else ",".join(ids),
            QualifiedName=",".join(qualified_names),
            State=" | ".join(states),
            Attributes={},
            QueueLen=queue_len,
            Events=tuple(events),
        )


def NewGroup(*instances: typing.Union[str, Instance, Group, None]) -> Group:
    return Group(*instances)


MakeGroup = NewGroup


def _new_future() -> asyncio.Future[None]:
    return asyncio.get_running_loop().create_future()


def _after_future(waiters: list[tuple[typing.Any, asyncio.Future[None]]], expected: typing.Any) -> asyncio.Future[None]:
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


def _resolve_observable_machine(sm: typing.Union[HSM[TInstance], Instance]) -> HSM[TInstance]:
    machine = _resolve_machine(sm)
    machine._ensure_accepting_events()
    return machine


def AfterDispatch(ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], event: Event) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(machine._after.dispatch, event.qualified_name)


def AfterProcess(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance],
    maybe_event: Event | None = None,
) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(machine._after.process, None if maybe_event is None else maybe_event.qualified_name)


def AfterEntry(ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], state: str) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(machine._after.entry, state)


def AfterExit(ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], state: str) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(machine._after.exit, state)


def AfterExecuted(ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], state: str) -> asyncio.Future[None]:
    machine = _resolve_observable_machine(hsm)
    return _after_future(machine._after.executed, state)


def _resolve_machine(sm: typing.Union[HSM[TInstance], Instance]) -> HSM[TInstance]:
    if isinstance(sm, HSM):
        return sm
    hsm = getattr(sm, "_Instance__hsm", None)
    if hsm is None:
        raise ValidationError("operation requires a started HSM")
    return hsm


def _event_from_name(event_or_name: str | Event, kind_value: int = Kinds.Event) -> Event:
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
        return machine._id
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
        event_copy.target = target._id
    return event_copy


def _finalize_model(model: Model) -> None:
    if not model.initial:
        raise ValidationError("initial state is required for state machine")
    if model.entry:
        raise ValidationError("entry actions are not allowed on top level state machine")
    if model.exit:
        raise ValidationError("exit actions are not allowed on top level state machine")
    _build_operation_aliases(model)
    for name, scope in model.pending_operations:
        if _resolve_operation_name(model, scope, name) not in model.operations:
            raise ValidationError(f'missing operation "{name}" for behavior or guard')
    _validate_submachine_transitions(model)
    entry_point_failure_boundaries = _resolve_submachine_entry_point_targets(model)
    _index_when_transitions(model)
    _build_attribute_aliases(model)
    _validate_transition_events(model)
    _resolve_transition_paths(model)
    _apply_entry_point_failure_boundaries(model, entry_point_failure_boundaries)
    _index_history_target_owners(model)
    _build_transition_table(model)
    _build_snapshot_event_table(model)
    _build_deferred_table(model)
    _build_timer_activity_table(model)
    _mark_synchronous_transition_paths(model)


def _submachine_ancestor_for(model: Model, qualified_name: str) -> SubmachineStateNode | None:
    current = qualified_name
    while current not in ("", ".", "/"):
        element = model.get(current, SubmachineStateNode)
        if element is not None:
            return element
        if current == model.qualified_name:
            return None
        current = _parent_path(current)
    return None


def _split_entry_point_target(target: str) -> tuple[str, str] | None:
    marker = "/.entry/"
    if marker in target:
        boundary, entry_name = target.rsplit(marker, 1)
    elif target.startswith(".entry/"):
        boundary, entry_name = ".", target[len(".entry/"):]
    else:
        return None
    if not entry_name or "/" in entry_name:
        return None
    return boundary or "/", entry_name


def _validate_submachine_transitions(model: Model) -> None:
    for transition in list(model.members.values()):
        if not isinstance(transition, TransitionNode):
            continue
        validation_target = transition.target
        entry_point_target = _split_entry_point_target(transition.target)
        if entry_point_target is not None:
            target_name, entry_point_name = entry_point_target
            target_state = model.get(target_name, SubmachineStateNode)
            if target_state is None:
                raise ValidationError(
                    f'EntryPoint "{entry_point_name}" can only target a SubmachineState'
                )
            if entry_point_name not in target_state.entry_points:
                raise ValidationError(
                    f'SubmachineState "{target_name}" has no entry point "{entry_point_name}"'
                )
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
                    f'Transition "{transition.qualified_name}" cannot target internal state "{validation_target}" of SubmachineState "{target_submachine.qualified_name}"'
                )
        for event_name in transition.events:
            if not event_name.startswith("@exit:"):
                continue
            source_state = model.get(transition.source, SubmachineStateNode)
            if source_state is None:
                raise ValidationError("ExitPoint outcome can only be handled by a SubmachineState")
            exit_point_name = event_name.rsplit(":", 1)[-1]
            if exit_point_name not in source_state.exit_points:
                raise ValidationError(
                    f'SubmachineState "{source_state.qualified_name}" has no exit point "{exit_point_name}"'
                )


def _resolve_submachine_entry_point_targets(model: Model) -> dict[str, tuple[str, int]]:
    failure_boundaries: dict[str, tuple[str, int]] = {}
    for transition in list(model.members.values()):
        if not isinstance(transition, TransitionNode):
            continue
        entry_point_target = _split_entry_point_target(transition.target)
        if entry_point_target is None:
            continue
        target_name, entry_point_name = entry_point_target
        target_state = model.get(target_name, SubmachineStateNode)
        if target_state is None:
            continue
        entry_point = target_state.entry_points.get(entry_point_name)
        if entry_point is None:
            continue
        effect_failure_state_index = len(transition.effect)
        transition.effect = [*transition.effect, *entry_point.effect]
        transition.target = entry_point.target
        failure_boundaries[transition.qualified_name] = (target_name, effect_failure_state_index)
    return failure_boundaries


def _apply_entry_point_failure_boundaries(
    model: Model,
    failure_boundaries: dict[str, tuple[str, int]],
) -> None:
    for transition_name, (entry_boundary, effect_failure_state_index) in failure_boundaries.items():
        transition = model.get(transition_name, TransitionNode)
        if transition is None:
            continue
        for leaf_name, path in list(transition.paths.items()):
            transition.paths[leaf_name] = TransitionPath(
                target=path.target,
                enter=path.enter,
                exit=path.exit,
                effect_failure_state_index=effect_failure_state_index,
                effect_failure_state=entry_boundary,
                synchronous=path.synchronous,
            )


def _index_when_transitions(model: Model) -> None:
    attribute_events: list[str] = []
    for name in model.attributes:
        if model.get(name, Event) is None:
            model.set(
                name,
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
        if not isinstance(transition, TransitionNode):
            continue
        if transition.when is None and transition.generated_when is not None and attribute_events:
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
                        source = model.get(transition.source, StateNode)
                        if source is not None:
                            source.activity = [
                                behavior_name
                                for behavior_name in source.activity
                                if not (
                                    (behavior := model.get(behavior_name, BehaviorNode[typing.Any])) is not None
                                    and behavior.generated_when_event in synthetic_events
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
        source = model.get(transition.source, StateNode)
        guard = model.get(transition.when, GuardNode[typing.Any])
        if source is None or guard is None:
            continue
        event = Event(
            name=join(transition.qualified_name, ".when"),
            qualified_name=join(transition.qualified_name, ".when"),
            kind=Kinds.ChangeEvent,
        )
        model.set(event.qualified_name, event)
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

        behavior = BehaviorNode(
            qualified_name=join(source.qualified_name, event.name, str(len(model.members))),
            kind=Kinds.Concurrent,
            operation=operation,
            scope=source.qualified_name,
            generated_when_event=event.qualified_name,
        )
        source.activity.append(behavior.qualified_name)
        model.set(behavior.qualified_name, behavior)


def _build_attribute_aliases(model: Model) -> None:
    model.attribute_aliases.clear()
    model.attribute_scope_aliases.clear()
    duplicates: builtins.set[str] = builtins.set()
    by_owner: dict[str, dict[str, str]] = {}
    for qualified_name in model.attributes:
        alias = posixpath.basename(qualified_name)
        owner = _parent_path(qualified_name)
        by_owner.setdefault(owner, {})[alias] = qualified_name
        if alias in model.attribute_aliases:
            duplicates.add(alias)
            continue
        model.attribute_aliases[alias] = qualified_name
    for alias in duplicates:
        model.attribute_aliases.pop(alias, None)
    for scope in model.members:
        current = scope
        seen: builtins.set[str] = builtins.set()
        while current not in ("", ".", "/"):
            for alias, qualified_name in by_owner.get(current, {}).items():
                if alias not in seen:
                    model.attribute_scope_aliases[(scope, alias)] = qualified_name
                    seen.add(alias)
            if current == model.qualified_name:
                break
            current = _parent_path(current)


def _operation_scope(qualified_name: str) -> str:
    owner = _parent_path(qualified_name)
    if posixpath.basename(owner) == ".operation":
        return _parent_path(owner)
    return owner


def _build_operation_aliases(model: Model) -> None:
    model.operation_aliases.clear()
    model.operation_name_aliases.clear()
    declared: dict[tuple[str, str], str] = {}
    duplicates: builtins.set[str] = builtins.set()
    for qualified_name, operation in list(model.operations.items()):
        if qualified_name != operation.qualified_name:
            continue
        name = operation.declared_name or operation.name()
        scope = _operation_scope(operation.qualified_name)
        declared[(scope, name)] = operation.qualified_name
        if name in model.operation_name_aliases:
            duplicates.add(name)
        else:
            model.operation_name_aliases[name] = operation.qualified_name
    for alias in duplicates:
        model.operation_name_aliases.pop(alias, None)
    scopes = [
        element.qualified_name
        for element in model.members.values()
        if isinstance(element, StateNode)
    ]
    for scope in scopes:
        for operation_scope, operation_name in declared:
            current = scope
            while current not in ("", ".", "/"):
                if current == operation_scope:
                    model.operation_aliases[(scope, operation_name)] = declared[(operation_scope, operation_name)]
                    break
                if current == model.qualified_name:
                    break
                current = _parent_path(current)
        for operation_name, operation_qualified_name in model.operation_name_aliases.items():
            model.operation_aliases.setdefault((scope, operation_name), operation_qualified_name)


def _resolve_operation_name(model: Model, scope: str, name: str) -> str:
    if posixpath.isabs(name) and name in model.operations:
        return name
    if scope:
        resolved = model.operation_aliases.get((scope, name))
        if resolved is not None:
            return resolved
    return model.operation_name_aliases.get(name, name)


def _validate_transition_events(model: Model) -> None:
    for transition in model.members.values():
        if not isinstance(transition, TransitionNode):
            continue
        source = model.get(transition.source, VertexNode)
        if source is None or isinstance(source, PseudostateNode):
            continue
        if not transition.events:
            raise ValidationError(f'Transition "{transition.qualified_name}" has no events')


def _resolve_transition_paths(model: Model) -> None:
    for element in model.members.values():
        if isinstance(element, TransitionNode):
            element.paths.clear()
            ResolvePaths(transition=element).apply(model, [])


def _index_history_target_owners(model: Model) -> None:
    for element in model.members.values():
        if not isinstance(element, TransitionNode):
            continue
        target = model.get(element.target, VertexNode)
        element.history_target_owner = (
            target.owner()
            if isinstance(target, (ShallowHistoryNode, DeepHistoryNode))
            else None
        )


def _build_transition_table(model: Model) -> None:
    model.transition_map.clear()
    for state_name, element in model.members.items():
        if not isinstance(element, StateNode):
            continue
        model.transition_map[state_name] = {}
        transitions_by_event: dict[str, list[tuple[TransitionNode, int]]] = {}
        for index, transition_name in enumerate(element.transitions):
            transition = model.get(transition_name, TransitionNode)
            if transition is None or not transition.events:
                continue
            for event_name in transition.events:
                transitions_by_event.setdefault(event_name, []).append((transition, index))
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
            model.transition_map[state_name][event_name] = [item[0] for item in transitions]


def _build_snapshot_event_table(model: Model) -> None:
    model.snapshot_event_map.clear()
    for state_name, element in model.members.items():
        if not isinstance(element, StateNode):
            continue
        snapshots: list[EventSnapshot] = []
        current_path = state_name
        while current_path:
            source = model.get(current_path, StateNode)
            if source is None:
                break
            for transition_name in source.transitions:
                transition = model.get(transition_name, TransitionNode)
                if transition is None:
                    continue
                if transition.paths and state_name not in transition.paths:
                    continue
                for event_name in transition.events:
                    event = model.events.get(event_name)
                    if event is None:
                        continue
                    if event.kind == Kinds.CompletionEvent and not isinstance(element, FinalStateNode):
                        continue
                    guard = model.get(transition.guard, GuardNode[typing.Any]) if transition.guard is not None else None
                    snapshots.append(
                        EventSnapshot(
                            Name=event_name,
                            Kind=event.kind,
                            Target=transition.target or None,
                            Guard=(
                                bool(getattr(guard.expression, "_hsm_snapshot_guard", True))
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


def _callable_is_synchronous(callback: typing.Callable[..., typing.Any]) -> bool:
    try:
        callback = inspect.unwrap(callback)
    except Exception:
        pass
    return not inspect.iscoroutinefunction(callback)


def _guard_is_synchronous(model: Model, guard_name: str | None) -> bool:
    guard = model.get(guard_name or "", GuardNode)
    return guard is None or _callable_is_synchronous(guard.expression)


def _behavior_is_synchronous(model: Model, behavior_name: str) -> bool:
    behavior = model.get(behavior_name, BehaviorNode)
    if behavior is None or behavior.kind == Kinds.Concurrent:
        return True
    return _callable_is_synchronous(behavior.operation)


def _behaviors_are_synchronous(model: Model, behavior_names: list[str]) -> bool:
    return all(_behavior_is_synchronous(model, behavior_name) for behavior_name in behavior_names)


def _entry_is_synchronous(
    model: Model,
    vertex: VertexNode,
    default_entry: bool,
    seen: builtins.set[tuple[str, str]],
) -> bool:
    if isinstance(vertex, ExitPointNode):
        return _behaviors_are_synchronous(model, vertex.effect)
    if isinstance(vertex, (ShallowHistoryNode, DeepHistoryNode)):
        return False
    if isinstance(vertex, ChoiceNode):
        for transition_name in vertex.transitions:
            transition = model.get(transition_name, TransitionNode)
            if transition is None:
                continue
            if not _transition_is_synchronous(model, transition, vertex.qualified_name, seen):
                return False
        return True
    if isinstance(vertex, StateNode):
        if not _behaviors_are_synchronous(model, vertex.entry):
            return False
        if not default_entry or vertex.initial == "":
            return True
        initial = model.get(vertex.initial, VertexNode)
        if isinstance(initial, VertexNode) and initial.transitions:
            transition = model.get(initial.transitions[0], TransitionNode)
            if transition is not None:
                return _transition_is_synchronous(model, transition, vertex.qualified_name, seen)
    return True


def _transition_is_synchronous(
    model: Model,
    transition: TransitionNode,
    leaf_name: str,
    seen: builtins.set[tuple[str, str]],
) -> bool:
    key = (transition.qualified_name, leaf_name)
    if key in seen:
        return True
    seen.add(key)
    path = transition.paths.get(leaf_name)
    if path is None:
        return True
    if not _guard_is_synchronous(model, transition.when):
        return False
    if not _guard_is_synchronous(model, transition.guard):
        return False
    if not _behaviors_are_synchronous(model, transition.effect):
        return False
    for exiting in path.exit:
        vertex = model.get(exiting, StateNode)
        if vertex is not None and not _behaviors_are_synchronous(model, vertex.exit):
            return False
    if transition.kind == Kinds.Internal:
        return True
    for entering in path.enter:
        vertex = model.get(entering, VertexNode)
        if vertex is None:
            continue
        if not _entry_is_synchronous(model, vertex, entering == path.target, seen):
            return False
    return True


def _mark_synchronous_transition_paths(model: Model) -> None:
    for element in model.members.values():
        if not isinstance(element, TransitionNode):
            continue
        for leaf_name, path in element.paths.items():
            path.synchronous = _transition_is_synchronous(model, element, leaf_name, builtins.set())


def _build_deferred_table(model: Model) -> None:
    model.deferred_map.clear()
    model.deferred_owner_map.clear()
    model.direct_deferred_map.clear()
    model.submachine_owner_map.clear()
    for state_name, element in model.members.items():
        if not isinstance(element, StateNode):
            continue
        current_owner = state_name
        while current_owner:
            owner_state = model.members.get(current_owner)
            if isinstance(owner_state, SubmachineStateNode):
                model.submachine_owner_map[state_name] = current_owner
                break
            if current_owner in ("", "/", model.qualified_name):
                break
            current_owner = _parent_path(current_owner)
        model.deferred_map[state_name] = {}
        model.direct_deferred_map[state_name] = builtins.set(element.deferred)
        current_path = state_name
        model.deferred_owner_map[state_name] = {}
        while current_path:
            current_state = model.members.get(current_path)
            if isinstance(current_state, StateNode):
                for deferred_event in current_state.deferred:
                    model.deferred_map[state_name][deferred_event] = True
                    model.deferred_owner_map[state_name].setdefault(deferred_event, current_path)
            if current_path in ("", "/", model.qualified_name):
                if current_path == model.qualified_name:
                    current_path = _parent_path(current_path)
                else:
                    break
            current_path = _parent_path(current_path)


def _build_timer_activity_table(model: Model) -> None:
    model.timer_activity_map.clear()
    model.timer_event_order.clear()
    for state_name, element in model.members.items():
        if not isinstance(element, StateNode):
            continue
        by_event: dict[str, str] = {}
        for index, transition_name in enumerate(element.transitions):
            transition = model.get(transition_name, TransitionNode)
            if transition is None:
                continue
            for event_name in transition.events:
                event = model.get(event_name, Event)
                if event is not None and event.kind == Kinds.TimeEvent:
                    model.timer_event_order.setdefault(event_name, index)
        for behavior_name in element.activity:
            behavior = model.get(behavior_name, BehaviorNode[typing.Any])
            if (
                behavior is None
                or not behavior.timer_repeating
                or behavior.timer_event is None
            ):
                continue
            by_event[behavior.timer_event.qualified_name] = behavior_name
        if by_event:
            model.timer_activity_map[state_name] = by_event


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
    _finalize_model(model)
    return model


def State(name: str, *elements: NamedElement) -> PartialState:
    return PartialState(qualified_name=name, owned_elements=list(elements))


def SubmachineState(name: str, machine: Model, *elements: NamedElement) -> PartialSubmachineState:
    return PartialSubmachineState(
        qualified_name=name,
        machine=machine,
        owned_elements=list(elements),
    )


def Initial(name_or_element: str | NamedElement, *elements: NamedElement) -> PartialInitial:
    name = ".initial"
    owned_elements = list(elements)
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.insert(0, name_or_element)
    return PartialInitial(qualified_name=name, owned_elements=owned_elements)


def Transition(name_or_element: str | PartialElement, *elements: NamedElement) -> PartialTransition:
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
    return PartialBehaviors(operations=list(operations), type=StateNode, qualified_name="entry")


def Exit(*operations: BehaviorArgument[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(operations=list(operations), type=StateNode, qualified_name="exit")


def Activity(*operations: BehaviorArgument[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(
        operations=list(operations),
        type=StateNode,
        concurrent=True,
        qualified_name="activity",
    )


def Effect(*operations: BehaviorArgument[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(operations=list(operations), type=TransitionNode, qualified_name="effect")


def Guard(expression: ExpressionArgument[TInstance]) -> PartialGuard[TInstance]:
    return PartialGuard(
        qualified_name=expression if isinstance(expression, str) else getattr(expression, "__name__", "guard"),
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
        return PartialAfter(duration=_duration_attribute(duration), repeating=False)
    return PartialAfter(duration=duration, repeating=False)


def At(timepoint: str | Timepoint[TInstance]) -> PartialAfter[TInstance]:
    if isinstance(timepoint, str):
        return PartialAfter(timepoint=_timepoint_attribute(timepoint), repeating=False)
    return PartialAfter(timepoint=timepoint, repeating=False)


def Every(duration: str | Duration[TInstance]) -> PartialAfter[TInstance]:
    if isinstance(duration, str):
        return PartialAfter(duration=_duration_attribute(duration), repeating=True)
    return PartialAfter(duration=duration, repeating=True)


def When(expression: str | WhenExpression[TInstance]) -> PartialOnSet | PartialWhen[TInstance]:
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
        history_type=ShallowHistoryVertex,
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
        history_type=DeepHistoryVertex,
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
    default = None if maybe_type_or_default is _ATTRIBUTE_DEFAULT_UNSET else maybe_type_or_default
    value_type = None if default is None else type(default)
    return PartialAttribute(qualified_name=name, default=default, value_type=value_type)


def New(instance: TInstance, model: Model, maybe_config: Config | None = None) -> HSM[TInstance]:
    return HSM(instance=instance, model=model, config=maybe_config)


async def _start_public(
    ctx: Context | None,
    instance: TInstance | HSM[TInstance],
    model: Model | typing.Any | None = None,
    data: typing.Any = None,
) -> HSM[TInstance]:
    if isinstance(instance, HSM):
        sm = instance
        if sm._started or sm._processing.locked():
            raise ValidationError("Start() called on an already started HSM")
        start_data = model
        base_context = ctx or Context()
        root_context = _WithRuntimeHSM(base_context, sm)
        if sm._root_context is not root_context:
            sm._root_context.unregister(sm)
        sm._base_context = base_context
        sm._root_context = root_context
        sm._reset_for_restart()
    else:
        if not isinstance(model, Model):
            raise ValidationError("Start() requires a model when starting an instance")
        existing = getattr(instance, "_Instance__hsm", None)
        if isinstance(existing, HSM) and (existing._started or existing._processing.locked()):
            raise ValidationError("Start() called on an instance that already has a running HSM")
        sm = HSM(instance=instance, model=model, ctx=ctx)
        start_data = data
    await sm._start(start_data)
    return sm


def Start(
    ctx: Context | None,
    instance: TInstance | HSM[TInstance],
    model: Model | typing.Any | None = None,
    data: typing.Any = None,
) -> typing.Awaitable[HSM[TInstance]]:
    return _start_public(ctx, instance, model, data)


async def _started_public(
    ctx: Context | None,
    instance: TInstance,
    model: Model,
    maybe_config: Config | None = None,
) -> HSM[TInstance]:
    existing = getattr(instance, "_Instance__hsm", None)
    if isinstance(existing, HSM) and (existing._started or existing._processing.locked()):
        raise ValidationError("Start() called on an instance that already has a running HSM")
    sm = New(instance, model, maybe_config)
    data = maybe_config.Data if maybe_config is not None else None
    return await Start(ctx, sm, data)


def Started(
    ctx: Context | None,
    instance: TInstance,
    model: Model,
    maybe_config: Config | None = None,
) -> typing.Awaitable[HSM[TInstance]]:
    return _started_public(ctx, instance, model, maybe_config)


def Stop(sm: typing.Union[HSM[TInstance], Instance, Group]) -> typing.Awaitable[None]:
    if isinstance(sm, Group):
        return sm.stop()
    machine = _resolve_machine(sm)
    return machine.stop()


def Restart(
    sm: typing.Union[HSM[TInstance], Instance, Group],
    data: typing.Any = None,
) -> typing.Awaitable[None]:
    if isinstance(sm, Group):
        return sm.restart(data)
    machine = _resolve_machine(sm)
    return machine.restart(data)


def Dispatch(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
    event: Event,
) -> typing.Awaitable[None]:
    source = _context_machine(ctx)
    if isinstance(hsm, Group):
        return hsm.dispatch(event, source)
    machine = _resolve_machine(hsm)
    return machine.dispatch(_clone_event_for_delivery(event, machine, source))


def DispatchAll(ctx: Context | None, event: Event) -> typing.Awaitable[None]:
    if ctx is None or ctx.done:
        return _completed_none()
    machines = [
        machine
        for machine in ctx.machines()
        if isinstance(machine, HSM) and machine._started
    ]
    source = _context_machine(ctx)
    if any(machine._deferred_events for machine in machines):
        return _dispatch_machines_sequential_started(
            (machine, _clone_event_for_delivery(event, machine, source)) for machine in machines
        )
    if _dispatch_from_processing(source):
        return _queue_dispatches_for_later(
            (machine, _clone_event_for_delivery(event, machine, source)) for machine in machines
        )
    return _dispatch_machines_ordered_task(
        (machine, _clone_event_for_delivery(event, machine, source)) for machine in machines
    )


def DispatchTo(ctx: Context | None, event: Event, *maybe_ids: str) -> typing.Awaitable[None]:
    if ctx is None or ctx.done:
        return _completed_none()
    machines = [
        machine
        for machine in ctx.machines()
        if isinstance(machine, HSM) and machine._started
    ]
    if maybe_ids:
        selected = []
        seen: builtins.set[int] = builtins.set()
        for maybe_id in maybe_ids:
            for machine in machines:
                if builtins.id(machine) in seen or not Match(machine.take_snapshot().ID, maybe_id):
                    continue
                selected.append(machine)
                seen.add(builtins.id(machine))
    else:
        selected = machines
    source = _context_machine(ctx)
    if any(machine._deferred_events for machine in selected):
        return _dispatch_machines_sequential_started(
            (machine, _clone_event_for_delivery(event, machine, source)) for machine in selected
        )
    if _dispatch_from_processing(source):
        return _queue_dispatches_for_later(
            (machine, _clone_event_for_delivery(event, machine, source)) for machine in selected
        )
    return _dispatch_machines_ordered_task(
        (machine, _clone_event_for_delivery(event, machine, source)) for machine in selected
    )


def Get(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
    name: str,
) -> tuple[typing.Any, bool]:
    if isinstance(hsm, Group):
        return hsm.get(name)
    machine = _resolve_machine(hsm)
    return machine.get(name)


def Set(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
    name: str,
    value: typing.Any,
) -> typing.Awaitable[None]:
    if isinstance(hsm, Group):
        return hsm.set(ctx, name, value)
    machine = _resolve_machine(hsm)
    return machine.set(name, value)


def Call(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
    name: str,
    *args: typing.Any,
) -> typing.Awaitable[typing.Any]:
    if isinstance(hsm, Group):
        return hsm.call(ctx, name, *args)
    machine = _resolve_machine(hsm)
    return machine.call(name, *args)


def TakeSnapshot(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
) -> Snapshot:
    if isinstance(hsm, Group):
        return hsm.take_snapshot()
    machine = _resolve_machine(hsm)
    return machine.take_snapshot()


define = Define
element = Element
validation_error = ValidationError
behavior = Behavior
model = Model
instance = Instance
state = State
submachine_state = SubmachineState
final_state = FinalState
initial = Initial
transition = Transition
source = Source
target = Target
entry = Entry
exit = Exit
activity = Activity
effect = Effect
guard = Guard
on = On
after = After
at = At
every = Every
when = When
defer = Defer
choice = Choice
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
is_ancestor = IsAncestor  # type: ignore[assignment]


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
    "Behavior",
    "BehaviorKind",
    "Call",
    "CallData",
    "CallEventKind",
    "ChangeEventKind",
    "Choice",
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
    "DefaultClock",
    "Dispatch",
    "DispatchAll",
    "DispatchTo",
    "Effect",
    "Element",
    "ElementKind",
    "Entry",
    "EntryPoint",
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
    "FinalState",
    "FinalStateKind",
    "FromContext",
    "Get",
    "Group",
    "Guard",
    "HSM",
    "ID",
    "Initial",
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
    "Queue",
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
    "State",
    "StateKind",
    "StateMachineKind",
    "SubmachineState",
    "SubmachineStateKind",
    "Stop",
    "TakeSnapshot",
    "Target",
    "TimeEventKind",
    "Transition",
    "TransitionKind",
    "ValidationError",
    "VertexKind",
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
    "context",
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
    model = Define("root", State("s1"), State("s2"), Initial(Target("s1")))
    print(model.members)
