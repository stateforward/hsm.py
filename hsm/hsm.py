from __future__ import annotations

import asyncio
import collections
import copy
import fnmatch
import inspect
import posixpath
import sys
import threading
import typing
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum

from .kind import IsKind, MakeKind, is_kind, make_kind

TElement = typing.TypeVar("TElement", bound="Element")
TInstance = typing.TypeVar("TInstance", bound="Instance")
TData = typing.TypeVar("TData", default=None)
TNewData = typing.TypeVar("TNewData")
_next_id_counter = 0

OperationCallback = typing.Callable[
    ["Context", TInstance, "Event"],
    typing.Awaitable[None] | None,
]
Expression = typing.Callable[
    ["Context", TInstance, "Event"],
    typing.Awaitable[bool] | bool,
]
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


def _future_done() -> asyncio.Future[None]:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.set_result(None)
    return future


async def _maybe_await(value: typing.Any) -> typing.Any:
    if inspect.isawaitable(value):
        return await typing.cast(typing.Awaitable[typing.Any], value)
    return value


async def _normalize_waitable(value: typing.Any) -> None:
    if value is None:
        return
    if isinstance(value, asyncio.Event):
        await value.wait()
        return
    if inspect.isawaitable(value):
        await typing.cast(typing.Awaitable[typing.Any], value)
        return
    wait = getattr(value, "wait", None)
    if callable(wait):
        result = wait()
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


def Match(value: str, *patterns: str) -> bool:
    if not patterns:
        return False
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


match = Match


class ValidationError(RuntimeError):
    pass


class Context:
    def __init__(self):
        self._done = False
        self._listeners: list[typing.Callable[[], None]] = []
        self._done_future: asyncio.Future[None] | None = None
        self._machines: weakref.WeakSet["HSM[typing.Any]"] = weakref.WeakSet()

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
        if self._done_future is None:
            self._done_future = asyncio.get_running_loop().create_future()
        await self._done_future

    def register(self, machine: "HSM[typing.Any]") -> None:
        self._machines.add(machine)

    def unregister(self, machine: "HSM[typing.Any]") -> None:
        try:
            self._machines.remove(machine)
        except KeyError:
            pass

    def machines(self) -> list["HSM[typing.Any]"]:
        return list(self._machines)


context = Context


class Kinds(IntEnum):
    Null = MakeKind()
    Element = MakeKind()
    Partial = MakeKind(Element)
    Namespace = MakeKind(Element)
    NamedElement = MakeKind(Element)
    Vertex = MakeKind(Element)
    State = MakeKind(Vertex, NamedElement, Namespace)
    FinalState = MakeKind(State)
    Transition = MakeKind(NamedElement)
    Pseudostate = MakeKind(Vertex)
    Initial = MakeKind(Pseudostate)
    Choice = MakeKind(Pseudostate)
    ShallowHistory = MakeKind(Pseudostate)
    DeepHistory = MakeKind(Pseudostate)
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
ChoiceKind = Kinds.Choice
ShallowHistoryKind = Kinds.ShallowHistory
DeepHistoryKind = Kinds.DeepHistory
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
choice_kind = ChoiceKind
shallow_history_kind = ShallowHistoryKind
deep_history_kind = DeepHistoryKind
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
        return posixpath.dirname(self.qualified_name)

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


async def noop_operation(ctx: Context, instance: Element, event: "Event") -> None:
    return None


@dataclass
class Behavior(typing.Generic[TInstance], NamedElement, Namespace):
    kind: int = Kinds.Behavior
    operation: OperationCallback[TInstance] = field(default=noop_operation)


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
class AttributeDef(NamedElement):
    kind: int = Kinds.Attribute
    declared_name: str = ""
    default: typing.Any = None
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


@dataclass
class Config:
    ID: str = ""
    Name: str = ""
    Data: typing.Any = None
    Clock: Clock | None = None


config = Config


@dataclass
class Model(State):
    events: dict[str, "Event[typing.Any]"] = field(default_factory=dict)
    attributes: dict[str, AttributeDef] = field(default_factory=dict)
    operations: dict[str, OperationDef] = field(default_factory=dict)
    transition_map: dict[str, dict[str, list[typing.Any]]] = field(default_factory=dict)
    deferred_map: dict[str, dict[str, bool]] = field(default_factory=dict)
    pending_oncall: typing.Set[str] = field(default_factory=set)

    def add(self, partial: PartialElement) -> None:
        self.owned_elements.append(partial)

    def get(
        self, name: str, *kinds: typing.Type[TElement]
    ) -> typing.Optional[TElement]:
        element = self.members.get(name)
        if element is None:
            return None
        bases = tuple(getattr(kind_value, "__origin__", kind_value) for kind_value in kinds)
        if bases and not isinstance(element, bases):
            return None
        return typing.cast(TElement, element)

    def set(self, qualified_name: str, element: typing.Union[Element, "Event[typing.Any]"]) -> None:
        self.members[qualified_name] = element
        if isinstance(element, Event):
            self.events[qualified_name] = element
        elif isinstance(element, AttributeDef):
            self.attributes[element.declared_name or qualified_name] = element
        elif isinstance(element, OperationDef):
            self.operations[element.declared_name or element.name()] = element


@dataclass
class Event(typing.Generic[TData]):
    name: str = field(default_factory=str)
    data: typing.Optional[TData] = field(default=None)
    kind: int = Kinds.Event
    id: str = field(default_factory=str)
    source: str = field(default_factory=str)
    target: str = field(default_factory=str)
    qualified_name: str = field(default_factory=str)
    schema: typing.Any = None

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
    def Data(self) -> typing.Optional[TData]:
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


@dataclass
class EventSnapshot:
    Name: str
    Kind: int
    Target: str
    Guard: bool
    Schema: typing.Any

    @property
    def name(self) -> str:
        return self.Name

    @property
    def kind(self) -> int:
        return self.Kind

    @property
    def target(self) -> str:
        return self.Target

    @property
    def guard(self) -> bool:
        return self.Guard

    @property
    def schema(self) -> typing.Any:
        return self.Schema


@dataclass
class Snapshot:
    ID: str = ""
    QualifiedName: str = ""
    State: str = ""
    Attributes: dict[str, typing.Any] | None = None
    QueueLen: int = 0
    Events: list[EventSnapshot] = field(default_factory=list)

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
    def attributes(self) -> dict[str, typing.Any] | None:
        return self.Attributes

    @property
    def queue_len(self) -> int:
        return self.QueueLen

    @property
    def events(self) -> list[EventSnapshot]:
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
class FinalState(State):
    kind: int = Kinds.FinalState


@dataclass
class TransitionPaths:
    enter: list[str] = field(default_factory=list)
    exit: list[str] = field(default_factory=list)


@dataclass
class Transition(NamedElement):
    kind: int = Kinds.Transition
    source: str = field(default_factory=str)
    target: str = field(default_factory=str)
    guard: str | None = None
    effect: list[str] = field(default_factory=list)
    events: list[str] = field(default_factory=list)
    paths: dict[str, TransitionPaths] = field(default_factory=dict)


BehaviorNode = Behavior
VertexNode = Vertex
StateNode = State
InitialNode = Initial
ChoiceNode = Choice
PseudostateNode = Pseudostate
ShallowHistoryNode = ShallowHistoryVertex
DeepHistoryNode = DeepHistoryVertex
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
        if not is_ancestor(state.qualified_name, initial_transition.target) and state.qualified_name != posixpath.dirname(initial_transition.target):
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
        owner_state = find(stack, StateNode)
        if owner_state is None or owner_state is model:
            name = self.history_type.__name__.replace("Vertex", "")
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: you must call {name}() within a nested State"
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
                    self.transition.paths[name] = TransitionPaths(enter=[], exit=[])
            return
        enter: list[str] = []
        entering = self.transition.target
        lca = LCA(self.transition.source, self.transition.target)
        while entering not in ("", "/", lca):
            enter.insert(0, entering)
            entering = posixpath.dirname(entering)
        if self.transition.kind == Kinds.Self:
            enter.append(self.transition.source)
        source_element = model.get(self.transition.source, VertexNode)
        if isinstance(source_element, InitialNode):
            self.transition.paths[posixpath.dirname(self.transition.source)] = TransitionPaths(enter=enter, exit=[])
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
                    exiting = posixpath.dirname(exiting)
            self.transition.paths[qualified_name] = TransitionPaths(enter=enter, exit=exit_path)


def LCA(a: str, b: str) -> str:
    if a == b:
        return posixpath.dirname(a)
    if not a:
        return b
    if not b:
        return a
    if posixpath.dirname(a) == posixpath.dirname(b):
        return posixpath.dirname(a)
    if IsAncestor(a, b):
        return a
    if IsAncestor(b, a):
        return b
    return LCA(posixpath.dirname(a), posixpath.dirname(b))


def least_common_ancestor(source: str, target: str) -> str:
    return LCA(source, target)


def IsAncestor(current: str, target: str) -> bool:
    current_norm = posixpath.normpath(current)
    target_norm = posixpath.normpath(target)
    if current_norm in ("", ".", target_norm):
        return False
    if current_norm == "/":
        return True
    parent = posixpath.dirname(target_norm)
    while parent not in ("", ".", "/"):
        if parent == current_norm:
            return True
        parent = posixpath.dirname(parent)
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
        if not transition.events and not isinstance(source_element, PseudostateNode):
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Transition "{transition.qualified_name}" has no events'
            )
        if transition.target == transition.source:
            transition.kind = Kinds.Self
        elif not transition.target:
            transition.kind = Kinds.Internal
        elif IsAncestor(transition.source, transition.target):
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
    operations: list[OperationCallback[TInstance]] = field(default_factory=list)
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
        for callback in self.operations:
            behavior = BehaviorNode(
                qualified_name=join(
                    element.qualified_name,
                    self.qualified_name,
                    getattr(callback, "__name__", "anonymous"),
                    str(len(behaviors)),
                ),
                operation=callback,
                kind=Kinds.Concurrent if self.concurrent else Kinds.Sequential,
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


GuardNode = Guard


@dataclass
class PartialGuard(typing.Generic[TInstance], PartialElement):
    expression: Expression[TInstance] = field(default=noop_expression)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: guard must be called within a Transition"
            )
        guard = GuardNode(
            qualified_name=join(transition.qualified_name, self.qualified_name),
            expression=self.expression,
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
class PartialAttribute(PartialElement):
    default: typing.Any = None
    implicit: bool = False

    def apply(self, model: Model, stack: list[NamedElement]) -> AttributeDef:
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
            implicit=self.implicit,
        )
        model.set(attribute.qualified_name, attribute)
        return attribute


@dataclass
class PartialOperationDeclaration(PartialElement):
    callback: OperationImplementation | None = None

    def apply(self, model: Model, stack: list[NamedElement]) -> OperationDef:
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
        model.pending_oncall.add(self.qualified_name)
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

        async def operation(ctx: Context, instance: TInstance, event: Event) -> None:
            while not ctx.is_done():
                if self.timepoint is None:
                    delta = await _maybe_await(self.duration(ctx, instance, event))
                else:
                    target = await _maybe_await(self.timepoint(ctx, instance, event))
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
                    if self.repeating:
                        return
                    return
                try:
                    await _clock_for_instance(instance).Sleep(delta)
                except asyncio.CancelledError:
                    return
                if ctx.is_done():
                    return
                instance.dispatch(self.event)
                if not self.repeating:
                    return

        behavior = BehaviorNode(
            qualified_name=join(source.qualified_name, self.event.name, str(len(model.members))),
            kind=Kinds.Concurrent,
            operation=operation,
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
        qualified_name = join(
            transition.qualified_name,
            getattr(self.timepoint or self.duration, "__name__", "duration"),
            str(len(model.members)),
        )
        event = Event(name=qualified_name, qualified_name=qualified_name, kind=Kinds.TimeEvent)
        model.set(event.qualified_name, event)
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
class PartialWhen(typing.Generic[TInstance], PartialElement):
    expression: WhenExpression[TInstance] = field(default=lambda *_: None)

    def apply(self, model: Model, stack: list[NamedElement]) -> None:
        transition = find(stack, TransitionNode)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: when must be called within a Transition"
            )
        source = model.get(transition.source or "", StateNode)
        if source is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: when can only be used on transitions where the source is a State"
            )
        qualified_name = join(
            transition.qualified_name,
            getattr(self.expression, "__name__", "when"),
            str(len(model.members)),
        )
        event = Event(name=qualified_name, qualified_name=qualified_name, kind=Kinds.ChangeEvent)
        model.set(event.qualified_name, event)
        transition.events.append(event.qualified_name)

        async def operation(ctx: Context, instance: TInstance, current_event: Event) -> None:
            try:
                result = await _maybe_await(self.expression(ctx, instance, current_event))
                await _normalize_waitable(result)
                if not ctx.is_done():
                    instance.dispatch(event)
            except asyncio.CancelledError:
                return

        behavior = BehaviorNode(
            qualified_name=join(source.qualified_name, event.name, str(len(model.members))),
            kind=Kinds.Concurrent,
            operation=operation,
        )
        source.activity.append(behavior.qualified_name)
        model.set(behavior.qualified_name, behavior)


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


def _segments_between(owner: str, target: str) -> list[str]:
    if owner == target:
        return []
    current = target
    segments: list[str] = []
    while current not in ("", owner):
        segments.insert(0, current)
        current = posixpath.dirname(current)
    return segments


class Mutex:
    def __init__(self) -> None:
        self._locked = False
        self._waiters: collections.deque[asyncio.Future[None]] = collections.deque()

    def try_acquire(self) -> bool:
        if self._locked:
            return False
        self._locked = True
        return True

    def locked(self) -> bool:
        return self._locked

    async def acquire(self) -> None:
        if not self._locked:
            self._locked = True
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

    def release(self) -> None:
        while self._waiters:
            future = self._waiters.popleft()
            if future.cancelled():
                continue
            future.set_result(None)
            return
        self._locked = False


class Queue:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._completion_events: collections.deque[Event] = collections.deque()
        self._regular_events: collections.deque[Event] = collections.deque()

    def push(self, event: Event) -> None:
        with self._lock:
            if is_kind(event.kind, Kinds.CompletionEvent):
                self._completion_events.appendleft(event)
            else:
                self._regular_events.append(event)

    async def pop(self) -> Event | None:
        with self._lock:
            if self._completion_events:
                return self._completion_events.popleft()
            if self._regular_events:
                return self._regular_events.popleft()
            return None

    def len(self) -> int:
        with self._lock:
            return len(self._completion_events) + len(self._regular_events)


@dataclass
class ActiveBehavior:
    context: Context
    task: asyncio.Task[None]


def _clock_for_instance(instance: typing.Any) -> Clock:
    machine = getattr(instance, "_Instance__hsm", None)
    if isinstance(machine, HSM):
        return machine.clock()
    return DefaultClock.with_defaults()


class Instance(Element):
    __hsm: typing.Optional["HSM[typing.Self]"] = None

    def dispatch(self, event: Event) -> typing.Awaitable[None]:
        if self.__hsm is None:
            return _future_done()
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

    async def stop(self) -> None:
        if self.__hsm is not None:
            await self.__hsm.stop()

    async def restart(self, data: typing.Any = None) -> None:
        if self.__hsm is not None:
            await self.__hsm.restart(data)

    Dispatch = dispatch
    State = state
    Context = context
    Clock = clock
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
        self._root_context = ctx or Context()
        self._runtime_context = Context()
        self._processing = Mutex()
        self._queue = Queue()
        self._active: dict[str, ActiveBehavior] = {}
        self._after = _AfterWaiters()
        self._state: VertexNode = model
        self._awaitable: typing.Awaitable[None] = _future_done()
        self._attributes = _default_attribute_values(model)
        self._history_shallow: dict[str, str] = {}
        self._history_deep: dict[str, str] = {}
        self._id = config.ID or _next_id()
        self._qualified_name = config.Name or model.qualified_name
        self._clock = (config.Clock or DefaultClock).with_defaults()
        self._started = False
        self._stopping = False
        self._stop_requested = False
        self._restart_requested: tuple[typing.Any] | None = None
        self._root_context.register(self)
        setattr(self._instance, "_Instance__hsm", self)

        async def operation(ctx: Context, inst: TInstance, event: Event) -> None:
            self._state = await self._enter(self.model, event, True)
            startup_deferred: list[Event] = []
            try:
                await self._drain_queue(startup_deferred)
            finally:
                for deferred_event in startup_deferred:
                    self._queue.push(deferred_event)

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

    def _ensure_accepting_events(self) -> None:
        if not self._started and not self._processing.locked():
            raise ValidationError("operation requires a started HSM")

    async def _start(self, data: typing.Any = None) -> None:
        await self._processing.acquire()
        try:
            await self._start_locked(data)
        except BaseException:
            self._cleanup_failed_start()
            raise
        finally:
            self._processing.release()

    async def _start_locked(self, data: typing.Any = None) -> None:
        initial_event = InitialEvent.WithData(data) if data is not None else InitialEvent
        await self._execute(self, initial_event)
        self._started = True

    def _cleanup_failed_start(self) -> None:
        for active in list(self._active.values()):
            active.context.cancel()
            if active.task is asyncio.current_task():
                continue
            active.task.cancel()
        self._active.clear()
        self._runtime_context.cancel()
        self._state = self.model
        self._started = False
        self._root_context.unregister(self)

    def _remember_history(self, leaf_name: str) -> None:
        current = leaf_name
        while current not in ("", "/", self.model.qualified_name):
            parent = posixpath.dirname(current)
            if parent in ("", ".", "/") or parent == current:
                break
            self._history_shallow[parent] = current
            current = parent
        current = leaf_name
        while current not in ("", ".", "/"):
            parent = posixpath.dirname(current)
            if parent in ("", ".", "/") or parent == current:
                break
            self._history_deep[parent] = leaf_name
            if parent == self.model.qualified_name:
                break
            current = parent

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

    async def _enter(self, vertex: VertexNode, event: Event, default_entry: bool) -> VertexNode:
        if isinstance(vertex, (ShallowHistoryNode, DeepHistoryNode)):
            owner_name = vertex.owner()
            remembered = (
                self._history_shallow.get(owner_name)
                if isinstance(vertex, ShallowHistoryNode)
                else self._history_deep.get(owner_name)
            )
            if remembered:
                return await self._follow_from_owner(owner_name, remembered, event)
            if vertex.transitions:
                transition = self.model.get(vertex.transitions[0], TransitionNode)
                if transition is not None:
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
            for behavior_name in vertex.entry:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    await self._execute(behavior, event)
            self._after._notify(self._after.entry, lambda expected: expected == vertex.qualified_name)
            for behavior_name in vertex.activity:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    await self._execute(behavior, event)
            if not default_entry or vertex.initial == "":
                return vertex
            initial = self.model.get(vertex.initial, VertexNode)
            if isinstance(initial, VertexNode) and initial.transitions:
                transition = self.model.get(initial.transitions[0], TransitionNode)
                if transition is not None:
                    return await self._transition(vertex, transition, event)
        return vertex

    async def _exit(self, vertex: VertexNode, event: Event) -> VertexNode:
        if isinstance(vertex, StateNode):
            for behavior_name in vertex.activity:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    await self._terminate(behavior)
            for behavior_name in vertex.exit:
                behavior = self.model.get(behavior_name, BehaviorNode[TInstance])
                if behavior is not None:
                    await self._execute(behavior, event)
            self._after._notify(self._after.exit, lambda expected: expected == vertex.qualified_name)
        return vertex

    async def _evaluate(self, guard: GuardNode[TInstance], event: Event) -> bool:
        try:
            result = await _maybe_await(guard.expression(self._runtime_context, self._instance, event))
            return bool(result)
        except Exception:
            return False

    async def _execute(self, behavior: BehaviorNode[TInstance], event: Event) -> None:
        try:
            if behavior.kind == Kinds.Concurrent:
                activity_ctx = Context()

                async def run_activity() -> None:
                    try:
                        await _maybe_await(behavior.operation(activity_ctx, self._instance, event))
                        self._after._notify(
                            self._after.executed,
                            lambda expected: expected == behavior.owner(),
                        )
                    except asyncio.CancelledError:
                        activity_ctx.cancel()
                    except Exception as error:
                        if activity_ctx.is_done():
                            return
                        activity_ctx.cancel()
                        self._dispatch_error(error)

                task = asyncio.create_task(run_activity(), name=behavior.qualified_name)
                self._active[behavior.qualified_name] = ActiveBehavior(context=activity_ctx, task=task)
                return
            await _maybe_await(behavior.operation(self._runtime_context, self._instance, event))
        except Exception as error:
            if is_kind(event.kind, Kinds.ErrorEvent):
                return
            self._dispatch_error(error)

    def _dispatch_error(self, error: Exception) -> None:
        if self._stopping:
            return
        try:
            self._dispatch_task(Event(name=ErrorEvent.name, data=error, kind=Kinds.ErrorEvent))
        except ValidationError:
            return

    async def _terminate(self, behavior: BehaviorNode[TInstance]) -> None:
        active = self._active.pop(behavior.qualified_name, None)
        if active is None:
            return
        active.context.cancel()
        if active.task is asyncio.current_task():
            return
        active.task.cancel()
        try:
            await active.task
        except asyncio.CancelledError:
            pass

    async def _enabled(self, source: StateNode, event: Event) -> TransitionNode | None:
        source_transitions = self.model.transition_map.get(source.qualified_name, {})
        ordered = [*source_transitions.get(event.qualified_name, []), *source_transitions.get(AnyEvent.qualified_name, [])]
        for transition in ordered:
            maybe_guard = self.model.get(transition.guard or "", GuardNode[TInstance])
            if maybe_guard is None:
                return transition
            if await self._evaluate(maybe_guard, event):
                return transition
        return None

    async def _should_retry_deferred(self, event: Event) -> bool:
        qualified_name = self._state.qualified_name
        while qualified_name:
            source = self.model.get(qualified_name, StateNode)
            if source is None:
                break
            if await self._enabled(source, event) is not None:
                return True
            if self.model.deferred_map.get(qualified_name, {}).get(event.qualified_name, False):
                return False
            qualified_name = source.owner()
        return True

    async def _process(self) -> None:
        deferred: list[Event] = []
        try:
            await self._drain_queue(deferred)
        finally:
            for deferred_event in deferred:
                self._queue.push(deferred_event)
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
            finally:
                self._processing.release()

    async def _drain_queue(self, deferred: list[Event]) -> None:
        local_events: collections.deque[Event] = collections.deque()
        event = await self._queue.pop()
        while event is not None:
            current_leaf = self._state
            qualified_name = current_leaf.qualified_name
            while qualified_name:
                source = self.model.get(qualified_name, StateNode)
                if source is None:
                    break
                transition = await self._enabled(source, event)
                if transition is not None:
                    self._state = await self._transition(current_leaf, transition, event)
                    break
                if self.model.deferred_map.get(qualified_name, {}).get(event.qualified_name, False):
                    deferred.append(event)
                    break
                qualified_name = source.owner()
            event_qualified_name = event.qualified_name
            self._after._notify(
                self._after.process,
                lambda expected: expected is None or expected == event_qualified_name,
            )
            if local_events:
                event = local_events.popleft()
            else:
                event = await self._queue.pop()
            if event is None and deferred:
                retry: list[Event] = []
                still_deferred: list[Event] = []
                for deferred_event in deferred:
                    if await self._should_retry_deferred(deferred_event):
                        retry.append(deferred_event)
                    else:
                        still_deferred.append(deferred_event)
                deferred[:] = still_deferred
                if retry:
                    local_events.extend(retry[1:])
                    event = retry[0]
        for deferred_event in deferred:
            self._queue.push(deferred_event)
        deferred.clear()

    async def _transition(self, current_leaf: VertexNode, transition: TransitionNode, event: Event) -> VertexNode:
        path = transition.paths.get(current_leaf.qualified_name)
        if path is None:
            return current_leaf
        if transition.kind != Kinds.Internal:
            self._remember_history(current_leaf.qualified_name)
        for exiting in path.exit:
            vertex = self.model.get(exiting, VertexNode)
            if vertex is not None:
                await self._exit(vertex, event)
        for effect_name in transition.effect:
            effect = self.model.get(effect_name, BehaviorNode[TInstance])
            if effect is not None:
                await self._execute(effect, event)
        if transition.kind == Kinds.Internal:
            return current_leaf
        current: VertexNode = current_leaf
        for entering in path.enter:
            vertex = self.model.get(entering, VertexNode)
            if vertex is None:
                continue
            current = await self._enter(vertex, event, entering == transition.target)
            if entering == transition.target:
                return current
        target = self.model.get(transition.target, VertexNode)
        return current if target is None else target

    def _dispatch_task(self, event: Event[typing.Any]) -> typing.Awaitable[None]:
        self._ensure_accepting_events()
        self._queue.push(event)
        self._after._notify(self._after.dispatch, lambda expected: expected == event.qualified_name)
        if self._processing.try_acquire():
            self._awaitable = asyncio.create_task(self._process())
        elif asyncio.current_task() is self._awaitable:
            return _future_done()
        return self._awaitable

    def dispatch(self, event: Event[typing.Any]) -> typing.Awaitable[None]:
        return asyncio.shield(self._dispatch_task(event))

    async def _stop_locked(self) -> None:
        final_event = Event(name=FinalEvent.name, kind=Kinds.CompletionEvent)
        self._stopping = True
        try:
            while self._state.qualified_name != self.model.qualified_name:
                await self._exit(self._state, final_event)
                parent = self.model.get(posixpath.dirname(self._state.qualified_name), VertexNode)
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
        self._runtime_context.cancel()
        self._state = self.model
        self._started = False
        self._root_context.unregister(self)

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
        await self.stop()
        self._reset_for_restart()
        await self._start(data)

    def _reset_for_restart(self) -> None:
        self._runtime_context = Context()
        self._queue = Queue()
        self._active.clear()
        self._attributes = _default_attribute_values(self.model)
        self._history_shallow.clear()
        self._history_deep.clear()
        self._state = self.model
        self._started = False
        self._stopping = False
        self._stop_requested = False
        self._restart_requested = None
        self._awaitable = _future_done()
        self._root_context.register(self)

    async def _restart_locked(self, data: typing.Any = None) -> None:
        await self._stop_locked()
        self._reset_for_restart()
        initial_event = InitialEvent.WithData(data) if data is not None else InitialEvent
        self._state = await self._enter(self.model, initial_event, True)
        self._started = True
        startup_deferred: list[Event] = []
        try:
            await self._drain_queue(startup_deferred)
        finally:
            for deferred_event in startup_deferred:
                self._queue.push(deferred_event)

    def get(self, name: str) -> tuple[typing.Any, bool]:
        qualified_name = _qualify_model_name(self.model.qualified_name, name)
        if qualified_name in self._attributes:
            return self._attributes[qualified_name], True
        return None, False

    async def set(self, name: str, value: typing.Any) -> None:
        self._ensure_accepting_events()
        qualified_name = _qualify_model_name(self.model.qualified_name, name)
        old_value = self._attributes.get(qualified_name)
        existed = qualified_name in self._attributes
        self._attributes[qualified_name] = value
        if existed and old_value == value:
            return
        if qualified_name not in self.model.attributes:
            self.model.attributes[qualified_name] = AttributeDef(
                qualified_name=qualified_name,
                declared_name=qualified_name,
                default=None,
                implicit=True,
            )
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

    async def call(self, name: str, *args: typing.Any) -> typing.Any:
        self._ensure_accepting_events()
        if not name:
            raise ValidationError("operation name cannot be empty")
        operation = self.model.operations.get(name)
        if operation is None:
            raise ValidationError(f'missing operation "{name}" for OnCall()')
        callback = operation.callback
        if callback is None:
            callback = getattr(self._instance, name, None)
        if callback is None:
            raise ValidationError(f'missing operation "{name}" for OnCall()')
        result = callback
        if inspect.ismethod(callback) or inspect.isfunction(callback):
            signature = inspect.signature(callback)
            parameters = list(signature.parameters.values())
            if len(parameters) >= 2 and parameters[0].name == "ctx":
                result = callback(self._runtime_context, self._instance, *args)
            else:
                result = callback(*args)
        event = Event(
            name=_oncall_event_name(name),
            qualified_name=_oncall_event_name(name),
            kind=Kinds.CallEvent,
            data=CallData(name=name, args=args),
            schema=CallData,
        )
        self.model.set(event.qualified_name, event)
        value = await _maybe_await(result)
        await self.dispatch(event)
        return value

    def take_snapshot(self) -> Snapshot:
        events: list[EventSnapshot] = []
        current_name = self._state.qualified_name
        for event_name, transitions in self.model.transition_map.get(current_name, {}).items():
            event = self.model.events.get(event_name)
            if event is None:
                continue
            for transition in transitions:
                events.append(
                    EventSnapshot(
                        Name=event_name,
                        Kind=event.kind,
                        Target=transition.target,
                        Guard=transition.guard is not None,
                        Schema=event.schema,
                    )
                )
        return Snapshot(
            ID=self._id,
            QualifiedName=self._qualified_name,
            State=self._state.qualified_name,
            Attributes=copy.deepcopy(self._attributes),
            QueueLen=self._queue.len(),
            Events=events,
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
    def __init__(self, *instances: typing.Union[Instance, "Group", None]):
        self.instances: list[Instance] = []
        for instance in instances:
            if instance is None:
                continue
            if isinstance(instance, Group):
                self.instances.extend(instance.instances)
            else:
                self.instances.append(instance)
        self.id = _next_id()

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

    async def dispatch(self, event: Event) -> None:
        await asyncio.gather(*(instance.dispatch(event) for instance in self.instances if instance is not None))

    async def stop(self) -> None:
        await asyncio.gather(*(instance.stop() for instance in self.instances if instance is not None))

    async def restart(self, data: typing.Any = None) -> None:
        await asyncio.gather(*(instance.restart(data) for instance in self.instances if instance is not None))

    def get(self, name: str) -> tuple[typing.Any, bool]:
        if not self.instances:
            return None, False
        return Get(self.context(), self.instances[0], name)

    async def set(self, ctx: Context | None, name: str, value: typing.Any) -> None:
        await asyncio.gather(*(Set(ctx, instance, name, value) for instance in self.instances if instance is not None))

    async def call(self, ctx: Context | None, name: str, *args: typing.Any) -> typing.Any:
        if not self.instances:
            raise ValidationError("missing hsm")
        return await Call(ctx, self.instances[0], name, *args)

    def take_snapshot(self) -> Snapshot:
        return Snapshot(ID=self.id, QualifiedName="", State="", Attributes=None, QueueLen=0, Events=[])


def NewGroup(*instances: typing.Union[Instance, Group, None]) -> Group:
    return Group(*instances)


MakeGroup = NewGroup


def _new_future() -> asyncio.Future[None]:
    return asyncio.get_running_loop().create_future()


def _after_future(waiters: list[tuple[typing.Any, asyncio.Future[None]]], expected: typing.Any) -> asyncio.Future[None]:
    future = _new_future()
    waiters.append((expected, future))
    return future


def AfterDispatch(ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], event: Event) -> asyncio.Future[None]:
    machine = _resolve_machine(hsm)
    return _after_future(machine._after.dispatch, event.qualified_name)


def AfterProcess(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance],
    maybe_event: Event | None = None,
) -> asyncio.Future[None]:
    machine = _resolve_machine(hsm)
    return _after_future(machine._after.process, None if maybe_event is None else maybe_event.qualified_name)


def AfterEntry(ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], state: str) -> asyncio.Future[None]:
    machine = _resolve_machine(hsm)
    return _after_future(machine._after.entry, state)


def AfterExit(ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], state: str) -> asyncio.Future[None]:
    machine = _resolve_machine(hsm)
    return _after_future(machine._after.exit, state)


def AfterExecuted(ctx: Context | None, hsm: typing.Union[HSM[TInstance], Instance], state: str) -> asyncio.Future[None]:
    machine = _resolve_machine(hsm)
    return _after_future(machine._after.executed, state)


def _resolve_machine(sm: typing.Union[HSM[TInstance], Instance]) -> HSM[TInstance]:
    if isinstance(sm, HSM):
        return sm
    hsm = getattr(sm, "_Instance__hsm", None)
    if hsm is None:
        raise ValidationError("missing hsm")
    return hsm


def _event_from_name(event_or_name: str | Event, kind_value: int = Kinds.Event) -> Event:
    if isinstance(event_or_name, Event):
        return event_or_name
    if event_or_name == AnyEvent.name:
        return AnyEvent
    return Event(name=event_or_name, kind=kind_value)


def _finalize_model(model: Model) -> None:
    if not model.initial:
        raise ValidationError("initial state is required for state machine")
    if model.entry:
        raise ValidationError("entry actions are not allowed on top level state machine")
    if model.exit:
        raise ValidationError("exit actions are not allowed on top level state machine")
    for name in model.pending_oncall:
        if name not in model.operations:
            raise ValidationError(f'missing operation "{name}" for OnCall()')
    _build_transition_table(model)
    _build_deferred_table(model)


def _build_transition_table(model: Model) -> None:
    model.transition_map.clear()
    for state_name, element in model.members.items():
        if not isinstance(element, StateNode):
            continue
        model.transition_map[state_name] = {}
        transitions_by_event: dict[str, list[tuple[TransitionNode, int, int]]] = {}
        current_path = state_name
        depth = 0
        while current_path:
            current_state = model.members.get(current_path)
            if isinstance(current_state, StateNode):
                for index, transition_name in enumerate(current_state.transitions):
                    transition = model.get(transition_name, TransitionNode)
                    if transition is None or not transition.events:
                        continue
                    for event_name in transition.events:
                        transitions_by_event.setdefault(event_name, []).append((transition, depth, index))
            if current_path in ("", "/", model.qualified_name):
                if current_path == model.qualified_name:
                    current_path = posixpath.dirname(current_path)
                else:
                    break
            current_path = posixpath.dirname(current_path)
            depth += 1
        for event_name, transitions in transitions_by_event.items():
            transitions.sort(key=lambda item: (item[1], item[2]))
            model.transition_map[state_name][event_name] = [item[0] for item in transitions]


def _build_deferred_table(model: Model) -> None:
    model.deferred_map.clear()
    for state_name, element in model.members.items():
        if not isinstance(element, StateNode):
            continue
        model.deferred_map[state_name] = {}
        current_path = state_name
        while current_path:
            current_state = model.members.get(current_path)
            if isinstance(current_state, StateNode):
                for deferred_event in current_state.deferred:
                    transitions = model.transition_map[state_name].get(deferred_event, [])
                    if transitions and any(transition.source == state_name for transition in transitions):
                        continue
                    model.deferred_map[state_name][deferred_event] = True
            if current_path in ("", "/", model.qualified_name):
                if current_path == model.qualified_name:
                    current_path = posixpath.dirname(current_path)
                else:
                    break
            current_path = posixpath.dirname(current_path)


def Define(name: str, *elements: NamedElement) -> Model:
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


def Entry(*operations: OperationCallback[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(operations=list(operations), type=StateNode, qualified_name="entry")


def Exit(*operations: OperationCallback[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(operations=list(operations), type=StateNode, qualified_name="exit")


def Activity(*operations: OperationCallback[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(
        operations=list(operations),
        type=StateNode,
        concurrent=True,
        qualified_name="activity",
    )


def Effect(*operations: OperationCallback[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors(operations=list(operations), type=TransitionNode, qualified_name="effect")


def Guard(expression: Expression[TInstance]) -> PartialGuard[TInstance]:
    return PartialGuard(qualified_name=getattr(expression, "__name__", "guard"), expression=expression)


def On(*events: str | Event) -> PartialTrigger:
    return PartialTrigger(events=[_event_from_name(event) for event in events])


def OnSet(name: str) -> PartialOnSet:
    return PartialOnSet(qualified_name=name)


def OnCall(name: str) -> PartialOnCall:
    return PartialOnCall(qualified_name=name)


def After(duration: Duration[TInstance]) -> PartialAfter[TInstance]:
    return PartialAfter(duration=duration, repeating=False)


def At(timepoint: Timepoint[TInstance]) -> PartialAfter[TInstance]:
    return PartialAfter(timepoint=timepoint, repeating=False)


def Every(duration: Duration[TInstance]) -> PartialAfter[TInstance]:
    return PartialAfter(duration=duration, repeating=True)


def When(expression: WhenExpression[TInstance]) -> PartialWhen[TInstance]:
    return PartialWhen(expression=expression)


def Defer(*events: Event) -> PartialDefer:
    return PartialDefer(events=list(events))


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


def Attribute(name: str, maybe_default: typing.Any = None) -> PartialAttribute:
    return PartialAttribute(qualified_name=name, default=maybe_default)


def New(instance: TInstance, model: Model, maybe_config: Config | None = None) -> HSM[TInstance]:
    return HSM(instance=instance, model=model, config=maybe_config)


async def Start(
    ctx: Context | None,
    instance: TInstance | HSM[TInstance],
    model: Model | typing.Any | None = None,
    data: typing.Any = None,
) -> HSM[TInstance]:
    if isinstance(instance, HSM):
        sm = instance
        if sm._started:
            raise ValidationError("Start() called on an already started HSM")
        start_data = model
        sm._root_context = ctx or Context()
        sm._reset_for_restart()
    else:
        if not isinstance(model, Model):
            raise ValidationError("Start() requires a model when starting an instance")
        existing = getattr(instance, "_Instance__hsm", None)
        if isinstance(existing, HSM) and existing._started:
            raise ValidationError("Start() called on an instance that already has a running HSM")
        sm = HSM(instance=instance, model=model, ctx=ctx)
        start_data = data
    await sm._start(start_data)
    return sm


async def Started(
    ctx: Context | None,
    instance: TInstance,
    model: Model,
    maybe_config: Config | None = None,
) -> HSM[TInstance]:
    sm = New(instance, model, maybe_config)
    data = maybe_config.Data if maybe_config is not None else None
    return await Start(ctx, sm, data)


async def Stop(sm: typing.Union[HSM[TInstance], Instance, Group]) -> None:
    if isinstance(sm, Group):
        await sm.stop()
        return
    machine = _resolve_machine(sm)
    await machine.stop()


async def Restart(
    sm: typing.Union[HSM[TInstance], Instance, Group],
    data: typing.Any = None,
) -> None:
    if isinstance(sm, Group):
        await sm.restart(data)
        return
    machine = _resolve_machine(sm)
    await machine.restart(data)


async def Dispatch(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
    event: Event,
) -> None:
    if isinstance(hsm, Group):
        await hsm.dispatch(event)
        return
    machine = _resolve_machine(hsm)
    await machine.dispatch(event)


async def DispatchAll(ctx: Context | None, event: Event) -> None:
    if ctx is None:
        return
    machines = [machine for machine in ctx.machines() if machine._started]
    await asyncio.gather(*(machine.dispatch(event) for machine in machines))


async def DispatchTo(ctx: Context | None, event: Event, *maybe_ids: str) -> None:
    if ctx is None:
        return
    selected = [
        machine
        for machine in ctx.machines()
        if machine._started and (not maybe_ids or Match(machine.take_snapshot().ID, *maybe_ids))
    ]
    await asyncio.gather(*(machine.dispatch(event) for machine in selected))


def Get(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
    name: str,
) -> tuple[typing.Any, bool]:
    if isinstance(hsm, Group):
        return hsm.get(name)
    machine = _resolve_machine(hsm)
    return machine.get(name)


async def Set(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
    name: str,
    value: typing.Any,
) -> None:
    if isinstance(hsm, Group):
        await hsm.set(ctx, name, value)
        return
    machine = _resolve_machine(hsm)
    await machine.set(name, value)


async def Call(
    ctx: Context | None,
    hsm: typing.Union[HSM[TInstance], Instance, Group],
    name: str,
    *args: typing.Any,
) -> typing.Any:
    if isinstance(hsm, Group):
        return await hsm.call(ctx, name, *args)
    machine = _resolve_machine(hsm)
    return await machine.call(name, *args)


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
    "ErrorEvent",
    "ErrorEventKind",
    "Event",
    "EventKind",
    "EventSnapshot",
    "Every",
    "Exit",
    "Expression",
    "ExternalKind",
    "Final",
    "FinalEvent",
    "FinalState",
    "FinalStateKind",
    "Get",
    "Guard",
    "HSM",
    "ID",
    "Initial",
    "InitialEvent",
    "InitialKind",
    "InfiniteDuration",
    "Instance",
    "InternalKind",
    "IsAncestor",
    "IsKind",
    "Kinds",
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
    "error_event",
    "error_event_kind",
    "event",
    "event_snapshot",
    "event_kind",
    "every",
    "exit",
    "expression",
    "external_kind",
    "final",
    "final_event",
    "final_state",
    "final_state_kind",
    "get",
    "guard",
    "id",
    "initial",
    "initial_event",
    "initial_kind",
    "infinite_duration",
    "instance",
    "internal_kind",
    "is_ancestor",
    "is_kind",
    "kinds",
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


if __name__ == "__main__":
    model = Define("root", State("s1"), State("s2"), Initial(Target("s1")))
    print(model.members)
