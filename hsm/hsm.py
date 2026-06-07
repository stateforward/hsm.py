from __future__ import annotations

import asyncio
import collections
import collections.abc
import posixpath
import dataclasses
import functools
import types
import typing
import traceback
import threading
import datetime
import abc
import re
import fnmatch
import weakref
import inspect

from hsm import muid
from . import context
from . import kind
from . import generic

TElement = typing.TypeVar("TElement", bound="Element")
TInstance = typing.TypeVar("TInstance", bound="Instance")
TData = typing.TypeVar("TData", default=typing.Any)
TEventData = typing.TypeVar("TEventData", default=typing.Any, covariant=True)
TKey = typing.TypeVar("TKey", bound=typing.Hashable)
TValue = typing.TypeVar("TValue")
TModel = typing.TypeVar("TModel", bound="Model", covariant=True, default="Model")
TReturn = typing.TypeVar("TReturn")


Expression = typing.Callable[[context.Context, TInstance, "Event"], TReturn]

TimeExpression = Expression[
    TInstance,
    collections.abc.Awaitable[datetime.datetime | datetime.timedelta]
    | datetime.datetime
    | datetime.timedelta,
]

OperationExpression = typing.Callable[
    [context.Context, TInstance, "Event"],
    collections.abc.Coroutine[None, None, None] | None,
]


NullKind = kind.Make()
ElementKind = kind.Make()
NamespaceKind = kind.Make(ElementKind)
VertexKind = kind.Make(ElementKind)
StateKind = kind.Make(VertexKind, ElementKind, NamespaceKind)
SubmachineStateKind = kind.Make(StateKind)
FinalStateKind = kind.Make(StateKind)
TransitionKind = kind.Make(ElementKind)
PseudostateKind = kind.Make(VertexKind)
InitialKind = kind.Make(PseudostateKind)
ChoiceKind = kind.Make(PseudostateKind)
ShallowHistoryKind = kind.Make(PseudostateKind)
DeepHistoryKind = kind.Make(PseudostateKind)
EntryPointKind = kind.Make(PseudostateKind)
ExitPointKind = kind.Make(PseudostateKind)
ExternalKind = kind.Make(TransitionKind)
SelfKind = kind.Make(TransitionKind)
InternalKind = kind.Make(TransitionKind)
LocalKind = kind.Make(TransitionKind)
BehaviorKind = kind.Make(ElementKind)
StateMachineKind = kind.Make(BehaviorKind, NamespaceKind)
ConcurrentKind = kind.Make(BehaviorKind)
SequentialKind = kind.Make(BehaviorKind)
ConstraintKind = kind.Make(ElementKind)
EventKind = kind.Make(ElementKind)
CompletionEventKind = kind.Make(EventKind)
ErrorEventKind = kind.Make(CompletionEventKind)
TimeEventKind = kind.Make(EventKind)
ChangeEventKind = kind.Make(EventKind)
CallEventKind = kind.Make(EventKind)
AttributeKind = kind.Make(ElementKind)
OperationKind = kind.Make(ElementKind)


class ErrorMissingHSM(Exception):
    pass


class ErrorInvalidState(Exception):
    pass


class ErrorMissingOperation(Exception):
    pass


class ErrorInvalidOperation(Exception):
    pass


class ErrorAlreadyStarted(Exception):
    pass


class ErrorValidatingModel(Exception):
    def __init__(self, location: Location, message: str):
        super().__init__(f"{location.filename}:{location.lineno}: {message}")


ValidationError = ErrorValidatingModel


@dataclasses.dataclass(frozen=True, kw_only=True)
class Location:
    filename: str
    lineno: int

    @classmethod
    def capture(cls) -> "Location":
        stack = traceback.extract_stack()
        for frame in reversed(stack[:-1]):
            if frame.filename != __file__:
                return cls(filename=frame.filename, lineno=frame.lineno or 0)
        frame = stack[-2]
        return cls(filename=frame.filename, lineno=frame.lineno or 0)


@dataclasses.dataclass(kw_only=True)
class Element:
    kind: kind.Kind = ElementKind
    id: str = dataclasses.field(default="")
    qualified_name: str = dataclasses.field(default="")
    owned_elements: list[Element] = dataclasses.field(default_factory=list)
    location: Location = dataclasses.field(default_factory=Location.capture)

    def __init_subclass__(cls, kind: kind.Kind | None = None) -> None:
        super().__init_subclass__()
        cls.kind = kind or cls.kind

    def __post_init__(self) -> None:
        if self.kind == ElementKind and type(self).kind != ElementKind:
            self.kind = type(self).kind

    def Kind(self) -> kind.Kind:
        return self.kind

    def ID(self) -> str:
        return self.id

    def QualifiedName(self) -> str:
        return self.qualified_name

    def owner(self) -> str:
        if self.qualified_name in ("", "/"):
            return ""
        return posixpath.dirname(self.qualified_name)

    Owner: typing.Callable[[typing.Self], str] = owner

    def name(self) -> str:
        return posixpath.basename(self.qualified_name)

    Name: typing.Callable[[typing.Self], str] = name


@dataclasses.dataclass(kw_only=True)
class NamespaceElement(Element, kind=NamespaceKind):
    members: dict[str, Element] = dataclasses.field(default_factory=dict)


@typing.runtime_checkable
class Redefinable(typing.Protocol[TElement]):
    def redefine(
        self,
        model: "Model",
        stack: list[Element],
        element: TElement | None = None,
    ) -> TElement | None: ...


@dataclasses.dataclass(kw_only=True)
class RedefinableElement(Element, typing.Generic[TElement]):
    def redefine(
        self,
        model: "Model",
        stack: list[Element],
        element: TElement | None = None,
    ) -> TElement | None:
        substack = [*stack, element] if element is not None else stack
        for owned_element in self.owned_elements:
            if isinstance(owned_element, Redefinable):
                _ = typing.cast(Redefinable[TElement], owned_element).redefine(
                    model,
                    substack,
                )
        return element


@dataclasses.dataclass(kw_only=True)
class BehaviorElement(NamespaceElement, typing.Generic[TInstance], kind=BehaviorKind):
    operation: OperationExpression[TInstance] = dataclasses.field(
        default=lambda ctx, instance, event: None
    )


@dataclasses.dataclass(kw_only=True)
class VertexElement(Element, kind=VertexKind):
    transitions: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(kw_only=True)
class StateElement(VertexElement, NamespaceElement, kind=StateKind):
    initial: str = dataclasses.field(default_factory=str)
    entry: list[str] = dataclasses.field(default_factory=list)
    exit: list[str] = dataclasses.field(default_factory=list)
    activity: list[str] = dataclasses.field(default_factory=list)
    deferred: list[str] = dataclasses.field(default_factory=list)
    submachine: "Model | None" = None


@dataclasses.dataclass(kw_only=True)
class AttributeElement(Element, kind=AttributeKind):
    declared_name: str = ""
    default: typing.Any = None
    value_type: type[typing.Any] | None = None
    dynamic: bool = False
    implicit: bool = False


@dataclasses.dataclass(kw_only=True)
class OperationElement(Element, typing.Generic[TInstance], kind=OperationKind):
    method: OperationExpression[TInstance] | None = dataclasses.field(default=None)


@dataclasses.dataclass(kw_only=True)
class PseudostateElement(VertexElement, kind=PseudostateKind):
    pass


@dataclasses.dataclass(kw_only=True)
class InitialElement(PseudostateElement, kind=InitialKind):
    pass


@dataclasses.dataclass(kw_only=True)
class EntryPointElement(PseudostateElement, kind=EntryPointKind):
    pass


@dataclasses.dataclass(kw_only=True)
class ChoiceElement(PseudostateElement, kind=ChoiceKind):
    pass


@dataclasses.dataclass(kw_only=True)
class ShallowHistoryElement(PseudostateElement, kind=ShallowHistoryKind):
    pass


@dataclasses.dataclass(kw_only=True)
class DeepHistoryElement(PseudostateElement, kind=DeepHistoryKind):
    pass


@dataclasses.dataclass(kw_only=True)
class ExitPointElement(PseudostateElement, kind=ExitPointKind):
    pass


@dataclasses.dataclass(kw_only=True)
class FinalStateElement(StateElement, kind=FinalStateKind):
    pass


@dataclasses.dataclass(kw_only=True)
class ConstraintElement(Element, typing.Generic[TInstance], kind=ConstraintKind):
    expression: Expression[TInstance, bool]


@dataclasses.dataclass(kw_only=True, frozen=True)
class Event(typing.Generic[TEventData]):
    name: str = dataclasses.field(default_factory=str)
    data: TEventData | None = dataclasses.field(default=None)
    kind: int = EventKind
    id: str = dataclasses.field(default_factory=str)
    source: str = dataclasses.field(default_factory=str)
    target: str = dataclasses.field(default_factory=str)
    schema: object | None = None

    def WithData[TNewData](self, data: TNewData) -> "Event[TNewData]":
        return Event(
            name=self.name,
            data=data,
            kind=self.kind,
            id=self.id,
            source=self.source,
            target=self.target,
            schema=self.schema,
        )

    def WithDataAndID[TNewData](self, data: TNewData, id: str) -> "Event[TNewData]":
        return Event(
            name=self.name,
            data=data,
            kind=self.kind,
            id=id,
            source=self.source,
            target=self.target,
            schema=self.schema,
        )

    def with_data[TNewData](self, data: TNewData) -> "Event[TNewData]":
        return self.WithData(data)

    def with_data_and_id[TNewData](self, data: TNewData, id: str) -> "Event[TNewData]":
        return self.WithDataAndID(data, id)

    @property
    def Name(self) -> str:
        return self.name

    @property
    def Data(self) -> TEventData | None:
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
    def Schema(self) -> object | None:
        return self.schema


@dataclasses.dataclass(frozen=True, kw_only=True)
class CompletionEvent(Event[TEventData]):
    def __init__(self, name: str = "", data: TEventData | None = None):
        super().__init__(name=name, data=data, kind=CompletionEventKind)


@dataclasses.dataclass(kw_only=True)
class TransitionPath:
    enter: list[str] = dataclasses.field(default_factory=list)
    exit: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(kw_only=True)
class TransitionElement(Element, kind=TransitionKind):
    source: str = dataclasses.field(default_factory=str)
    target: str = dataclasses.field(default_factory=str)
    guard: str | None = None
    effect: list[str] = dataclasses.field(default_factory=list)
    events: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(kw_only=True)
class ModelValidator(abc.ABC):
    validated: list[str] = dataclasses.field(default_factory=list)
    pattern: typing.ClassVar[re.Pattern[str]] = re.compile(r"(?<!^)(?=[A-Z][a-z])")

    def _to_snake_case(self, name: str) -> str:
        return self.pattern.sub(r"_", name).lower()

    def validate(self, model: "Model") -> None:
        self.validated.clear()
        for qualified_name, member in model.members.items():
            if qualified_name in self.validated:
                continue
            self.validated.append(qualified_name)
            for cls in type(member).__mro__:
                method_name = self._to_snake_case(cls.__name__)
                if method_name.endswith("_element"):
                    method_name = method_name[: -len("_element")]
                method = getattr(self, f"_validate_{method_name}", None)
                if method is not None:
                    method(model, member)
                    break


class ModelFinalizer(typing.Protocol):
    def finalize(self, model: "Model") -> "Model": ...


@dataclasses.dataclass(kw_only=True)
class ValidatorElement(Element):
    validator: ModelValidator


@dataclasses.dataclass(kw_only=True)
class FinalizerElement(Element):
    finalizer: ModelFinalizer


class DefaultModelValidator(ModelValidator):
    def _validate_member(
        self,
        model: "Model",
        qualified_name: str,
        expected_type: type[Element],
        location: Location,
        role: str,
        owner: str,
    ) -> Element:
        member = model.members.get(qualified_name)
        if not isinstance(member, expected_type):
            role_name = role
            if expected_type is VertexElement:
                role_name = f"{role} Vertex"
            raise ErrorValidatingModel(
                location,
                f"{role_name} '{qualified_name}' not found for {owner}",
            )
        return member

    def _validate_model(self, model: "Model", member: Model) -> None:
        model_name = member.qualified_name.removeprefix("/")
        if "/" in model_name:
            raise ErrorValidatingModel(
                member.location,
                f"model name '{model_name}' cannot contain '/'",
            )
        if member.initial == "":
            raise ErrorValidatingModel(
                member.location,
                "initial state is required for state machine",
            )
        if member.entry:
            raise ErrorValidatingModel(
                member.location,
                "entry actions are not allowed on top level state machine",
            )
        if member.exit:
            raise ErrorValidatingModel(
                member.location,
                "exit actions are not allowed on top level state machine",
            )
        self._validate_state(model, member)

    def _validate_state(self, model: "Model", state: StateElement) -> None:
        initial_vertices = [
            member
            for member in model.members.values()
            if isinstance(member, InitialElement)
            and member.owner() == state.qualified_name
        ]
        if len(initial_vertices) > 1:
            raise ErrorValidatingModel(
                state.location,
                f"state '{state.qualified_name}' has more than one initial vertex",
            )
        if state.initial:
            _ = self._validate_member(
                model,
                state.initial,
                InitialElement,
                state.location,
                "initial",
                f"state '{state.qualified_name}'",
            )
        shallow_history_vertices = [
            member
            for member in model.members.values()
            if isinstance(member, ShallowHistoryElement)
            and member.owner() == state.qualified_name
        ]
        if len(shallow_history_vertices) > 1:
            raise ErrorValidatingModel(
                state.location,
                f"state '{state.qualified_name}' has more than one shallow history vertex",
            )
        deep_history_vertices = [
            member
            for member in model.members.values()
            if isinstance(member, DeepHistoryElement)
            and member.owner() == state.qualified_name
        ]
        if len(deep_history_vertices) > 1:
            raise ErrorValidatingModel(
                state.location,
                f"state '{state.qualified_name}' has more than one deep history vertex",
            )
        nested_states = [
            member
            for member in model.members.values()
            if isinstance(member, StateElement)
            and member.owner() == state.qualified_name
        ]
        if state.submachine is not None and (state.initial or nested_states):
            raise ErrorValidatingModel(
                state.location,
                "state cannot have both a submachine and nested states",
            )
        for entry in state.entry:
            behavior = self._validate_member(
                model,
                entry,
                BehaviorElement,
                state.location,
                "entry",
                f"state '{state.qualified_name}'",
            )
            if inspect.iscoroutinefunction(
                typing.cast(BehaviorElement[typing.Any], behavior).operation
            ):
                raise ErrorValidatingModel(
                    behavior.location,
                    "entry must be a synchronous function",
                )
        for exit in state.exit:
            behavior = self._validate_member(
                model,
                exit,
                BehaviorElement,
                state.location,
                "exit",
                f"state '{state.qualified_name}'",
            )
            if inspect.iscoroutinefunction(
                typing.cast(BehaviorElement[typing.Any], behavior).operation
            ):
                raise ErrorValidatingModel(
                    behavior.location,
                    "exit must be a synchronous function",
                )
        for activity in state.activity:
            _ = self._validate_member(
                model,
                activity,
                BehaviorElement,
                state.location,
                "activity",
                f"state '{state.qualified_name}'",
            )

    def _validate_pseudostate(
        self, model: "Model", pseudostate: PseudostateElement
    ) -> None:
        if isinstance(pseudostate, InitialElement) and len(pseudostate.transitions) > 1:
            raise ErrorValidatingModel(
                pseudostate.location,
                f"initial vertex '{pseudostate.qualified_name}' has more than one outgoing transition",
            )
        if isinstance(pseudostate, EntryPointElement):
            if len(pseudostate.transitions) > 1:
                raise ErrorValidatingModel(
                    pseudostate.location,
                    f"entry point '{pseudostate.qualified_name}' has more than one outgoing transition",
                )
            if pseudostate.transitions:
                transition = get(model, pseudostate.transitions[0], TransitionElement)
                if transition is not None and transition.guard is not None:
                    raise ErrorValidatingModel(
                        transition.location,
                        "entry point cannot have a guard",
                    )
        if isinstance(pseudostate, ExitPointElement):
            if len(pseudostate.transitions) > 1:
                raise ErrorValidatingModel(
                    pseudostate.location,
                    f"exit point '{pseudostate.qualified_name}' has more than one outgoing transition",
                )
            if pseudostate.transitions:
                transition = get(model, pseudostate.transitions[0], TransitionElement)
                if transition is not None and transition.guard is not None:
                    raise ErrorValidatingModel(
                        transition.location,
                        "exit point cannot have a guard",
                    )
        if (
            isinstance(pseudostate, (ShallowHistoryElement, DeepHistoryElement))
            and len(pseudostate.transitions) > 1
        ):
            raise ErrorValidatingModel(
                pseudostate.location,
                f"history vertex '{pseudostate.qualified_name}' has more than one outgoing transition",
            )

    def _validate_transition(
        self, model: "Model", transition: TransitionElement
    ) -> None:
        if transition.source == "":
            raise ErrorValidatingModel(
                transition.location,
                "transition source is required",
            )
        source = self._validate_member(
            model,
            transition.source,
            VertexElement,
            transition.location,
            "source",
            f"transition '{transition.qualified_name}'",
        )
        if transition.target == "" and isinstance(source, PseudostateElement):
            raise ErrorValidatingModel(
                transition.location,
                f"target is required for transition '{transition.qualified_name}'",
            )
        if isinstance(source, InitialElement):
            extra_events = [
                event for event in transition.events if event != InitialEvent.name
            ]
            if extra_events:
                raise ErrorValidatingModel(
                    transition.location,
                    f"initial transition '{transition.qualified_name}' cannot have triggers",
                )
            if transition.guard is not None:
                raise ErrorValidatingModel(
                    transition.location,
                    f"initial transition '{transition.qualified_name}' cannot have a guard",
                )
        elif isinstance(source, PseudostateElement) and transition.events:
            raise ErrorValidatingModel(
                transition.location,
                f"transition '{transition.qualified_name}' outgoing pseudostate '{source.qualified_name}' cannot have triggers",
            )
        target: Element | None = None
        if transition.target != "":
            target = self._validate_member(
                model,
                transition.target,
                VertexElement,
                transition.location,
                "target",
                f"transition '{transition.qualified_name}'",
            )
            if isinstance(target, EntryPointElement):
                owner = target.owner()
                if transition.source == owner or IsAncestor(owner, transition.source):
                    raise ErrorValidatingModel(
                        transition.location,
                        "entry point target cannot be internal",
                    )
                boundary = model.members.get(posixpath.dirname(owner))
                if not isinstance(boundary, StateElement) or not kind.Is(
                    boundary.kind, SubmachineStateKind
                ):
                    raise ErrorValidatingModel(
                        transition.location,
                        "entry point can only target a SubmachineState",
                    )
        if transition.guard is not None:
            guard = self._validate_member(
                model,
                transition.guard,
                ConstraintElement,
                transition.location,
                "guard",
                f"transition '{transition.qualified_name}'",
            )
            if inspect.iscoroutinefunction(
                typing.cast(ConstraintElement[typing.Any], guard).expression
            ):
                raise ErrorValidatingModel(
                    guard.location,
                    "guard must be a synchronous function",
                )
        for effect in transition.effect:
            behavior = self._validate_member(
                model,
                effect,
                BehaviorElement,
                transition.location,
                "effect",
                f"transition '{transition.qualified_name}'",
            )
            if inspect.iscoroutinefunction(
                typing.cast(BehaviorElement[typing.Any], behavior).operation
            ):
                raise ErrorValidatingModel(
                    behavior.location,
                    "effect must be a synchronous function",
                )
        for event in transition.events:
            if event not in model.events:
                raise ErrorValidatingModel(
                    transition.location,
                    f"event '{event}' not found for transition '{transition.qualified_name}'",
                )
            if event.startswith(ExitPointEventPrefix) and (
                not isinstance(source, StateElement)
                or not kind.Is(source.kind, SubmachineStateKind)
            ):
                raise ErrorValidatingModel(
                    transition.location,
                    "ExitPoint outcome can only be handled by a SubmachineState",
                )
        if not transition.events and not isinstance(source, PseudostateElement):
            raise ErrorValidatingModel(
                transition.location,
                f"transition '{transition.qualified_name}' has no events",
            )
        if kind.Is(transition.kind, ExternalKind) and isinstance(
            source, EntryPointElement
        ):
            raise ErrorValidatingModel(
                transition.location,
                f"external transition '{transition.qualified_name}' cannot source an entry point",
            )
        if kind.Is(transition.kind, InternalKind) and not isinstance(
            source, StateElement
        ):
            raise ErrorValidatingModel(
                transition.location,
                f"internal transition '{transition.qualified_name}' must source a state",
            )

    def _validate_choice(self, model: "Model", choice: ChoiceElement) -> None:
        if not choice.transitions:
            raise ErrorValidatingModel(
                choice.location,
                f"choice '{choice.qualified_name}' has no transitions",
            )
        incoming = [
            transition
            for transition in model.members.values()
            if isinstance(transition, TransitionElement)
            and transition.target == choice.qualified_name
        ]
        if not incoming:
            raise ErrorValidatingModel(
                choice.location,
                f"choice '{choice.qualified_name}' has no incoming transitions",
            )
        last_transition = model.members.get(choice.transitions[-1])
        if (
            isinstance(last_transition, TransitionElement)
            and last_transition.guard is not None
        ):
            raise ErrorValidatingModel(
                last_transition.location,
                f"the last transition of choice state '{choice.qualified_name}' cannot have a guard",
            )

    def _validate_final_state(
        self, model: "Model", final_state: FinalStateElement
    ) -> None:
        if final_state.transitions:
            raise ErrorValidatingModel(
                final_state.location,
                "final state cannot have transitions",
            )
        if final_state.initial or any(
            isinstance(member, (VertexElement, TransitionElement))
            and member is not final_state
            and member.owner() == final_state.qualified_name
            for member in model.members.values()
        ):
            raise ErrorValidatingModel(
                final_state.location,
                "final state cannot have regions",
            )
        if final_state.submachine is not None:
            raise ErrorValidatingModel(
                final_state.location,
                "final state cannot reference a submachine",
            )
        if final_state.entry:
            raise ErrorValidatingModel(
                final_state.location,
                "final state cannot have an entry action",
            )
        if final_state.exit:
            raise ErrorValidatingModel(
                final_state.location,
                "final state cannot have an exit action",
            )
        if final_state.activity:
            raise ErrorValidatingModel(
                final_state.location,
                "final state cannot have an activity",
            )


@dataclasses.dataclass(kw_only=True)
class Model(StateElement):
    members: dict[str, Element] = dataclasses.field(default_factory=dict)
    events: dict[str, Event[typing.Any]] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.members[self.qualified_name] = self

    def redefine(
        self,
        model: "Model",
        stack: list[Element],
        element: TElement | None = None,
    ) -> TElement | None:
        if isinstance(element, StateElement):
            state = StateElement(
                qualified_name=join(element.qualified_name, self.name()),
                location=self.location,
            )
            model.members[state.qualified_name] = state
            _ = RedefinableInitial(
                owned_elements=[Target(state.qualified_name)],
                location=self.location,
            ).redefine(model, [*stack, element])
            _ = RedefinableElement[StateElement](
                owned_elements=self.owned_elements,
            ).redefine(model, [*stack, element], state)
            return element
        return RedefinableElement[TElement](
            owned_elements=self.owned_elements,
        ).redefine(model, stack, element)


@dataclasses.dataclass(kw_only=True)
class FinalizedModel(Model):
    transition_map: dict[str, dict[str, list[TransitionElement]]] = dataclasses.field(
        default_factory=dict
    )
    deferred_map: dict[str, dict[str, bool]] = dataclasses.field(default_factory=dict)
    transition_paths: dict[str, dict[str, TransitionPath]] = dataclasses.field(
        default_factory=dict
    )
    history_paths: dict[tuple[str, str], tuple[str, ...]] = dataclasses.field(
        default_factory=dict
    )
    history_targets: dict[tuple[str, str | None], dict[str, str]] = dataclasses.field(
        default_factory=dict
    )


def find(stack: list[Element], *maybe_kinds: type[TElement]) -> TElement | None:
    for element in reversed(stack):
        if isinstance(element, maybe_kinds):
            return element
    return None


def get(model: Model, name: str, _: type[TElement] = Element) -> TElement | None:
    if name == "":
        return None
    if element := model.members.get(name):
        return typing.cast(TElement, element)
    return None


def join(path: str, *paths: str) -> str:
    return posixpath.normpath(posixpath.join(path, *paths))


class Keys:
    class Instances:
        pass

    class Owner:
        pass

    class HSM:
        pass


async def sleep(duration: datetime.timedelta) -> datetime.datetime:
    await asyncio.sleep(duration.total_seconds())
    return datetime.datetime.now()


class Timer:
    def __init__(self, duration: datetime.timedelta):
        self._task: asyncio.Task[datetime.datetime] = asyncio.Task(
            sleep(duration),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )

    def __await__(
        self,
    ) -> collections.abc.Generator[typing.Any, None, datetime.datetime]:
        return self._task.__await__()

    def Stop(self) -> bool:
        if self._task.done():
            return False
        return self._task.cancel()

    def Reset(self, duration: datetime.timedelta) -> bool:
        was_active = self.Stop()
        self._task = asyncio.Task(
            sleep(duration),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )
        return was_active


class Clock(typing.Protocol):
    def After(
        self, duration: datetime.timedelta
    ) -> asyncio.Task[datetime.datetime]: ...

    def NewTimer(self, duration: datetime.timedelta) -> Timer: ...


class DefaultClock:
    def After(self, duration: datetime.timedelta) -> asyncio.Task[datetime.datetime]:
        return asyncio.Task(
            sleep(duration),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )

    def NewTimer(self, duration: datetime.timedelta) -> Timer:
        return Timer(duration)

    after: typing.Callable[
        [typing.Self, datetime.timedelta], asyncio.Task[datetime.datetime]
    ] = After
    new_timer: typing.Callable[[typing.Self, datetime.timedelta], Timer] = NewTimer


@dataclasses.dataclass(init=False, kw_only=True, frozen=True)
class Config(typing.Generic[TData]):
    id: str = dataclasses.field(default="")
    name: str = dataclasses.field(default="")
    data: TData | None = dataclasses.field(default=None)
    clock: Clock | None = dataclasses.field(default=None)
    queue: generic.Queue[Event] | None = dataclasses.field(default=None)

    def __init__(
        self,
        *,
        id: str = "",
        name: str = "",
        data: TData | None = None,
        clock: Clock | None = None,
        queue: generic.Queue[Event] | None = None,
        ID: str | object = dataclasses.MISSING,
        Name: str | object = dataclasses.MISSING,
        Data: TData | object = dataclasses.MISSING,
        Clock: Clock | None | object = dataclasses.MISSING,
        Queue: generic.Queue[Event] | None | object = dataclasses.MISSING,
    ) -> None:
        if ID is not dataclasses.MISSING:
            id = typing.cast(str, ID)
        if Name is not dataclasses.MISSING:
            name = typing.cast(str, Name)
        if Data is not dataclasses.MISSING:
            data = typing.cast(TData | None, Data)
        if Clock is not dataclasses.MISSING:
            clock = typing.cast("Clock | None", Clock)
        if Queue is not dataclasses.MISSING:
            queue = typing.cast(generic.Queue[Event] | None, Queue)
        object.__setattr__(self, "id", id)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "data", data)
        object.__setattr__(self, "clock", clock)
        object.__setattr__(self, "queue", queue)

    @property
    def ID(self) -> str:
        return self.id

    @property
    def Name(self) -> str:
        return self.name

    @property
    def Data(self) -> TData | None:
        return self.data

    @property
    def Clock(self) -> Clock | None:
        return self.clock

    @property
    def Queue(self) -> generic.Queue[Event] | None:
        return self.queue


@dataclasses.dataclass
class CallData:
    name: str
    args: tuple[typing.Any, ...]


@dataclasses.dataclass
class AttributeChange:
    name: str
    value: typing.Any
    old_value: typing.Any


@dataclasses.dataclass(frozen=True)
class Snapshot:
    ID: str = ""
    QualifiedName: str = ""
    State: str = ""
    Attributes: collections.abc.Mapping[str, typing.Any] | None = None
    QueueLen: int = 0
    Transitions: tuple[TransitionElement, ...] = dataclasses.field(
        default_factory=tuple
    )

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
    def attributes(self) -> collections.abc.Mapping[str, typing.Any] | None:
        return self.Attributes

    @property
    def queue_len(self) -> int:
        return self.QueueLen

    @property
    def transitions(self) -> tuple[TransitionElement, ...]:
        return self.Transitions


InitialEvent = Event(name="hsm/initial", kind=EventKind)
ErrorEvent = Event(name="hsm/error", kind=ErrorEventKind)
AnyEvent = Event(name="*", kind=EventKind)
FinalEvent = Event(name="hsm/final", kind=CompletionEventKind)
InfiniteDuration = datetime.timedelta(-1)
ExitPointEventPrefix = "hsm/exit/"


def _exit_point_event_name(name: str) -> str:
    return f"{ExitPointEventPrefix}{name}"


@dataclasses.dataclass(kw_only=True)
class RedefinableVertex(RedefinableElement[TElement]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: TElement | None = None,
    ) -> TElement | None:
        return element


@dataclasses.dataclass(kw_only=True)
class RedefinableState(RedefinableElement[StateElement]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: StateElement | None = None,
    ) -> StateElement | None:
        # _validate_slashless_name("state", self.qualified_name, self.traceback)
        namespace = find(stack, NamespaceElement)
        if namespace is None:
            raise ErrorValidatingModel(
                self.location,
                "state must be called within Define() or State()",
            )
        state = StateElement(
            qualified_name=join(namespace.qualified_name, self.name()),
            location=self.location,
        )
        model.members[state.qualified_name] = state
        return super().redefine(model, stack, state)


@dataclasses.dataclass(kw_only=True)
class RedefinableSubmachineState(RedefinableState):
    machine: Model | None = None

    @typing.override
    def redefine(
        self, model: Model, stack: list[Element], element: StateElement | None = None
    ) -> StateElement | None:
        namespace = find(stack, StateElement)
        if namespace is None:
            raise ErrorValidatingModel(
                self.location,
                "submachine state must be called within Define() or State()",
            )
        if self.machine is None:
            raise ErrorValidatingModel(
                self.location,
                "submachine state requires a model",
            )
        state = StateElement(
            kind=SubmachineStateKind,
            qualified_name=join(namespace.qualified_name, self.name()),
            location=self.location,
        )
        model.members[state.qualified_name] = state
        _ = self.machine.redefine(model, stack, state)
        return RedefinableElement[StateElement](
            owned_elements=self.owned_elements,
        ).redefine(model, stack, state)


@dataclasses.dataclass(kw_only=True)
class RedefinableInitial(RedefinableElement[TransitionElement]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: TransitionElement | None = None,
    ) -> TransitionElement | None:
        namespace = find(stack, StateElement)
        if namespace is None:
            raise ErrorValidatingModel(
                self.location,
                "initial must be called within a State()",
            )
        initial = InitialElement(
            qualified_name=join(namespace.qualified_name, ".initial"),
        )
        if model.members.get(initial.qualified_name) is not None:
            raise ErrorValidatingModel(
                self.location,
                f"state {namespace.qualified_name} already has an initial state",
            )
        model.members[initial.qualified_name] = initial
        namespace.initial = initial.qualified_name
        stack = [*stack, initial]
        transition = RedefinableTransition(
            owned_elements=[
                Source(initial.qualified_name),
                *self.owned_elements,
                On(InitialEvent),
            ],
            location=self.location,
        ).redefine(model, stack)
        if transition is None:
            raise ErrorValidatingModel(
                self.location,
                "initial transition is required for state machine",
            )
        return transition


@dataclasses.dataclass(kw_only=True)
class RedefinableHistory(
    RedefinableElement[ShallowHistoryElement | DeepHistoryElement]
):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: ShallowHistoryElement | DeepHistoryElement | None = None,
    ) -> ShallowHistoryElement | DeepHistoryElement | None:
        namespace = find(stack, StateElement)
        if namespace is None:
            raise ErrorValidatingModel(
                self.location,
                "history must be called within a State()",
            )
        name = self.name()
        if not name:
            name = (
                f"shallow_history_{len(model.members)}"
                if self.kind == ShallowHistoryKind
                else f"deep_history_{len(model.members)}"
            )
        qualified_name = join(namespace.qualified_name, name)
        if model.members.get(qualified_name) is not None:
            raise ErrorValidatingModel(
                self.location,
                f"history '{qualified_name}' already defined",
            )
        history = model.members[qualified_name] = (
            ShallowHistoryElement(
                qualified_name=qualified_name,
                location=self.location,
            )
            if self.kind == ShallowHistoryKind
            else DeepHistoryElement(
                qualified_name=qualified_name,
                location=self.location,
            )
        )
        return super().redefine(model, stack, history)


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


class DefaultModelFinalizer(ModelFinalizer):
    @typing.override
    def finalize(self, model: Model) -> Model:
        finalized = self._finalize_model(model)
        finalized.transition_map.clear()
        finalized.deferred_map.clear()
        finalized.transition_paths.clear()
        finalized.history_paths.clear()
        finalized.history_targets.clear()
        self._finalize_transitions(finalized)
        self._finalize_history_paths(finalized)
        self._finalize_scheduled_events(finalized)
        self._finalize_deferred_map(finalized)
        self._finalize_transition_map(finalized)
        return finalized

    def _finalize_model(self, model: Model) -> FinalizedModel:
        if isinstance(model, FinalizedModel):
            model.members[model.qualified_name] = model
            return model
        finalized = FinalizedModel(
            kind=model.kind,
            id=model.id,
            qualified_name=model.qualified_name,
            owned_elements=model.owned_elements,
            location=model.location,
            members=dict(model.members),
            transitions=model.transitions,
            initial=model.initial,
            entry=model.entry,
            exit=model.exit,
            activity=model.activity,
            deferred=model.deferred,
            submachine=model.submachine,
            events=model.events,
        )
        finalized.members[finalized.qualified_name] = finalized
        return finalized

    def _finalize_transitions(self, model: FinalizedModel) -> None:
        for element in model.members.values():
            if not isinstance(element, TransitionElement):
                continue
            element.kind = self._finalize_transition_kind(element)
            model.transition_paths[element.qualified_name] = (
                self._finalize_paths_for_transition(model, element)
            )

    def _finalize_transition_kind(self, transition: TransitionElement) -> kind.Kind:
        if transition.kind != TransitionKind:
            return transition.kind
        if transition.target == transition.source:
            return SelfKind
        if transition.target == "":
            return InternalKind
        if IsAncestor(transition.source, transition.target):
            return LocalKind
        return ExternalKind

    def _finalize_paths_for_transition(
        self, model: FinalizedModel, transition: TransitionElement
    ) -> dict[str, TransitionPath]:
        source = get(model, transition.source, VertexElement)
        if source is None:
            return {}
        return {
            current: self._finalize_path_for_current(model, current, transition)
            for current in self._finalize_current_vertices_for(model, source)
        }

    def _finalize_current_vertices_for(
        self, model: FinalizedModel, source: VertexElement
    ) -> list[str]:
        if isinstance(source, InitialElement):
            return [posixpath.dirname(source.qualified_name)]
        if isinstance(source, ChoiceElement):
            return [source.qualified_name]
        return [
            qualified_name
            for qualified_name, member in model.members.items()
            if isinstance(member, VertexElement)
            and (
                qualified_name == source.qualified_name
                or IsAncestor(source.qualified_name, qualified_name)
            )
        ]

    def _finalize_path_for_current(
        self, model: FinalizedModel, current: str, transition: TransitionElement
    ) -> TransitionPath:
        if transition.target == "":
            return TransitionPath()
        lca = (
            posixpath.dirname(transition.source)
            if kind.Is(transition.kind, SelfKind)
            else LCA(current, transition.target)
        )
        return TransitionPath(
            enter=self._finalize_enter_path(model, lca, transition.target),
            exit=self._finalize_exit_path(lca, current),
        )

    def _finalize_enter_path(
        self, model: FinalizedModel, lca: str, target: str
    ) -> list[str]:
        enter: list[str] = []
        entering = target
        while entering not in (lca, model.qualified_name, ""):
            enter.insert(0, entering)
            entering = posixpath.dirname(entering)
        return enter

    def _finalize_exit_path(self, lca: str, current: str) -> list[str]:
        exit_path: list[str] = []
        exiting = current
        while exiting not in (lca, "", "."):
            exit_path.append(exiting)
            exiting = posixpath.dirname(exiting)
        return exit_path

    def _finalize_history_paths(self, model: FinalizedModel) -> None:
        history_elements = [
            element
            for element in model.members.values()
            if isinstance(element, (ShallowHistoryElement, DeepHistoryElement))
        ]
        history_targets: dict[str, dict[str, str]] = {}
        history_owners = {
            history.owner() for history in history_elements if history.owner()
        }
        for history in history_elements:
            owner = history.owner()
            if not owner:
                continue
            for target, element in model.members.items():
                if not isinstance(element, StateElement):
                    continue
                if target == owner or not IsAncestor(owner, target):
                    continue
                model.history_paths[(owner, target)] = tuple(
                    self._finalize_enter_path(model, owner, target)
                )
                history_target = target
                if isinstance(history, ShallowHistoryElement):
                    while posixpath.dirname(history_target) != owner:
                        history_target = posixpath.dirname(history_target)
                        if history_target in ("", ".", "/"):
                            break
                if history_target not in ("", ".", "/"):
                    history_targets.setdefault(target, {})[history.qualified_name] = (
                        history_target
                    )
        for target, targets in history_targets.items():
            model.history_targets[(target, None)] = targets
            for owner in history_owners:
                filtered = {
                    history_name: history_target
                    for history_name, history_target in targets.items()
                    if posixpath.dirname(history_name) != owner
                }
                if filtered:
                    model.history_targets[(target, owner)] = filtered

    def _finalize_scheduled_events(self, model: FinalizedModel) -> None:
        for transition in (
            element
            for element in list(model.members.values())
            if isinstance(element, TransitionElement)
        ):
            for event_name in transition.events:
                event = model.events.get(event_name)
                if not isinstance(event, Event):
                    continue
                data = typing.cast(object, event.data)
                if event.kind != TimeEventKind or not callable(data):
                    continue
                source = model.members.get(transition.source)
                if not isinstance(source, StateElement):
                    raise ErrorValidatingModel(
                        transition.location,
                        "time event can only be used on transitions where the source is a State",
                    )
                activity_name = join(
                    source.qualified_name,
                    "activity",
                    event.name.removeprefix("/"),
                )
                if activity_name not in model.members:
                    model.members[activity_name] = BehaviorElement(
                        kind=ConcurrentKind,
                        qualified_name=activity_name,
                        operation=typing.cast(OperationExpression[typing.Any], data),
                    )
                if activity_name not in source.activity:
                    source.activity.append(activity_name)

    def _finalize_deferred_map(self, model: FinalizedModel) -> None:
        for qualified_name, element in model.members.items():
            if not isinstance(element, StateElement):
                continue
            model.deferred_map[qualified_name] = {}
            current = qualified_name
            while current:
                current_state = model.members.get(current)
                if isinstance(current_state, StateElement):
                    for deferred_event in current_state.deferred:
                        _ = model.deferred_map[qualified_name].setdefault(
                            deferred_event, True
                        )
                if current == model.qualified_name:
                    break
                current = posixpath.dirname(current)

    def _finalize_transition_map(self, model: FinalizedModel) -> None:
        for qualified_name, element in model.members.items():
            if not isinstance(element, StateElement):
                continue
            model.transition_map[qualified_name] = {}
            current = qualified_name
            while current:
                current_vertex = model.members.get(current)
                if isinstance(current_vertex, VertexElement):
                    self._finalize_add_vertex_transitions(
                        model, qualified_name, current_vertex
                    )
                if current == model.qualified_name:
                    break
                current = posixpath.dirname(current)

    def _finalize_add_vertex_transitions(
        self,
        model: FinalizedModel,
        current: str,
        vertex: VertexElement,
    ) -> None:
        for transition_name in vertex.transitions:
            transition = get(model, transition_name, TransitionElement)
            if transition is None or not transition.events:
                continue
            if current not in model.transition_paths.get(transition.qualified_name, {}):
                continue
            for event_name in transition.events:
                model.transition_map[current].setdefault(event_name, []).append(
                    transition
                )
                if event_name.startswith(ExitPointEventPrefix):
                    model.transition_map[current][event_name].sort(
                        key=lambda item: item.guard is None
                    )


@dataclasses.dataclass(kw_only=True)
class RedefinableTransition(RedefinableElement[TransitionElement]):
    @dataclasses.dataclass(kw_only=True)
    class Source(RedefinableElement[TransitionElement]):
        @typing.override
        def redefine(
            self,
            model: Model,
            stack: list[Element],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            transition = find(stack, TransitionElement)
            if transition is None:
                raise ErrorValidatingModel(
                    self.location,
                    "hsm.Source() must be called within hsm.Transition()",
                )
            if transition.source not in ("", "."):
                raise ErrorValidatingModel(
                    self.location,
                    f"Transition '{transition.qualified_name}' already has a source '{transition.source}'",
                )

            source_qualified_name = _resolve_source_or_target_qualified_name(
                model, stack, self
            )
            transition.source = source_qualified_name
            return transition

    @dataclasses.dataclass(kw_only=True)
    class Target(RedefinableElement[TransitionElement]):
        @typing.override
        def redefine(
            self,
            model: Model,
            stack: list[Element],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            transition = find(stack, TransitionElement)
            if transition is None:
                raise ErrorValidatingModel(
                    self.location,
                    "hsm.Target() must be called within hsm.Transition()",
                )
            if transition.target:
                raise ErrorValidatingModel(
                    self.location,
                    f"Transition '{transition.qualified_name}' already has a target '{transition.target}'",
                )
            qualified_name = _resolve_source_or_target_qualified_name(
                model, stack, self
            )
            if qualified_name in model.members:
                transition.target = qualified_name
                return transition
            model.owned_elements.append(
                RedefinableTransition.TargetResolver(
                    qualified_name=qualified_name,
                    location=self.location,
                    transition=transition.qualified_name,
                )
            )
            return transition

    @dataclasses.dataclass(kw_only=True)
    class TargetResolver(RedefinableElement[TransitionElement]):
        transition: str = dataclasses.field(default_factory=str)

        @typing.override
        def redefine(
            self,
            model: Model,
            stack: list[Element],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            transition = get(model, self.transition, TransitionElement)
            if transition is None:
                raise ErrorValidatingModel(
                    self.location,
                    f"transition '{self.transition}' not found",
                )
            if transition.target:
                raise ErrorValidatingModel(
                    self.location,
                    f"Transition '{transition.qualified_name}' already has a target '{transition.target}'",
                )
            transition.target = self.qualified_name
            return transition

    @dataclasses.dataclass(kw_only=True)
    class TimeEvent(RedefinableElement[TransitionElement], typing.Generic[TInstance]):
        def WithTimeEvent(
            self,
            model: Model,
            stack: list[Element],
            operation: collections.abc.Callable[
                [Event], OperationExpression[TInstance]
            ],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            transition = find(stack, TransitionElement)
            if transition is None:
                raise ErrorValidatingModel(
                    self.location,
                    "time event must be called within a Transition",
                )
            event_name = join(
                transition.qualified_name,
                self.qualified_name or f"time_{len(model.members)}",
            )
            event = Event(name=event_name, kind=TimeEventKind)
            model.events[event_name] = Event(
                name=event.name,
                kind=event.kind,
                data=operation(event),
            )
            transition.events.append(event_name)
            return element

    @dataclasses.dataclass(kw_only=True)
    class WhenEvent(TimeEvent[TInstance]):
        expression: Expression[TInstance, typing.Any]

        @typing.override
        def redefine(
            self,
            model: Model,
            stack: list[Element],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            return self.WithTimeEvent(model, stack, self._activity, element)

        def _activity(self, event: Event) -> OperationExpression[TInstance]:
            async def operation(
                ctx: context.Context, instance: TInstance, _: Event
            ) -> None:
                signal = typing.cast(object, self.expression(ctx, instance, event))
                if isinstance(signal, collections.abc.Awaitable):
                    await signal
                await instance.dispatch(instance.context(), event)

            return operation

    @dataclasses.dataclass(kw_only=True)
    class AfterEvent(TimeEvent[TInstance]):
        expression: TimeExpression[TInstance]

        @typing.override
        def redefine(
            self,
            model: Model,
            stack: list[Element],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            return self.WithTimeEvent(model, stack, self._activity, element)

        def _activity(self, event: Event) -> OperationExpression[TInstance]:
            async def operation(
                ctx: context.Context, instance: TInstance, _: Event
            ) -> None:
                delay = self.expression(ctx, instance, event)
                if isinstance(delay, collections.abc.Awaitable):
                    delay = await delay
                duration = typing.cast(datetime.timedelta, delay)
                if duration.total_seconds() < 0:
                    return
                timer_result = await instance.clock().After(duration)
                del timer_result
                await instance.dispatch(instance.context(), event)

            return operation

    @dataclasses.dataclass(kw_only=True)
    class AtEvent(TimeEvent[TInstance]):
        expression: TimeExpression[TInstance]

        @typing.override
        def redefine(
            self,
            model: Model,
            stack: list[Element],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            return self.WithTimeEvent(model, stack, self._activity, element)

        def _activity(self, event: Event) -> OperationExpression[TInstance]:
            async def operation(
                ctx: context.Context, instance: TInstance, _: Event
            ) -> None:
                result = self.expression(ctx, instance, event)
                if isinstance(result, collections.abc.Awaitable):
                    result = await result
                timepoint = typing.cast(datetime.datetime, result)
                now = datetime.datetime.now(timepoint.tzinfo)
                duration = timepoint - now
                if duration.total_seconds() < 0:
                    return
                timer_result = await instance.clock().After(duration)
                del timer_result
                await instance.dispatch(instance.context(), event)

            return operation

    @dataclasses.dataclass(kw_only=True)
    class EveryEvent(TimeEvent[TInstance]):
        expression: TimeExpression[TInstance]

        @typing.override
        def redefine(
            self,
            model: Model,
            stack: list[Element],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            return self.WithTimeEvent(model, stack, self._activity, element)

        def _activity(self, event: Event) -> OperationExpression[TInstance]:
            async def operation(
                ctx: context.Context, instance: TInstance, _: Event
            ) -> None:
                while True:
                    delay = self.expression(ctx, instance, event)
                    if isinstance(delay, collections.abc.Awaitable):
                        delay = await delay
                    duration = typing.cast(datetime.timedelta, delay)
                    if duration.total_seconds() < 0:
                        return
                    timer_result = await instance.clock().After(duration)
                    del timer_result
                    await instance.dispatch(instance.context(), event)

            return operation

    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: TransitionElement | None = None,
    ) -> TransitionElement | None:
        vertex = find(stack, VertexElement)
        if vertex is None:
            raise ErrorValidatingModel(
                self.location,
                "transition must be called within a StateElement() or Define()",
            )
        qualified_name = join(
            vertex.qualified_name,
            self.qualified_name or f"transition_{len(model.members)}",
        )
        if model.members.get(qualified_name) is not None:
            raise ErrorValidatingModel(
                self.location,
                f"transition '{qualified_name}' already defined",
            )
        source = "."
        transition = model.members[qualified_name] = TransitionElement(
            qualified_name=qualified_name,
            source=source,
        )
        substack = [*stack, transition]
        _ = super().redefine(model, substack, transition)
        if transition.source == ".":
            transition.source = vertex.qualified_name
        source = transition.source
        source_element = get(model, source, VertexElement)
        if source_element is None:
            raise ErrorValidatingModel(
                self.location,
                f"source '{source}' not found for transition '{qualified_name}'",
            )
        source_element.transitions.append(qualified_name)
        return transition


def _resolve_source_or_target_qualified_name(
    model: Model,
    stack: list[Element],
    source_or_target: RedefinableElement[TransitionElement],
) -> str:
    if source_or_target.owned_elements:
        owned_element = source_or_target.owned_elements[0]
        if isinstance(owned_element, Redefinable):
            resolved = typing.cast(Redefinable[Element], owned_element).redefine(
                model, stack
            )
            if not isinstance(resolved, Element):
                raise ErrorValidatingModel(
                    owned_element.location,
                    f"source or target '{owned_element.qualified_name}' is not an element",
                )
            return resolved.qualified_name
        return owned_element.qualified_name
    if not posixpath.isabs(source_or_target.qualified_name):
        ancestor = find(stack, StateElement)
        if ancestor is None:
            raise ErrorValidatingModel(
                source_or_target.location,
                "source or target must be called within a state or model",
            )
        return join(ancestor.qualified_name, source_or_target.qualified_name)
    elif not IsAncestor(model.qualified_name, source_or_target.qualified_name):
        relative_name = source_or_target.qualified_name.removeprefix("/")
        model_name, _, relative_path = relative_name.partition("/")
        ancestor = find(stack, StateElement)
        current = ancestor.qualified_name if ancestor is not None else ""
        while current:
            if posixpath.basename(current) == model_name:
                return join(current, relative_path) if relative_path else current
            parent = posixpath.dirname(current)
            if parent == current:
                break
            current = parent
        if ancestor is None:
            return join(model.qualified_name, source_or_target.qualified_name)
        return join(ancestor.qualified_name, "..", relative_name)
    return source_or_target.qualified_name


@dataclasses.dataclass(kw_only=True)
class RedefinableModel(RedefinableElement[Model]):
    @typing.override
    def redefine(
        self, model: Model, stack: list[Element], element: Model | None = None
    ) -> Model | None:
        if element is None:
            element = Model(
                qualified_name=join(model.qualified_name, self.qualified_name),
            )
        owned_elements = model.owned_elements[:]
        result = super().redefine(model, stack, element)
        model.owned_elements[:] = owned_elements
        return result


@dataclasses.dataclass(kw_only=True)
class RedefinableBehaviors(RedefinableElement[BehaviorElement[TInstance]]):
    behaviors: list[OperationExpression[TInstance] | BehaviorElement[TInstance]] = (
        dataclasses.field(default_factory=list)
    )

    def redefine_all(
        self,
        model: Model,
        owner: Element,
        *,
        kind: int | None = None,
    ) -> list[BehaviorElement[TInstance]]:
        behaviors: list[BehaviorElement[TInstance]] = []
        for behavior_or_method in self.behaviors:
            behavior: BehaviorElement[TInstance] | None = None
            if isinstance(behavior_or_method, BehaviorElement):
                behavior_element = typing.cast(
                    BehaviorElement[TInstance], behavior_or_method
                )
                behavior = BehaviorElement(
                    kind=kind if kind is not None else behavior_element.kind,
                    qualified_name=join(owner.qualified_name, behavior_element.name()),
                    operation=behavior_element.operation,
                    location=self.location,
                )
            else:
                name = getattr(
                    behavior_or_method,
                    "__name__",
                    f"behavior_{len(model.members)}",
                )
                behavior = BehaviorElement(
                    kind=kind if kind is not None else BehaviorKind,
                    qualified_name=join(owner.qualified_name, name),
                    operation=behavior_or_method,
                    location=self.location,
                )
            model.members[behavior.qualified_name] = behavior
            behaviors.append(behavior)
        return behaviors


@dataclasses.dataclass(kw_only=True)
class RedefinableConstraint(RedefinableElement[ConstraintElement[TInstance]]):
    expression: "Expression[TInstance, bool] | ConstraintElement[TInstance] | RedefinableConstraint[TInstance]"

    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: ConstraintElement[TInstance] | None = None,
    ) -> ConstraintElement[TInstance] | None:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ErrorValidatingModel(
                self.location,
                "constraint must be called within a Transition",
            )
        qualified_name = join(
            transition.qualified_name,
            self.qualified_name or ".guard",
        )
        constraint: ConstraintElement[TInstance] | None = None
        if isinstance(self.expression, ConstraintElement):
            constraint_element = typing.cast(
                ConstraintElement[TInstance], self.expression
            )
            constraint = ConstraintElement(
                qualified_name=join(
                    transition.qualified_name, constraint_element.name()
                ),
                expression=constraint_element.expression,
            )
        elif isinstance(self.expression, RedefinableConstraint):
            redefinable_constraint = typing.cast(
                RedefinableConstraint[TInstance], self.expression
            )
            constraint = redefinable_constraint.redefine(model, stack)
            if constraint is None:
                raise ErrorValidatingModel(
                    self.location,
                    "constraint must be called within a Transition",
                )
        else:
            constraint = ConstraintElement(
                qualified_name=qualified_name,
                expression=self.expression,
            )
        model.members[constraint.qualified_name] = constraint
        transition.guard = constraint.qualified_name
        return constraint


@dataclasses.dataclass(kw_only=True)
class RedefinableEntryBehavior(RedefinableBehaviors[TInstance]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: BehaviorElement[TInstance] | None = None,
    ) -> BehaviorElement[TInstance] | None:
        state = find(stack, StateElement)
        if state is None:
            raise ErrorValidatingModel(
                self.location,
                "entry must be called within a State",
            )
        state.entry.extend(
            behavior.qualified_name for behavior in self.redefine_all(model, state)
        )


@dataclasses.dataclass(kw_only=True)
class RedefinableExitBehavior(RedefinableBehaviors[TInstance]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: BehaviorElement[TInstance] | None = None,
    ) -> BehaviorElement[TInstance] | None:
        state = find(stack, StateElement)
        if state is None:
            raise ErrorValidatingModel(
                self.location,
                "exit must be called within a State",
            )
        state.exit.extend(
            behavior.qualified_name for behavior in self.redefine_all(model, state)
        )


@dataclasses.dataclass(kw_only=True)
class RedefinableActivityBehavior(RedefinableBehaviors[TInstance]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: BehaviorElement[TInstance] | None = None,
    ) -> BehaviorElement[TInstance] | None:
        state = find(stack, StateElement)
        if state is None:
            raise ErrorValidatingModel(
                self.location,
                "activity must be called within a State",
            )
        state.activity.extend(
            behavior.qualified_name
            for behavior in self.redefine_all(model, state, kind=ConcurrentKind)
        )


@dataclasses.dataclass(kw_only=True)
class RedefinableEffectBehavior(RedefinableBehaviors[TInstance]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: BehaviorElement[TInstance] | None = None,
    ) -> BehaviorElement[TInstance] | None:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ErrorValidatingModel(
                self.location,
                "effect must be called within a Transition",
            )
        transition.effect.extend(
            behavior.qualified_name for behavior in self.redefine_all(model, transition)
        )


@dataclasses.dataclass(kw_only=True)
class RedefinableStateWithDeferredEvents(RedefinableElement[StateElement]):
    deferred: list[Event] = dataclasses.field(default_factory=list)

    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: StateElement | None = None,
    ) -> StateElement | None:
        state = find(stack, StateElement)
        if state is None:
            raise ErrorValidatingModel(
                self.location,
                "defer must be called within a State",
            )
        for event in self.deferred:
            state.deferred.append(event.name)
            model.events[event.name] = event
        return state


@dataclasses.dataclass(kw_only=True)
class RedefinableEntryPoint(RedefinableElement[EntryPointElement]):
    @dataclasses.dataclass(kw_only=True)
    class Resolver(RedefinableElement[TransitionElement]):
        transition: str = dataclasses.field(default_factory=str)

        @typing.override
        def redefine(
            self,
            model: Model,
            stack: list[Element],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            transition = get(model, self.transition, TransitionElement)
            if transition is None:
                raise ErrorValidatingModel(
                    self.location,
                    f"transition '{self.transition}' not found",
                )
            if transition.target == "":
                raise ErrorValidatingModel(
                    self.location,
                    f"entry point '{self.qualified_name}' requires a transition target",
                )
            direct: list[EntryPointElement] = []
            nested: list[EntryPointElement] = []
            for member in model.members.values():
                if not isinstance(member, EntryPointElement):
                    continue
                if member.name() != self.qualified_name:
                    continue
                if not IsAncestor(transition.target, member.qualified_name):
                    continue
                if posixpath.dirname(member.owner()) == transition.target:
                    direct.append(member)
                else:
                    nested.append(member)
            entry_point = (direct or nested or [None])[0]
            if entry_point is None:
                raise ErrorValidatingModel(
                    self.location,
                    f"state '{transition.target}' has no entry point '{self.qualified_name}'",
                )
            transition.target = entry_point.qualified_name
            return transition

    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: EntryPointElement | None = None,
    ) -> EntryPointElement | None:
        transition = find(stack, TransitionElement)
        if transition is not None:
            model.owned_elements.append(
                RedefinableEntryPoint.Resolver(
                    qualified_name=self.qualified_name,
                    transition=transition.qualified_name,
                    location=self.location,
                )
            )
            return None
        namespace = find(stack, NamespaceElement)
        if namespace is None:
            raise ErrorValidatingModel(
                self.location,
                "entry point must be called within Define() or State()",
            )
        entry_point = EntryPointElement(
            qualified_name=join(namespace.qualified_name, self.name()),
            location=self.location,
        )
        model.members[entry_point.qualified_name] = entry_point
        transition = RedefinableTransition(
            owned_elements=self.owned_elements,
            location=self.location,
        ).redefine(model, [*stack, entry_point])
        if transition is not None:
            transition.kind = LocalKind
        return entry_point


@dataclasses.dataclass(kw_only=True)
class RedefinableExitPoint(RedefinableElement[ExitPointElement]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: ExitPointElement | None = None,
    ) -> ExitPointElement | None:
        transition = find(stack, TransitionElement)
        if transition is not None:
            event_name = _exit_point_event_name(self.qualified_name)
            model.events[event_name] = Event(name=event_name, kind=CompletionEventKind)
            transition.events.append(event_name)
            return None
        namespace = find(stack, NamespaceElement)
        if namespace is None:
            raise ErrorValidatingModel(
                self.location,
                "exit point must be called within Define() or State()",
            )
        exit_point = ExitPointElement(
            qualified_name=join(namespace.qualified_name, self.name()),
            location=self.location,
        )
        model.members[exit_point.qualified_name] = exit_point
        if self.owned_elements:
            transition = RedefinableTransition(
                owned_elements=[
                    RedefinableTransition.Target(qualified_name=exit_point.owner()),
                    *self.owned_elements,
                ],
                location=self.location,
            ).redefine(model, [*stack, exit_point])
            if transition is not None:
                transition.kind = LocalKind
        return exit_point


@dataclasses.dataclass(kw_only=True)
class RedefinableTransitionWithEvents(RedefinableElement[TransitionElement]):
    events: list[Event] = dataclasses.field(default_factory=list)

    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: TransitionElement | None = None,
    ) -> TransitionElement | None:
        transition = find(stack, TransitionElement)
        if transition is None:
            raise ErrorValidatingModel(
                self.location,
                "hsm.Trigger() must be called within a TransitionElement",
            )
        for event in self.events:
            transition.events.append(event.name)
            model.events[event.name] = event
        return transition


@dataclasses.dataclass(kw_only=True)
class RedefinableChoice(RedefinableElement[ChoiceElement]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: ChoiceElement | None = None,
    ) -> ChoiceElement | None:
        state_or_transition = find(stack, StateElement, TransitionElement)
        if state_or_transition is None:
            raise ErrorValidatingModel(
                self.location,
                "choice must be called within a state or transition",
            )
        if isinstance(state_or_transition, TransitionElement):
            source_name = state_or_transition.source
            if source_name in ("", "."):
                source_vertex = find(stack, VertexElement)
                if source_vertex is None:
                    raise ErrorValidatingModel(
                        self.location,
                        "choice must be called within a state",
                    )
                source_name = source_vertex.qualified_name
            if isinstance(
                get(model, source_name, PseudostateElement), PseudostateElement
            ):
                state_or_transition = find(stack, StateElement)
                if state_or_transition is None:
                    raise ErrorValidatingModel(
                        self.location,
                        "choice must be called within a state",
                    )
        qualified_name = join(
            state_or_transition.qualified_name,
            self.qualified_name or f"choice_{len(model.members)}",
        )
        choice = model.members[qualified_name] = ChoiceElement(
            qualified_name=qualified_name,
        )
        return super().redefine(model, stack, choice)


@dataclasses.dataclass(kw_only=True)
class RedefinableFinalState(RedefinableElement[FinalStateElement]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: FinalStateElement | None = None,
    ) -> FinalStateElement | None:
        namespace = find(stack, StateElement)
        if namespace is None:
            raise ErrorValidatingModel(
                self.location,
                "final state must be called within a State",
            )
        final_state = FinalStateElement(
            qualified_name=join(
                namespace.qualified_name,
                self.qualified_name or f"final_{len(model.members)}",
            ),
            location=self.location,
        )
        model.members[final_state.qualified_name] = final_state
        return final_state


@dataclasses.dataclass(kw_only=True)
class RedefinableAttribute(RedefinableElement[AttributeElement]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: AttributeElement | None = None,
    ) -> AttributeElement | None:
        return element


@dataclasses.dataclass(kw_only=True)
class RedefinableOperation(RedefinableElement[OperationElement[TInstance]]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: OperationElement[TInstance] | None = None,
    ) -> OperationElement[TInstance] | None:
        operation = find(stack, OperationElement[TInstance])
        if operation is None:
            raise ErrorValidatingModel(
                self.location,
                "operation must be called within a State or Transition",
            )
        return element


@typing.final
class Mutex:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._signal: generic.Awaitable[None] | None = None

    def wait(self) -> generic.Awaitable[None]:
        if self._signal is None:
            self._signal = generic.Awaitable[None]()
            self._signal.set_result()
        return self._signal

    def try_lock(self) -> bool:
        acquired = self._lock.acquire(blocking=False)
        if acquired:
            self._signal = generic.Awaitable[None]()
        return acquired

    def locked(self) -> bool:
        return self._lock.locked()

    async def lock(self) -> None:
        while not self.try_lock():
            await self.wait()

    def release(self) -> None:
        self._lock.release()
        if self._signal is not None and not self._signal.done():
            self._signal.set_result(None)

    async def __aenter__(self) -> None:
        await self.lock()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        self.release()


@typing.final
class MultiQueue:
    def __init__(self, fifo: generic.Queue[Event] | None = None) -> None:
        self._lock = threading.Lock()
        self._lifo: collections.deque[Event] = collections.deque()
        self._fifo: generic.Queue[Event] = fifo or generic.Queue()

    def push(
        self, _: context.Context, event: Event[typing.Any]
    ) -> BaseException | None:
        if kind.Is(event.kind, CompletionEventKind):
            with self._lock:
                self._lifo.appendleft(event)
            return None
        with self._lock:
            return self._fifo.push(event)

    def pop(self, _: context.Context) -> tuple[Event, bool, BaseException | None]:
        with self._lock:
            if self._lifo:
                return (self._lifo.popleft(), True, None)
            try:
                return self._fifo.pop()
            except BaseException as error:
                return (Event(), False, error)

    def len(self, _: context.Context) -> tuple[int, BaseException | None]:
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


@dataclasses.dataclass(kw_only=True)
class ActiveBehavior:
    context: context.Context
    task: asyncio.Task[None]


@functools.cache
def _done() -> generic.Awaitable[None]:
    awaitable = generic.Awaitable[None]()
    awaitable.set_result()
    return awaitable


@functools.cache
def _error(exception: BaseException) -> generic.Awaitable[None]:
    awaitable = generic.Awaitable[None]()
    awaitable.set_exception(exception)
    return awaitable


class Instance:
    __hsm: "HSM[typing.Self] | None" = None

    def dispatch(
        self, ctx: context.Context, event: Event
    ) -> collections.abc.Awaitable[None]:
        if self.__hsm is None:
            return _error(RuntimeError("dispatch requires a started HSM"))
        return self.__hsm.dispatch(ctx, event)

    def state(self) -> str:
        if self.__hsm is None:
            return ""
        return self.__hsm.state()

    def context(self) -> context.Context:
        if self.__hsm is None:
            return context.Context()
        return self.__hsm.context()

    def clock(self) -> Clock:
        if self.__hsm is None:
            return DefaultClock()
        return self.__hsm.clock()

    # def get(self, name: str) -> tuple[typing.Any, bool]:
    #     if self.__hsm is None:
    #         return None, False
    #     return self.__hsm.get(name)

    # def set(self, name: str, value: typing.Any) -> collections.abc.Awaitable[None]:
    #     if self.__hsm is None:
    #         raise RuntimeError("operation requires a started HSM")
    #     return self.__hsm.set(name, value)

    # def call(self, name: str, *args: typing.Any) -> typing.Awaitable[typing.Any]:
    #     if self.__hsm is None:
    #         raise RuntimeError("operation requires a started HSM")
    #     return self.__hsm.call(name, *args)

    def stop(self, ctx: context.Context) -> collections.abc.Awaitable[None]:
        if self.__hsm is None:
            return _done()
        return self.__hsm.stop(ctx)

    def restart(
        self, ctx: context.Context, data: TData = None
    ) -> collections.abc.Awaitable["HSM[typing.Self] | None"]:
        if self.__hsm is None:
            return _error(RuntimeError("restart requires a started HSM"))
        return self.__hsm.restart(ctx, data)

    def take_snapshot(self) -> Snapshot:
        if self.__hsm is None:
            return Snapshot()
        return self.__hsm.take_snapshot()


class HSM(BehaviorElement[TInstance]):
    __hash__: typing.ClassVar[typing.Any] = object.__hash__
    model: FinalizedModel
    _instance: TInstance
    _processing: Mutex
    _queue: MultiQueue
    _active: dict[str, ActiveBehavior]
    _state: VertexElement
    _context: context.Context
    _cancel: typing.Callable[[], None]
    _clock: Clock

    def __init__(
        self,
        instance: TInstance,
        model: Model,
        ctx: context.Context | None = None,
        config: Config | None = None,
    ):
        config = config or Config()

        def operation(ctx: context.Context, instance: TInstance, event: Event) -> None:
            del instance
            if not self._processing.try_lock():
                return None
            state = self._enter(ctx, self.model, event, True)
            if state is not None:
                self._state = state
            _ = asyncio.Task(
                self._process(ctx),
                loop=asyncio.get_running_loop(),
                eager_start=True,
            )

        super().__init__(
            kind=StateMachineKind,
            id=config.ID or muid.make(),
            qualified_name=config.Name or model.qualified_name,
            operation=operation,
        )
        self.model = (
            model
            if isinstance(model, FinalizedModel)
            else typing.cast(FinalizedModel, DefaultModelFinalizer().finalize(model))
        )
        self._instance = instance
        self._processing = Mutex()
        self._queue = (
            MultiQueue(config.Queue) if config.Queue is not None else MultiQueue()
        )
        # self._after = _AfterWaiters()
        self._state = model
        self._active = {}
        self._cancel = lambda: None
        # self._attributes = _default_attribute_values(model)
        self._history: dict[str, str] = {}
        self._context = ctx or context.Context()
        self._clock = config.Clock or DefaultClock()
        setattr(self._instance, "_Instance__hsm", self)

    def state(self) -> str:
        return self._state.qualified_name

    def context(self) -> context.Context:
        return self._context

    def clock(self) -> Clock:
        return self._clock

    async def _start(self, ctx: context.Context, data: TData = None) -> typing.Self:
        maybe_instances = ctx.value(Keys.Instances)
        if isinstance(maybe_instances, collections.abc.MutableMapping):
            instances = typing.cast(
                collections.abc.MutableMapping[str, Instance], maybe_instances
            )
        else:
            instances = weakref.WeakValueDictionary[str, Instance]()
        instances[self.id] = self._instance
        self._context, self._cancel = context.with_cancel(
            context.with_value(
                context.with_value(
                    context.with_value(ctx, Keys.Instances, instances),
                    Keys.HSM,
                    self,
                ),
                Keys.Owner,
                ctx.value(Keys.HSM),
            )
        )
        self._history.clear()
        self._execute(self._context, self, InitialEvent.WithData(data))
        return self

    def _enter(
        self,
        ctx: context.Context,
        vertex: VertexElement,
        event: Event[TData],
        default_entry: bool,
    ) -> VertexElement | None:
        if isinstance(vertex, StateElement):
            state = vertex
            for behavior_name in (*state.entry, *state.activity):
                behavior = typing.cast(
                    BehaviorElement[TInstance], self.model.members[behavior_name]
                )
                self._execute(ctx, behavior, event)
            if isinstance(state, FinalStateElement):
                completion_event = Event(
                    name=FinalEvent.name,
                    kind=FinalEvent.kind,
                    source=state.qualified_name,
                )
                if error := self._queue.push(ctx, completion_event):
                    raise error
                return state
            if not default_entry or state.initial == "":
                return state
            initial = self.model.members[state.initial]
            if isinstance(initial, VertexElement) and initial.transitions:
                transition_name = initial.transitions[0]
                transition = typing.cast(
                    TransitionElement, self.model.members[transition_name]
                )
                return self._transition(ctx, state, transition, event)
            return state
        elif isinstance(vertex, ChoiceElement):
            for transition_name in vertex.transitions:
                transition = typing.cast(
                    TransitionElement, self.model.members[transition_name]
                )
                if transition.guard is not None:
                    guard = typing.cast(
                        ConstraintElement[TInstance],
                        self.model.members[transition.guard],
                    )
                    if not self._evaluate(ctx, guard, event):
                        continue
                return self._transition(ctx, vertex, transition, event)
            return vertex
        elif isinstance(vertex, EntryPointElement):
            boundary = get(
                self.model,
                posixpath.dirname(vertex.owner()),
                VertexElement,
            )
            if boundary is not None:
                self._state = boundary
            if not vertex.transitions:
                return vertex
            transition = typing.cast(
                TransitionElement, self.model.members[vertex.transitions[0]]
            )
            return self._transition(ctx, vertex, transition, event)
        elif isinstance(vertex, (ShallowHistoryElement, DeepHistoryElement)):
            owner = vertex.owner()
            remembered = self._history.get(vertex.qualified_name)
            if remembered is not None:
                current: VertexElement | None = None
                for entering in self.model.history_paths.get((owner, remembered), []):
                    entry_vertex = typing.cast(
                        VertexElement, self.model.members[entering]
                    )
                    current = self._enter(
                        ctx, entry_vertex, event, entering == remembered
                    )
                return current
            if not vertex.transitions:
                return None
            transition = typing.cast(
                TransitionElement, self.model.members[vertex.transitions[0]]
            )
            if transition.guard is not None:
                guard = typing.cast(
                    ConstraintElement[TInstance],
                    self.model.members[transition.guard],
                )
                if not self._evaluate(ctx, guard, event):
                    return None
            return self._transition(ctx, vertex, transition, event)
        return None

    def _exit(
        self,
        ctx: context.Context,
        vertex: VertexElement,
        event: Event,
    ) -> VertexElement | None:
        if isinstance(vertex, StateElement):
            for behavior_name in vertex.activity:
                behavior = typing.cast(
                    BehaviorElement[TInstance], self.model.members[behavior_name]
                )
                self._terminate(ctx, behavior)
            for behavior_name in vertex.exit:
                behavior = typing.cast(
                    BehaviorElement[TInstance], self.model.members[behavior_name]
                )
                self._execute(ctx, behavior, event)
        return vertex

    def _evaluate(
        self, ctx: context.Context, guard: ConstraintElement[TInstance], event: Event
    ) -> bool:
        return guard.expression(ctx, self._instance, event)

    def _execute(
        self, ctx: context.Context, behavior: BehaviorElement[TInstance], event: Event
    ) -> None:
        if kind.Is(behavior.kind, ConcurrentKind):
            activity_ctx = context.Context(ctx)
            task = asyncio.Task(
                typing.cast(
                    collections.abc.Coroutine[None, None, None],
                    behavior.operation(activity_ctx, self._instance, event),
                ),
                loop=asyncio.get_running_loop(),
                name=behavior.qualified_name,
                eager_start=True,
            )

            self._active[behavior.qualified_name] = ActiveBehavior(
                context=activity_ctx,
                task=task,
            )
            return
        else:
            _ = behavior.operation(ctx, self._instance, event)
            return

    def _terminate(
        self, ctx: context.Context, behavior: BehaviorElement[TInstance]
    ) -> None:
        del ctx
        active = self._active.pop(behavior.qualified_name, None)
        if active is None:
            return
        active.context.cancel()
        _ = active.task.cancel()
        return

    def _select_transition(
        self,
        ctx: context.Context,
        current_state: VertexElement,
        event: Event,
    ) -> TransitionElement | None:
        event_names = [event.name]
        if event.name != AnyEvent.name:
            event_names.append(AnyEvent.name)
        for event_name in event_names:
            transitions = self.model.transition_map.get(
                current_state.qualified_name, {}
            ).get(event_name)
            if not transitions:
                continue
            for transition in transitions:
                if transition.guard is not None:
                    guard = typing.cast(
                        ConstraintElement[TInstance],
                        self.model.members[transition.guard],
                    )
                    if not self._evaluate(ctx, guard, event):
                        continue
                return transition
        return None

    def _process_event(
        self, ctx: context.Context, event: Event[TData]
    ) -> tuple[bool, bool]:
        if not event.id:
            event = Event(
                name=event.name,
                data=event.data,
                kind=event.kind,
                id=muid.make(),
                source=event.source,
                target=event.target,
                schema=event.schema,
            )
        current_state = self._state
        current_qualified_name = current_state.qualified_name
        transitioned = False
        if deferred_set := self.model.deferred_map.get(current_qualified_name):
            if event.name in deferred_set:
                return False, True
        transition = self._select_transition(ctx, current_state, event)
        if transition is not None:
            state = self._transition(ctx, current_state, transition, event)
            if state is not None:
                self._state = state
                transitioned = True
        return transitioned, False

    async def _process(
        self,
        ctx: context.Context,
    ) -> None:
        deferred: list[Event] = []
        while queued := self._queue.pop(ctx):
            event, ok, error = queued
            if error is not None:
                raise error
            if not ok:
                break
            transitioned, defer = self._process_event(ctx, event)
            if defer:
                deferred.append(event)
                continue
            elif transitioned:
                for deferred_event in deferred:
                    if error := self._queue.push(ctx, deferred_event):
                        raise error
                deferred.clear()
                continue
        for deferred_event in deferred:
            if error := self._queue.push(ctx, deferred_event):
                raise error
        self._processing.release()
        return

    def _transition(
        self,
        ctx: context.Context,
        current: VertexElement | None,
        transition: TransitionElement,
        event: Event[TData],
    ) -> VertexElement | None:
        if current is None:
            return None
        path = self.model.transition_paths.get(transition.qualified_name, {}).get(
            current.qualified_name
        )
        if path is None:
            return
        target = get(self.model, transition.target, VertexElement)
        if isinstance(target, ExitPointElement):
            return self._exit_point(ctx, current, transition, target, event)
        skip_history_owner = (
            target.owner()
            if isinstance(target, (ShallowHistoryElement, DeepHistoryElement))
            else None
        )
        if path.exit and (
            history_targets := self.model.history_targets.get(
                (current.qualified_name, skip_history_owner)
            )
        ):
            self._history.update(history_targets)
        for exiting in path.exit:
            vertex = typing.cast(StateElement, self.model.members[exiting])
            if not self._exit(ctx, vertex, event):
                return None
        for effect in transition.effect:
            behavior = typing.cast(
                BehaviorElement[TInstance], self.model.members[effect]
            )
            self._execute(ctx, behavior, event)
        if kind.Is(transition.kind, InternalKind):
            return current
        for entering in path.enter:
            vertex = typing.cast(VertexElement, self.model.members[entering])
            default_entry = entering == transition.target
            current = self._enter(ctx, vertex, event, default_entry)
            if default_entry:
                return current
        return typing.cast(VertexElement, self.model.members[transition.target])

    def _exit_point(
        self,
        ctx: context.Context,
        current: VertexElement,
        transition: TransitionElement,
        exit_point: ExitPointElement,
        event: Event[TData],
    ) -> VertexElement | None:
        path = self.model.transition_paths.get(transition.qualified_name, {}).get(
            current.qualified_name
        )
        if path is None:
            return None
        boundary_name = posixpath.dirname(exit_point.owner())
        boundary = get(self.model, boundary_name, VertexElement)
        if boundary is None:
            return None
        exit_event = Event(
            name=_exit_point_event_name(exit_point.name()),
            data=event.data,
            kind=CompletionEventKind,
            id=event.id,
            source=boundary_name,
            target="",
            schema=event.schema,
        )
        for exiting in path.exit:
            vertex = typing.cast(StateElement, self.model.members[exiting])
            if not self._exit(ctx, vertex, event):
                return None
        for effect in transition.effect:
            behavior = typing.cast(
                BehaviorElement[TInstance], self.model.members[effect]
            )
            self._execute(ctx, behavior, event)
        transitions = self.model.transition_map.get(boundary_name, {}).get(
            exit_event.name
        )
        if not transitions:
            raise RuntimeError(f'unhandled exit point "{exit_point.name()}"')
        original_state = self._state
        self._state = boundary
        for transition_name in exit_point.transitions:
            exit_transition = typing.cast(
                TransitionElement, self.model.members[transition_name]
            )
            for effect in exit_transition.effect:
                behavior = typing.cast(
                    BehaviorElement[TInstance], self.model.members[effect]
                )
                self._execute(ctx, behavior, exit_event)
        exit_transition = self._select_transition(ctx, boundary, exit_event)
        if exit_transition is None:
            self._state = original_state
            raise RuntimeError(f'unhandled exit point "{exit_point.name()}"')
        return self._transition(ctx, boundary, exit_transition, exit_event)

    async def _restart(
        self, ctx: context.Context, data: TData = None
    ) -> typing.Self | None:
        ctx_done = asyncio.wrap_future(ctx.done())
        stop_task = asyncio.Task(
            self._stop(ctx),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )
        done, pending = await asyncio.wait(
            [ctx_done, stop_task], return_when=asyncio.FIRST_COMPLETED
        )
        for pending_task in pending:
            if not isinstance(pending_task, asyncio.Task):
                continue
            _ = pending_task.cancel()
            try:
                await pending_task
            except asyncio.CancelledError:
                pass
        if ctx_done in done or ctx.is_done():
            return
        await stop_task
        return await self._start(ctx, data)

    def dispatch(
        self,
        ctx: context.Context,
        event: Event[TData],
    ) -> collections.abc.Awaitable[None]:
        if error := self._queue.push(ctx, event):
            raise error
        if self._processing.try_lock():
            return asyncio.Task(
                self._process(ctx),
                loop=asyncio.get_running_loop(),
                eager_start=True,
            )
        return self._processing.wait()

    async def _stop(self, ctx: context.Context) -> None:
        async with self._processing:
            self._state = self.model
        ctx_done = asyncio.wrap_future(ctx.done())
        processing_wait = asyncio.ensure_future(self._processing.wait())
        done, pending = await asyncio.wait(
            [ctx_done, processing_wait], return_when=asyncio.FIRST_COMPLETED
        )
        for pending_task in pending:
            if not isinstance(pending_task, asyncio.Task):
                continue
            _ = pending_task.cancel()
            try:
                await pending_task
            except asyncio.CancelledError:
                pass
        if processing_wait in done:
            await processing_wait

    def take_snapshot(self) -> Snapshot:
        queue_len, error = self._queue.len(self._context)
        if error is not None:
            raise error
        return Snapshot(
            ID=self.id,
            QualifiedName=self._state.qualified_name,
            State=self._state.qualified_name,
            Attributes={},
            QueueLen=queue_len,
            Transitions=tuple(
                t
                for transitions in self.model.transition_map[
                    self._state.qualified_name
                ].values()
                for t in transitions
            ),
        )

    def stop(self, ctx: context.Context) -> collections.abc.Awaitable[None]:
        return asyncio.Task(
            self._stop(ctx),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )

    def restart(
        self, ctx: context.Context, data: TData = None
    ) -> collections.abc.Awaitable[typing.Self | None]:
        return asyncio.Task(
            self._restart(ctx, data),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )


class Group(Instance):
    _id: str = ""
    _context: context.Context
    _instances: list[Instance]

    def __init__(
        self,
        *instances: str | Instance | "Group" | None,
        ctx: context.Context | None = None,
        id: str | None = None,
    ):
        self._context = ctx or context.Context()
        self._instances = []
        self._id = id or muid.make()
        for instance in instances:
            if instance is None:
                continue
            if isinstance(instance, Group):
                self._instances.extend(instance._instances)
            elif isinstance(instance, Instance):
                self._instances.append(instance)

    @typing.override
    def state(self) -> str:
        if not self._instances:
            return ""
        snapshots = [instance.take_snapshot() for instance in self._instances]
        return ",".join(
            join(snapshot.QualifiedName, snapshot.ID, snapshot.State)
            for snapshot in snapshots
        )

    @typing.override
    def context(self) -> context.Context:
        return self._context

    @typing.override
    def dispatch(
        self,
        ctx: context.Context,
        event: Event[TData],
    ) -> collections.abc.Awaitable[None]:
        async def dispatch_all():
            _ = await asyncio.gather(
                *[instance.dispatch(ctx, event) for instance in self._instances]
            )

        return asyncio.ensure_future(dispatch_all())

    @typing.override
    def stop(self, ctx: context.Context) -> collections.abc.Awaitable[None]:
        async def stop_all():
            _ = await asyncio.gather(
                *[instance.stop(ctx) for instance in self._instances]
            )

        return asyncio.ensure_future(stop_all())

    @typing.override
    def restart(
        self, ctx: context.Context, data: TData = None
    ) -> collections.abc.Awaitable[None]:
        async def restart_all():
            _ = await asyncio.gather(
                *[instance.restart(ctx, data) for instance in self._instances]
            )

        return asyncio.ensure_future(restart_all())

def Define(name: str, *elements: Element) -> Model:
    validator: ModelValidator = DefaultModelValidator()
    finalizer: ModelFinalizer = DefaultModelFinalizer()
    model_elements: list[Element] = []
    for element in elements:
        if isinstance(element, ValidatorElement):
            validator = element.validator
        elif isinstance(element, FinalizerElement):
            finalizer = element.finalizer
        else:
            model_elements.append(element)
    qualified_name = join("/", name)
    model = Model(qualified_name=qualified_name, owned_elements=model_elements)
    model = RedefinableModel(
        qualified_name=qualified_name, owned_elements=model_elements
    ).redefine(model, [], model)

    if model is None:
        raise ErrorValidatingModel(
            Location.capture(),
            "failed to define model",
        )

    validator.validate(model)

    return finalizer.finalize(model)


def State(name: str, *elements: Element) -> RedefinableState:
    return RedefinableState(qualified_name=name, owned_elements=list(elements))


def Validator(validator: ModelValidator) -> ValidatorElement:
    return ValidatorElement(validator=validator)


def Finalizer(finalizer: ModelFinalizer) -> FinalizerElement:
    return FinalizerElement(finalizer=finalizer)


def SubmachineState(
    name: str, machine: Model, *elements: Element
) -> RedefinableSubmachineState:
    return RedefinableSubmachineState(
        qualified_name=name, machine=machine, owned_elements=list(elements)
    )


def Initial(name_or_element: str | Element, *elements: Element) -> RedefinableInitial:
    name = ".initial"
    owned_elements = list(elements)
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.insert(0, name_or_element)
    return RedefinableInitial(qualified_name=name, owned_elements=owned_elements)


def Transition(
    name_or_element: str | Element, *elements: Element
) -> RedefinableTransition:
    name = ""
    owned_elements = list(elements)
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.insert(0, name_or_element)
    return RedefinableTransition(qualified_name=name, owned_elements=owned_elements)


def Source(name_or_element: str | Element) -> RedefinableTransition.Source:
    if isinstance(name_or_element, str):
        return RedefinableTransition.Source(qualified_name=name_or_element)
    return RedefinableTransition.Source(owned_elements=[name_or_element])


def Target(name_or_element: str | Element) -> RedefinableTransition.Target:
    if isinstance(name_or_element, str):
        return RedefinableTransition.Target(qualified_name=name_or_element)
    return RedefinableTransition.Target(owned_elements=[name_or_element])


def Entry(
    *operations: OperationExpression[TInstance] | BehaviorElement[TInstance],
) -> RedefinableEntryBehavior[TInstance]:
    return RedefinableEntryBehavior(behaviors=list(operations))


def Exit(
    *operations: OperationExpression[TInstance] | BehaviorElement[TInstance],
) -> RedefinableExitBehavior[TInstance]:
    return RedefinableExitBehavior(behaviors=list(operations))


def Activity(
    *operations: OperationExpression[TInstance] | BehaviorElement[TInstance],
) -> RedefinableActivityBehavior[TInstance]:
    return RedefinableActivityBehavior(behaviors=list(operations))


def Effect(
    *operations: OperationExpression[TInstance] | BehaviorElement[TInstance],
) -> RedefinableEffectBehavior[TInstance]:
    return RedefinableEffectBehavior(behaviors=list(operations))


def Guard(
    expression: Expression[TInstance, bool],
) -> RedefinableConstraint[TInstance]:
    return RedefinableConstraint(
        qualified_name=getattr(expression, "__name__", ".guard"),
        expression=expression,
    )


def On(*events: str | Event) -> RedefinableTransitionWithEvents:
    return RedefinableTransitionWithEvents(
        events=[
            Event(name=event) if isinstance(event, str) else event for event in events
        ]
    )


def After(
    duration: TimeExpression[TInstance],
) -> RedefinableTransition.AfterEvent[TInstance]:
    return RedefinableTransition.AfterEvent(
        qualified_name=getattr(duration, "__name__", ""),
        expression=duration,
    )


def At(
    timepoint: TimeExpression[TInstance],
) -> RedefinableTransition.AtEvent[TInstance]:
    return RedefinableTransition.AtEvent(
        qualified_name=getattr(timepoint, "__name__", ""),
        expression=timepoint,
    )


def Every(
    duration: TimeExpression[TInstance],
) -> RedefinableTransition.EveryEvent[TInstance]:
    return RedefinableTransition.EveryEvent(
        qualified_name=getattr(duration, "__name__", ""),
        expression=duration,
    )


def When(
    expression: Expression[TInstance, typing.Any],
) -> RedefinableTransition.WhenEvent[TInstance]:
    return RedefinableTransition.WhenEvent(
        qualified_name=getattr(expression, "__name__", ""),
        expression=expression,
    )


def Defer(*events: str | Event) -> RedefinableStateWithDeferredEvents:
    return RedefinableStateWithDeferredEvents(
        deferred=[
            Event(name=event) if isinstance(event, str) else event for event in events
        ]
    )


def Choice(
    element_or_name: str | Element,
    *transitions: RedefinableTransition,
) -> RedefinableChoice:
    name = ""
    owned_elements: list[Element] = list(transitions)
    if isinstance(element_or_name, str):
        name = element_or_name
    else:
        owned_elements.insert(0, element_or_name)
    return RedefinableChoice(qualified_name=name, owned_elements=owned_elements)


def ShallowHistory(
    element_or_name: str | Element,
    *elements: Element,
) -> RedefinableHistory:
    name = ""
    owned_elements = list(elements)
    if isinstance(element_or_name, str):
        name = element_or_name
    else:
        owned_elements.insert(0, element_or_name)
    return RedefinableHistory(
        kind=ShallowHistoryKind,
        qualified_name=name,
        owned_elements=owned_elements,
    )


def DeepHistory(
    element_or_name: str | Element,
    *elements: Element,
) -> RedefinableHistory:
    name = ""
    owned_elements = list(elements)
    if isinstance(element_or_name, str):
        name = element_or_name
    else:
        owned_elements.insert(0, element_or_name)
    return RedefinableHistory(
        kind=DeepHistoryKind,
        qualified_name=name,
        owned_elements=owned_elements,
    )


def Final(name_or_element: str | Element) -> RedefinableFinalState:
    if isinstance(name_or_element, str):
        return RedefinableFinalState(qualified_name=name_or_element)
    return RedefinableFinalState(owned_elements=[name_or_element])


def EntryPoint(name: str, *elements: Element) -> RedefinableEntryPoint:
    return RedefinableEntryPoint(qualified_name=name, owned_elements=list(elements))


def ExitPoint(name: str, *elements: Element) -> RedefinableExitPoint:
    return RedefinableExitPoint(qualified_name=name, owned_elements=list(elements))


def Start(
    ctx: context.Context | None,
    sm: HSM[TInstance],
    data: TData = None,
) -> collections.abc.Awaitable[HSM[TInstance]]:
    return asyncio.Task(
        sm._start(ctx or sm.context(), data),
        loop=asyncio.get_running_loop(),
        eager_start=True,
    )


def Started(
    ctx: context.Context | None,
    instance: TInstance,
    model: Model,
    config: Config | None = None,
) -> collections.abc.Awaitable[HSM[TInstance]]:
    hsm = HSM(instance=instance, model=model, config=config)
    return Start(ctx, hsm, config.Data if config is not None else None)


def Stop(
    sm: HSM[TInstance], ctx: context.Context | None = None
) -> collections.abc.Awaitable[None]:
    return asyncio.Task(
        sm._stop(ctx or sm.context()),
        loop=asyncio.get_running_loop(),
        eager_start=True,
    )


def Dispatch(
    ctx: context.Context | None,
    hsm: HSM[TInstance] | Instance,
    event: Event,
) -> collections.abc.Awaitable[None]:
    return hsm.dispatch(ctx or hsm.context(), event)


def DispatchAll(
    ctx: context.Context | None,
    event: Event[TData],
) -> collections.abc.Awaitable[None]:
    return DispatchTo(ctx, event)


def DispatchTo(
    ctx: context.Context | None,
    event: Event[TData],
    *ids: str,
) -> collections.abc.Awaitable[None]:
    if ctx is None or ctx.is_done():
        return _done()
    maybe_instances = ctx.value(Keys.Instances)
    if isinstance(maybe_instances, collections.abc.Mapping):
        instances = typing.cast(
            collections.abc.Mapping[object, object], maybe_instances
        )
        candidates: list[object] = list(instances.values())
    else:
        return _done()
    completions: list[collections.abc.Awaitable[None]] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, Instance):
            continue
        machine = getattr(candidate, "_Instance__hsm", None)
        if (
            not isinstance(machine, HSM)
            or machine.state() == machine.model.qualified_name
        ):
            continue
        snapshot = machine.take_snapshot()
        if snapshot.ID in seen:
            continue
        if ids and not any(fnmatch.fnmatchcase(snapshot.ID, id_) for id_ in ids):
            continue
        seen.add(snapshot.ID)
        completions.append(
            candidate.dispatch(
                ctx,
                Event(
                    name=event.name,
                    data=event.data,
                    kind=event.kind,
                    id=event.id,
                    source=event.source,
                    target=event.target or snapshot.ID,
                    schema=event.schema,
                ),
            )
        )
    if not completions:
        return _done()

    async def wait_all() -> None:
        _ = await asyncio.gather(
            *(asyncio.shield(completion) for completion in completions)
        )

    return asyncio.Task(
        wait_all(),
        loop=asyncio.get_running_loop(),
        eager_start=True,
    )


Context = context.Context
ContextKey = context.ContextKey

define = Define
state = State
submachine_state = SubmachineState
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
shallow_history = ShallowHistory
deep_history = DeepHistory
final = Final
validator = Validator
finalizer = Finalizer
entry_point = EntryPoint
exit_point = ExitPoint
start = Start
started = Started
stop = Stop
dispatch = Dispatch
dispatch_all = DispatchAll
dispatch_to = DispatchTo

__all__ = [
    "Activity",
    "After",
    "AnyEvent",
    "At",
    "AttributeChange",
    "AttributeElement",
    "AttributeKind",
    "BehaviorElement",
    "BehaviorKind",
    "CallData",
    "CallEventKind",
    "Choice",
    "ChoiceElement",
    "ChoiceKind",
    "Clock",
    "CompletionEvent",
    "CompletionEventKind",
    "ConcurrentKind",
    "Config",
    "ConstraintElement",
    "ConstraintKind",
    "Context",
    "ContextKey",
    "DeepHistory",
    "DeepHistoryElement",
    "DeepHistoryKind",
    "DefaultClock",
    "DefaultModelFinalizer",
    "DefaultModelValidator",
    "Defer",
    "Define",
    "Dispatch",
    "DispatchAll",
    "DispatchTo",
    "Effect",
    "Element",
    "ElementKind",
    "Entry",
    "EntryPoint",
    "EntryPointElement",
    "EntryPointKind",
    "ErrorAlreadyStarted",
    "ErrorEvent",
    "ErrorEventKind",
    "ErrorInvalidOperation",
    "ErrorInvalidState",
    "ErrorMissingHSM",
    "ErrorMissingOperation",
    "ErrorValidatingModel",
    "Event",
    "EventKind",
    "Every",
    "Exit",
    "ExitPoint",
    "ExitPointElement",
    "ExitPointKind",
    "ExternalKind",
    "Final",
    "Finalizer",
    "FinalizerElement",
    "FinalEvent",
    "FinalizedModel",
    "FinalStateElement",
    "FinalStateKind",
    "Guard",
    "Group",
    "HSM",
    "Initial",
    "InitialElement",
    "InitialEvent",
    "InitialKind",
    "InfiniteDuration",
    "Instance",
    "InternalKind",
    "IsAncestor",
    "Keys",
    "LCA",
    "LocalKind",
    "Location",
    "Model",
    "ModelFinalizer",
    "ModelValidator",
    "NamespaceElement",
    "NamespaceKind",
    "NullKind",
    "On",
    "OperationElement",
    "OperationKind",
    "PseudostateElement",
    "PseudostateKind",
    "RedefinableElement",
    "SelfKind",
    "SequentialKind",
    "ShallowHistory",
    "ShallowHistoryElement",
    "ShallowHistoryKind",
    "Snapshot",
    "Source",
    "Start",
    "Started",
    "State",
    "StateElement",
    "StateKind",
    "StateMachineKind",
    "Stop",
    "SubmachineState",
    "SubmachineStateKind",
    "Target",
    "TimeEventKind",
    "Timer",
    "Transition",
    "TransitionElement",
    "TransitionKind",
    "TransitionPath",
    "ValidationError",
    "Validator",
    "ValidatorElement",
    "VertexElement",
    "VertexKind",
    "When",
    "activity",
    "after",
    "at",
    "choice",
    "deep_history",
    "define",
    "defer",
    "dispatch",
    "dispatch_all",
    "dispatch_to",
    "effect",
    "entry",
    "entry_point",
    "every",
    "exit",
    "exit_point",
    "final",
    "finalizer",
    "guard",
    "initial",
    "on",
    "shallow_history",
    "source",
    "start",
    "started",
    "state",
    "stop",
    "submachine_state",
    "target",
    "transition",
    "validator",
    "when",
]
