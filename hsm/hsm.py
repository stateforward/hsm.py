from __future__ import annotations

import asyncio
import collections
import collections.abc
import copy
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
    collections.abc.Awaitable[typing.Any] | typing.Any,
]
OperationMethod = typing.Callable[..., typing.Any]
ObservationExpression = OperationExpression[TInstance]


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
ConcurrentKind = kind.Make(BehaviorKind)
StateMachineKind = kind.Make(ConcurrentKind, NamespaceKind)
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
ObservationKind = kind.Make(ElementKind)


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
    owned_elements: list[Element] = dataclasses.field(
        default_factory=typing.cast(typing.Callable[[], list["Element"]], list)
    )
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
    members: dict[str, Element] = dataclasses.field(default_factory=dict[str, Element])


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
class ConcurrentBehaviorElement(
    BehaviorElement[TInstance], typing.Generic[TInstance], kind=ConcurrentKind
):
    pass


@typing.runtime_checkable
class RedefinableObservation(typing.Protocol[TInstance]):
    def redefine(
        self,
        model: "Model",
        stack: list[Element],
        element: "ObservationElement[TInstance] | None" = None,
    ) -> "ObservationElement[TInstance] | None": ...


@dataclasses.dataclass(kw_only=True)
class ObservationElement(
    Element,
    typing.Generic[TInstance],
    RedefinableObservation[TInstance],
    kind=ObservationKind,
):
    operation: ObservationExpression[TInstance] = dataclasses.field(
        default=lambda ctx, instance, event: None
    )
    targets: list[str] = dataclasses.field(default_factory=list[str])

    @typing.override
    def redefine(
        self,
        model: "Model",
        stack: list[Element],
        element: "ObservationElement[TInstance] | None" = None,
    ) -> "ObservationElement[TInstance] | None":
        del stack, element
        qualified_name = join(
            model.qualified_name,
            self.qualified_name or f"observation_{_model_member_count(model)}",
        )
        observation = ObservationElement(
            qualified_name=qualified_name,
            location=self.location,
            operation=self.operation,
            targets=self.targets,
        )
        model.members[qualified_name] = observation
        for member in list(model.members.values()):
            if member is observation or IsAncestor(
                observation.qualified_name, member.qualified_name
            ):
                continue
            if isinstance(member, TransitionElement):
                if not (
                    observation.matches(member.qualified_name)
                    or any(observation.matches(event) for event in member.events)
                ):
                    continue
                member.effect.insert(
                    0,
                    observation.observe_event(
                        model, member.qualified_name
                    ).qualified_name,
                )
            if isinstance(member, BehaviorElement) and observation.matches(
                member.qualified_name
            ):
                observation.wrap_behavior(
                    model, typing.cast(BehaviorElement[TInstance], member)
                )
        return observation

    def matches(self, qualified_name: str) -> bool:
        return not self.targets or qualified_name in self.targets

    def observe_event(
        self,
        model: "Model",
        qualified_name: str,
    ) -> BehaviorElement[TInstance]:
        observed_name = qualified_name
        behavior_name = join(
            self.qualified_name,
            "event",
            observed_name.removeprefix("/"),
        )

        def observed_event(event: Event) -> Event[dict[str, object]]:
            time = datetime.datetime.now(datetime.timezone.utc)
            return Event(
                name="hsm/observation",
                data={
                    "event": event,
                    "occurrence": "event",
                    "time": time,
                },
                source=observed_name,
                target=event.target,
                schema=event.schema,
            )

        def operation(ctx: context.Context, instance: TInstance, event: Event) -> None:
            _ = self.operation(ctx, instance, observed_event(event))

        behavior = BehaviorElement(
            kind=BehaviorKind,
            qualified_name=behavior_name,
            operation=typing.cast(OperationExpression[TInstance], operation),
            location=self.location,
        )
        model.members[behavior.qualified_name] = behavior
        return behavior

    def wrap_behavior(
        self,
        model: "Model",
        behavior: BehaviorElement[TInstance],
    ) -> BehaviorElement[TInstance]:
        original = behavior.operation
        qualified_name = behavior.qualified_name

        def observed_event(event: Event) -> Event[dict[str, object]]:
            return Event(
                name="hsm/observation",
                data={
                    "event": event,
                    "occurrence": "behavior",
                    "time": datetime.datetime.now(datetime.timezone.utc),
                },
                source=qualified_name,
                target=event.target,
                schema=event.schema,
            )

        async def concurrent_operation(
            ctx: context.Context, instance: TInstance, event: Event
        ) -> None:
            result = self.operation(ctx, instance, observed_event(event))
            if isinstance(result, collections.abc.Awaitable):
                await result
            original_result = original(ctx, instance, event)
            if isinstance(original_result, collections.abc.Awaitable):
                await original_result

        def operation(ctx: context.Context, instance: TInstance, event: Event) -> None:
            _ = self.operation(ctx, instance, observed_event(event))
            _ = original(ctx, instance, event)

        if isinstance(behavior, ConcurrentBehaviorElement):
            wrapped = ConcurrentBehaviorElement(
                kind=behavior.kind,
                qualified_name=qualified_name,
                operation=typing.cast(
                    OperationExpression[TInstance], concurrent_operation
                ),
                location=behavior.location,
            )
        elif isinstance(behavior, OperationElement):
            wrapped = OperationElement(
                kind=behavior.kind,
                qualified_name=qualified_name,
                method=behavior.method,
                operation=operation,
                location=behavior.location,
            )
        else:
            wrapped = BehaviorElement(
                kind=behavior.kind,
                qualified_name=qualified_name,
                operation=operation,
                location=behavior.location,
            )
        model.members[qualified_name] = wrapped
        return wrapped


@dataclasses.dataclass(kw_only=True)
class VertexElement(Element, kind=VertexKind):
    transitions: list[str] = dataclasses.field(default_factory=list[str])


@dataclasses.dataclass(kw_only=True)
class StateElement(VertexElement, NamespaceElement, kind=StateKind):
    initial: str = dataclasses.field(default_factory=str)
    entry: list[str] = dataclasses.field(default_factory=list[str])
    exit: list[str] = dataclasses.field(default_factory=list[str])
    activity: list[str] = dataclasses.field(default_factory=list[str])
    deferred: list[str] = dataclasses.field(default_factory=list[str])
    submachine: "Model | None" = None


@dataclasses.dataclass(kw_only=True)
class AttributeElement(Element, kind=AttributeKind):
    declared_name: str = ""
    default: typing.Any = None
    value_type: type[typing.Any] | None = None
    dynamic: bool = False
    implicit: bool = False


@dataclasses.dataclass(kw_only=True)
class OperationElement(
    BehaviorElement[TInstance], typing.Generic[TInstance], kind=OperationKind
):
    method: OperationMethod | None = dataclasses.field(default=None)


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
    metadata: dict[str, object] = dataclasses.field(default_factory=dict[str, object])

    def WithData[TNewData](self, data: TNewData) -> "Event[TNewData]":
        return Event(
            name=self.name,
            data=data,
            kind=self.kind,
            id=self.id,
            source=self.source,
            target=self.target,
            schema=self.schema,
            metadata=self.metadata,
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
            metadata=self.metadata,
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
    enter: list[str] = dataclasses.field(default_factory=list[str])
    exit: list[str] = dataclasses.field(default_factory=list[str])


@dataclasses.dataclass(kw_only=True)
class TransitionElement(Element, kind=TransitionKind):
    source: str = dataclasses.field(default_factory=str)
    target: str = dataclasses.field(default_factory=str)
    guard: str | None = None
    effect: list[str] = dataclasses.field(default_factory=list[str])
    events: list[str] = dataclasses.field(default_factory=list[str])


@dataclasses.dataclass(kw_only=True)
class ModelValidator(abc.ABC):
    validated: list[str] = dataclasses.field(default_factory=list[str])
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
    def _validate_concurrent_behavior(
        self, model: "Model", behavior: ConcurrentBehaviorElement[typing.Any]
    ) -> None:
        del model
        if not inspect.iscoroutinefunction(behavior.operation):
            raise ErrorValidatingModel(
                behavior.location,
                "concurrent behavior must be an async function",
            )

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
        if transition.target == "" and not transition.effect:
            raise ErrorValidatingModel(
                transition.location,
                f"target or effect is required for transition '{transition.qualified_name}'",
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
                boundary = model.members.get(owner)
                if not (
                    isinstance(boundary, StateElement)
                    and kind.Is(boundary.kind, SubmachineStateKind)
                ):
                    boundary = model.members.get(posixpath.dirname(owner))
                if not isinstance(source, ExitPointElement) and IsAncestor(
                    owner, transition.source
                ):
                    raise ErrorValidatingModel(
                        transition.location,
                        "entry point target cannot be internal",
                    )
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
    members: dict[str, Element] = dataclasses.field(default_factory=dict[str, Element])
    events: dict[str, Event[typing.Any]] = dataclasses.field(
        default_factory=dict[str, Event[typing.Any]]
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.members[self.qualified_name] = self

    def redefine(
        self,
        model: "Model",
        stack: list[Element],
        element: TElement | None = None,
    ) -> TElement | None:
        if element is None and isinstance(stack, tuple):
            return typing.cast(
                TElement,
                RedefinableModel(
                    qualified_name=self.qualified_name,
                    owned_elements=[*self.owned_elements, *stack],
                    location=self.location,
                ).redefine(model, []),
            )
        if isinstance(element, StateElement):
            _ = RedefinableElement[StateElement](
                owned_elements=self.owned_elements,
            ).redefine(model, [*stack, self], element)
            return element
        return RedefinableElement[TElement](
            owned_elements=self.owned_elements,
        ).redefine(model, stack, element)


@dataclasses.dataclass(kw_only=True)
class FinalizedModel(Model):
    transition_map: dict[str, dict[str, list[TransitionElement]]] = dataclasses.field(
        default_factory=dict[str, dict[str, list[TransitionElement]]]
    )
    deferred_map: dict[str, dict[str, str]] = dataclasses.field(
        default_factory=dict[str, dict[str, str]]
    )
    transition_paths: dict[str, dict[str, TransitionPath]] = dataclasses.field(
        default_factory=dict[str, dict[str, TransitionPath]]
    )
    history_paths: dict[tuple[str, str], tuple[str, ...]] = dataclasses.field(
        default_factory=dict[tuple[str, str], tuple[str, ...]]
    )
    history_targets: dict[tuple[str, str | None], dict[str, str]] = dataclasses.field(
        default_factory=dict[tuple[str, str | None], dict[str, str]]
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


def _operation_element(
    model: Model, qualified_name: str
) -> OperationElement[typing.Any] | None:
    member = model.members.get(qualified_name)
    if isinstance(member, OperationElement):
        return typing.cast(OperationElement[typing.Any], member)
    return None


def _model_member_count(model: Model) -> int:
    return sum(
        1
        for member in model.members.values()
        if not isinstance(member, (AttributeElement, OperationElement))
    )


def _attribute_accepts(value_type: type[typing.Any] | None, value: typing.Any) -> bool:
    if value_type is None:
        return True
    if value_type is float:
        return (type(value) is int or type(value) is float) and not isinstance(
            value, bool
        )
    return type(value) is value_type


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


class Clock:
    def __init__(
        self,
        *,
        sleep: typing.Callable[[datetime.timedelta], typing.Any] | None = None,
        after: typing.Callable[[datetime.timedelta], typing.Any] | None = None,
        new_timer: typing.Callable[[datetime.timedelta], Timer] | None = None,
        Sleep: typing.Callable[[datetime.timedelta], typing.Any]
        | None
        | object = dataclasses.MISSING,
        After: typing.Callable[[datetime.timedelta], typing.Any]
        | None
        | object = dataclasses.MISSING,
        NewTimer: typing.Callable[[datetime.timedelta], Timer]
        | None
        | object = dataclasses.MISSING,
    ) -> None:
        if Sleep is not dataclasses.MISSING:
            sleep = typing.cast(
                typing.Callable[[datetime.timedelta], typing.Any] | None, Sleep
            )
        if After is not dataclasses.MISSING:
            after = typing.cast(
                typing.Callable[[datetime.timedelta], typing.Any] | None, After
            )
        if NewTimer is not dataclasses.MISSING:
            new_timer = typing.cast(
                typing.Callable[[datetime.timedelta], Timer] | None, NewTimer
            )
        self._sleep = sleep
        self._after = after
        self._new_timer = new_timer

    async def Sleep(self, duration: datetime.timedelta) -> None:
        if self._sleep is None:
            _ = await sleep(duration)
            return
        result: object = self._sleep(duration)
        if isinstance(result, collections.abc.Awaitable):
            await typing.cast(collections.abc.Awaitable[object], result)

    def After(
        self, duration: datetime.timedelta
    ) -> collections.abc.Awaitable[datetime.datetime]:
        if self._after is not None:
            result = self._after(duration)
            if isinstance(result, collections.abc.Awaitable):
                return typing.cast(collections.abc.Awaitable[datetime.datetime], result)

            async def completed_after() -> datetime.datetime:
                if isinstance(result, datetime.datetime):
                    return result
                return datetime.datetime.now()

            return asyncio.Task(
                completed_after(),
                loop=asyncio.get_running_loop(),
                eager_start=True,
            )

        async def wait() -> datetime.datetime:
            if self._sleep is None:
                return await sleep(duration)
            result: object = self._sleep(duration)
            if isinstance(result, collections.abc.Awaitable):
                result = await typing.cast(collections.abc.Awaitable[object], result)
            if isinstance(result, datetime.datetime):
                return result
            return datetime.datetime.now()

        return asyncio.Task(
            wait(),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )

    def NewTimer(self, duration: datetime.timedelta) -> Timer:
        if self._new_timer is not None:
            return self._new_timer(duration)
        return Timer(duration)

    after: typing.Callable[
        [typing.Self, datetime.timedelta],
        collections.abc.Awaitable[datetime.datetime],
    ] = After
    sleep: typing.Callable[
        [typing.Self, datetime.timedelta], typing.Awaitable[None]
    ] = Sleep
    new_timer: typing.Callable[[typing.Self, datetime.timedelta], Timer] = NewTimer


class DefaultClock(Clock):
    pass


QueuePushResult = generic.QueuePushResult
QueuePopResult: typing.TypeAlias = generic.QueuePopResult[Event]
QueueLenResult = generic.QueueLenResult
Fifo: typing.TypeAlias = generic.Queue[Event]


@dataclasses.dataclass(init=False, kw_only=True, frozen=True)
class Config(typing.Generic[TData]):
    id: str = dataclasses.field(default="")
    name: str = dataclasses.field(default="")
    data: TData | None = dataclasses.field(default=None)
    clock: Clock | None = dataclasses.field(default=None)
    queue: Fifo | MultiQueue | None = dataclasses.field(default=None)

    def __init__(
        self,
        *,
        id: str = "",
        name: str = "",
        data: TData | None = None,
        clock: Clock | None = None,
        queue: Fifo | MultiQueue | None = None,
        ID: str | object = dataclasses.MISSING,
        Name: str | object = dataclasses.MISSING,
        Data: TData | object = dataclasses.MISSING,
        Clock: Clock | None | object = dataclasses.MISSING,
        Queue: Fifo | MultiQueue | None | object = dataclasses.MISSING,
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
            queue = typing.cast(Fifo | MultiQueue | None, Queue)
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
    def Queue(self) -> Fifo | MultiQueue | None:
        return self.queue


@dataclasses.dataclass
class CallData:
    name: str
    args: tuple[typing.Any, ...]


@dataclasses.dataclass
class AttributeChange:
    name: str
    old_value: typing.Any
    value: typing.Any

    @property
    def Old(self) -> typing.Any:
        return self.old_value

    @property
    def New(self) -> typing.Any:
        return self.value

    @property
    def Value(self) -> typing.Any:
        return self.value

    @property
    def Name(self) -> str:
        return self.name


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
        if model.members.get(state.qualified_name) is not None:
            raise ErrorValidatingModel(
                self.location,
                f"state '{state.qualified_name}' already defined",
            )
        model.members[state.qualified_name] = state
        for owned_element in self.owned_elements:
            if isinstance(owned_element, RedefinableAttribute):
                raise ErrorValidatingModel(
                    owned_element.location,
                    "attribute must be called within Define()",
                )
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
        if model.members.get(state.qualified_name) is not None:
            raise ErrorValidatingModel(
                self.location,
                f"state '{state.qualified_name}' already defined",
            )
        model.members[state.qualified_name] = state
        _ = self.machine.redefine(model, stack, state)
        for owned_element in self.owned_elements:
            if isinstance(owned_element, RedefinableAttribute):
                raise ErrorValidatingModel(
                    owned_element.location,
                    "attribute must be called within Define()",
                )
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
            count = _model_member_count(model)
            name = (
                f"shallow_history_{count}"
                if self.kind == ShallowHistoryKind
                else f"deep_history_{count}"
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
        self._finalize_transition_map(finalized)
        self._finalize_deferred_map(finalized)
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
            model.transition_paths[element.qualified_name] = (
                self._finalize_paths_for_transition(model, element)
            )

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
        source = model.members.get(transition.source)
        if isinstance(source, ExitPointElement) and transition.target == source.owner():
            return TransitionPath()
        if isinstance(source, EntryPointElement):
            boundary = source.owner()
            boundary_element = model.members.get(boundary)
            if not (
                isinstance(boundary_element, StateElement)
                and kind.Is(boundary_element.kind, SubmachineStateKind)
            ):
                boundary = posixpath.dirname(boundary)
            return TransitionPath(
                enter=self._finalize_enter_path(
                    model, posixpath.dirname(boundary), transition.target
                )
            )
        target = model.members.get(transition.target)
        if isinstance(target, EntryPointElement):
            boundary = target.owner()
            boundary_element = model.members.get(boundary)
            if not (
                isinstance(boundary_element, StateElement)
                and kind.Is(boundary_element.kind, SubmachineStateKind)
            ):
                boundary = posixpath.dirname(boundary)
            lca = (
                posixpath.dirname(transition.source)
                if kind.Is(transition.kind, SelfKind)
                else LCA(current, boundary)
            )
            if kind.Is(transition.kind, SelfKind) and isinstance(
                source, ExitPointElement
            ):
                lca = posixpath.dirname(boundary)
            if kind.Is(transition.kind, ExternalKind) and transition.source == boundary:
                lca = posixpath.dirname(boundary)
            return TransitionPath(
                enter=[transition.target],
                exit=self._finalize_exit_path(lca, current),
            )
        lca = (
            posixpath.dirname(transition.source)
            if kind.Is(transition.kind, SelfKind)
            else LCA(current, transition.target)
        )
        if (
            kind.Is(transition.kind, LocalKind)
            and isinstance(target, StateElement)
            and kind.Is(target.kind, SubmachineStateKind)
            and IsAncestor(target.qualified_name, current)
        ):
            lca = posixpath.dirname(target.qualified_name)
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
                        transitions = model.transition_map.get(qualified_name, {}).get(
                            deferred_event,
                            [],
                        )
                        if any(transition.guard is None for transition in transitions):
                            continue
                        _ = model.deferred_map[qualified_name].setdefault(
                            deferred_event, current
                        )
                if current == model.qualified_name:
                    break
                current = posixpath.dirname(current)

    def _finalize_transition_map(self, model: FinalizedModel) -> None:
        for qualified_name, element in model.members.items():
            if not isinstance(element, StateElement):
                continue
            model.transition_map[qualified_name] = {}
            shadowed_events: set[str] = set()
            current = qualified_name
            while current:
                current_vertex = model.members.get(current)
                if isinstance(current_vertex, VertexElement):
                    self._finalize_add_vertex_transitions(
                        model, qualified_name, current_vertex, shadowed_events
                    )
                if isinstance(current_vertex, StateElement):
                    shadowed_events.update(current_vertex.deferred)
                if current == model.qualified_name:
                    break
                current = posixpath.dirname(current)

    def _finalize_add_vertex_transitions(
        self,
        model: FinalizedModel,
        current: str,
        vertex: VertexElement,
        shadowed_events: set[str],
    ) -> None:
        transitions = [
            element
            for element in model.members.values()
            if isinstance(element, TransitionElement)
            and element.owner() == vertex.qualified_name
        ]
        transitions.sort(key=lambda transition: -transition.source.count("/"))
        for transition in transitions:
            if not transition.events:
                continue
            if current not in model.transition_paths.get(transition.qualified_name, {}):
                continue
            for event_name in transition.events:
                if event_name in shadowed_events:
                    continue
                model.transition_map[current].setdefault(event_name, []).append(
                    transition
                )


@dataclasses.dataclass(kw_only=True)
class RedefinableTransition(RedefinableElement[TransitionElement]):
    @dataclasses.dataclass(kw_only=True)
    class KindResolver(RedefinableElement[TransitionElement]):
        @typing.override
        def redefine(
            self,
            model: Model,
            stack: list[Element],
            element: TransitionElement | None = None,
        ) -> TransitionElement | None:
            del stack, element
            transition = get(model, self.qualified_name, TransitionElement)
            if transition is None:
                raise ErrorValidatingModel(
                    self.location,
                    f"transition '{self.qualified_name}' not found",
                )
            if transition.kind != TransitionKind:
                return transition
            target = model.members.get(transition.target)
            if isinstance(target, EntryPointElement):
                boundary = target.owner()
                boundary_element = model.members.get(boundary)
                if not (
                    isinstance(boundary_element, StateElement)
                    and kind.Is(boundary_element.kind, SubmachineStateKind)
                ):
                    boundary = posixpath.dirname(boundary)
                if transition.source == boundary:
                    transition.kind = ExternalKind
                    return transition
            if transition.target == transition.source:
                transition.kind = SelfKind
            elif transition.target == "":
                transition.kind = InternalKind
            elif IsAncestor(transition.source, transition.target):
                transition.kind = LocalKind
            else:
                transition.kind = ExternalKind
            return transition

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
        @dataclasses.dataclass(kw_only=True)
        class Activity(RedefinableElement[TransitionElement]):
            transition: str = dataclasses.field(default_factory=str)

            @typing.override
            def redefine(
                self,
                model: Model,
                stack: list[Element],
                element: TransitionElement | None = None,
            ) -> TransitionElement | None:
                del stack, element
                transition = get(model, self.transition, TransitionElement)
                event = model.events.get(self.qualified_name)
                if transition is None or not isinstance(event, Event):
                    raise ErrorValidatingModel(
                        self.location,
                        f"time event '{self.qualified_name}' not found",
                    )
                data = typing.cast(object, event.data)
                if event.kind != TimeEventKind or not callable(data):
                    raise ErrorValidatingModel(
                        self.location,
                        f"time event '{self.qualified_name}' is invalid",
                    )
                source = model.members.get(transition.source)
                if not isinstance(source, StateElement):
                    return transition
                activity_name = join(
                    source.qualified_name,
                    "activity",
                    event.name.removeprefix("/"),
                )
                if activity_name not in model.members:
                    model.members[activity_name] = ConcurrentBehaviorElement(
                        kind=ConcurrentKind,
                        qualified_name=activity_name,
                        operation=typing.cast(OperationExpression[typing.Any], data),
                    )
                if activity_name not in source.activity:
                    source.activity.append(activity_name)
                return transition

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
                self.qualified_name or f"time_{_model_member_count(model)}",
            )
            event = Event(name=event_name, kind=TimeEventKind)
            model.events[event_name] = Event(
                name=event.name,
                kind=event.kind,
                data=operation(event),
            )
            transition.events.append(event_name)
            model.owned_elements.append(
                RedefinableTransition.TimeEvent.Activity(
                    qualified_name=event_name,
                    transition=transition.qualified_name,
                    location=self.location,
                )
            )
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

    @dataclasses.dataclass(kw_only=True)
    class OnSetEvent(RedefinableElement[TransitionElement]):
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
                    "OnSet() must be called within a TransitionElement",
                )
            if self.qualified_name == "":
                raise ErrorValidatingModel(
                    self.location, "OnSet() requires a non-empty attribute name"
                )
            if "/" in self.qualified_name:
                raise ErrorValidatingModel(
                    self.location,
                    f'attribute name "{self.qualified_name}" cannot contain "/"',
                )
            qualified_name = join(model.qualified_name, self.qualified_name)
            transition.events.append(qualified_name)
            model.events[qualified_name] = Event(
                kind=ChangeEventKind,
                name=qualified_name,
                source=qualified_name,
            )
            if not isinstance(model.members.get(qualified_name), AttributeElement):
                model.members[qualified_name] = AttributeElement(
                    qualified_name=qualified_name,
                    declared_name=self.qualified_name,
                    implicit=True,
                    location=self.location,
                )
            return transition

    @dataclasses.dataclass(kw_only=True)
    class OnCallEvent(RedefinableElement[TransitionElement]):
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
                    "OnCall() must be called within a TransitionElement",
                )
            if self.qualified_name == "":
                raise ErrorValidatingModel(
                    self.location, "OnCall() requires a non-empty operation name"
                )
            if "/" in self.qualified_name:
                raise ErrorValidatingModel(
                    self.location,
                    f'operation name "{self.qualified_name}" cannot contain "/"',
                )
            qualified_name = join(model.qualified_name, self.qualified_name)
            transition.events.append(qualified_name)
            model.events[qualified_name] = Event(
                kind=CallEventKind,
                name=qualified_name,
                source=qualified_name,
            )
            return transition

    @dataclasses.dataclass(kw_only=True)
    class WhenAttribute(OnSetEvent):
        pass

    @dataclasses.dataclass(kw_only=True)
    class WhenPredicate(
        RedefinableElement[TransitionElement], typing.Generic[TInstance]
    ):
        expression: Expression[TInstance, typing.Any]

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
                    "When() must be called within a TransitionElement",
                )
            has_attribute_event = False
            for member in model.members.values():
                if (
                    isinstance(member, AttributeElement)
                    and member.owner() == model.qualified_name
                    and member.qualified_name not in transition.events
                ):
                    transition.events.append(member.qualified_name)
                    has_attribute_event = True
                    model.events[member.qualified_name] = Event(
                        kind=ChangeEventKind,
                        name=member.qualified_name,
                        source=member.qualified_name,
                    )
            if not has_attribute_event:
                transition.events.append(AnyEvent.name)
                model.events[AnyEvent.name] = AnyEvent
            guard = ConstraintElement(
                qualified_name=join(
                    transition.qualified_name,
                    self.qualified_name or ".when",
                ),
                expression=self.expression,
                location=self.location,
            )
            model.members[guard.qualified_name] = guard
            transition.guard = guard.qualified_name
            return transition

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
            self.qualified_name or f"transition_{_model_member_count(model)}",
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
        if transition.guard is not None and isinstance(
            source_element, ExitPointElement
        ):
            index = len(source_element.transitions)
            for current_index, transition_name in enumerate(source_element.transitions):
                existing = get(model, transition_name, TransitionElement)
                if (
                    existing is not None
                    and existing.guard is None
                    and existing.target != source_element.owner()
                ):
                    index = current_index
                    break
            source_element.transitions.insert(index, qualified_name)
        else:
            source_element.transitions.append(qualified_name)
        model.owned_elements.append(
            RedefinableTransition.KindResolver(
                qualified_name=transition.qualified_name,
                location=self.location,
            )
        )
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
        for index in range(len(stack) - 1, -1, -1):
            candidate = stack[index]
            if not isinstance(candidate, Model) or candidate.name() != model_name:
                continue
            for composed in stack[index + 1 :]:
                if isinstance(composed, StateElement):
                    return (
                        join(composed.qualified_name, relative_path)
                        if relative_path
                        else composed.qualified_name
                    )
        ancestor = find(stack, StateElement)
        current = ancestor.qualified_name if ancestor is not None else ""
        while current:
            if posixpath.basename(current) == model_name:
                return join(current, relative_path) if relative_path else current
            parent = posixpath.dirname(current)
            if parent == current:
                break
            current = parent
        for candidate in reversed(stack):
            if isinstance(candidate, StateElement) and kind.Is(
                candidate.kind, SubmachineStateKind
            ):
                return (
                    join(candidate.qualified_name, relative_path)
                    if relative_path
                    else candidate.qualified_name
                )
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
        del element
        validator: ModelValidator = DefaultModelValidator()
        finalizer: ModelFinalizer = DefaultModelFinalizer()
        model_elements: list[Element] = []
        for owned_element in self.owned_elements:
            if isinstance(owned_element, ValidatorElement):
                validator = owned_element.validator
            elif isinstance(owned_element, FinalizerElement):
                finalizer = owned_element.finalizer
            else:
                model_elements.append(owned_element)
        element = Model(
            qualified_name=self.qualified_name or model.qualified_name,
            owned_elements=model_elements,
            location=self.location,
        )
        owned_elements = self.owned_elements[:]
        result = RedefinableElement[Model](
            owned_elements=element.owned_elements,
        ).redefine(element, stack, element)
        element.owned_elements[:] = owned_elements
        if result is None:
            return None
        validator.validate(result)
        return finalizer.finalize(result)


@dataclasses.dataclass(kw_only=True)
class RedefinableBehaviors(RedefinableElement[BehaviorElement[TInstance]]):
    behaviors: list[
        str | OperationExpression[TInstance] | BehaviorElement[TInstance]
    ] = dataclasses.field(
        default_factory=list[
            str | OperationExpression[TInstance] | BehaviorElement[TInstance]
        ]
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
            if isinstance(behavior_or_method, str):
                if "/" in behavior_or_method:
                    raise ErrorValidatingModel(
                        owner.location,
                        f'operation name "{behavior_or_method}" cannot contain "/"',
                    )
                qualified_name = join(model.qualified_name, behavior_or_method)
                operation_element = _operation_element(model, qualified_name)
                if operation_element is None:
                    raise ErrorValidatingModel(
                        owner.location,
                        f'missing operation "{behavior_or_method}"',
                    )
                if kind == ConcurrentKind:

                    async def concurrent_operation(
                        ctx: context.Context,
                        instance: TInstance,
                        event: Event,
                        operation_element: OperationElement[
                            typing.Any
                        ] = operation_element,
                    ) -> None:
                        result = operation_element.operation(ctx, instance, event)
                        if isinstance(result, collections.abc.Awaitable):
                            await result

                    behavior = ConcurrentBehaviorElement(
                        kind=ConcurrentKind,
                        qualified_name=join(owner.qualified_name, behavior_or_method),
                        operation=concurrent_operation,
                        location=self.location,
                    )
                else:
                    if (
                        operation_element.method is not None
                        and inspect.iscoroutinefunction(operation_element.method)
                    ):

                        async def operation(
                            ctx: context.Context,
                            instance: TInstance,
                            event: Event,
                            operation_element: OperationElement[
                                typing.Any
                            ] = operation_element,
                        ) -> typing.Any:
                            result = operation_element.operation(ctx, instance, event)
                            if isinstance(result, collections.abc.Awaitable):
                                return typing.cast(typing.Any, await result)
                            return result

                    else:

                        def operation(
                            ctx: context.Context,
                            instance: TInstance,
                            event: Event,
                            operation_element: OperationElement[
                                typing.Any
                            ] = operation_element,
                        ) -> typing.Any:
                            return operation_element.operation(ctx, instance, event)

                    behavior = BehaviorElement(
                        kind=BehaviorKind,
                        qualified_name=join(owner.qualified_name, behavior_or_method),
                        operation=operation,
                        location=self.location,
                    )
            elif isinstance(behavior_or_method, BehaviorElement):
                behavior_element = typing.cast(
                    BehaviorElement[TInstance], behavior_or_method
                )
                if isinstance(behavior_element, ConcurrentBehaviorElement):
                    behavior = ConcurrentBehaviorElement(
                        kind=behavior_element.kind,
                        qualified_name=join(
                            owner.qualified_name, behavior_element.name()
                        ),
                        operation=behavior_element.operation,
                        location=self.location,
                    )
                else:
                    behavior = BehaviorElement(
                        kind=behavior_element.kind,
                        qualified_name=join(
                            owner.qualified_name, behavior_element.name()
                        ),
                        operation=behavior_element.operation,
                        location=self.location,
                    )
            else:
                name = getattr(
                    behavior_or_method,
                    "__name__",
                    f"behavior_{_model_member_count(model)}",
                )
                if kind == ConcurrentKind:
                    behavior = ConcurrentBehaviorElement(
                        kind=ConcurrentKind,
                        qualified_name=join(owner.qualified_name, name),
                        operation=behavior_or_method,
                        location=self.location,
                    )
                else:
                    behavior = BehaviorElement(
                        kind=BehaviorKind,
                        qualified_name=join(owner.qualified_name, name),
                        operation=behavior_or_method,
                        location=self.location,
                    )
            model.members[behavior.qualified_name] = behavior
            behaviors.append(behavior)
        return behaviors


@dataclasses.dataclass(kw_only=True)
class RedefinableConstraint(RedefinableElement[ConstraintElement[TInstance]]):
    expression: "str | Expression[TInstance, bool] | ConstraintElement[TInstance] | RedefinableConstraint[TInstance]"

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
        expression = typing.cast(object, self.expression)
        if isinstance(expression, str):
            if "/" in expression:
                raise ErrorValidatingModel(
                    transition.location,
                    f'operation name "{expression}" cannot contain "/"',
                )
            operation_qualified_name = join(model.qualified_name, expression)
            operation_element = _operation_element(model, operation_qualified_name)
            if operation_element is None:
                raise ErrorValidatingModel(
                    transition.location,
                    f'missing operation "{expression}"',
                )

            if operation_element.method is not None and inspect.iscoroutinefunction(
                operation_element.method
            ):

                async def operation_guard(
                    ctx: context.Context, instance: TInstance, event: Event
                ) -> bool:
                    result = operation_element.operation(ctx, instance, event)
                    if isinstance(result, collections.abc.Awaitable):
                        result = typing.cast(typing.Any, await result)
                    return bool(result)

            else:

                def operation_guard(
                    ctx: context.Context, instance: TInstance, event: Event
                ) -> bool:
                    result = operation_element.operation(ctx, instance, event)
                    return bool(result)

            constraint = ConstraintElement(
                qualified_name=qualified_name,
                expression=typing.cast(Expression[TInstance, bool], operation_guard),
            )
        elif isinstance(expression, ConstraintElement):
            constraint_element = typing.cast(ConstraintElement[TInstance], expression)
            constraint = ConstraintElement(
                qualified_name=join(
                    transition.qualified_name, constraint_element.name()
                ),
                expression=constraint_element.expression,
            )
        elif isinstance(expression, RedefinableConstraint):
            redefinable_constraint = typing.cast(
                RedefinableConstraint[TInstance], expression
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
                expression=typing.cast(Expression[TInstance, bool], expression),
            )
        previous_guard = transition.guard
        if previous_guard is not None:
            constraint_expression = constraint.expression

            def combined(
                ctx: context.Context, instance: TInstance, event: Event
            ) -> bool:
                previous = typing.cast(
                    ConstraintElement[TInstance], model.members[previous_guard]
                )
                return previous.expression(
                    ctx, instance, event
                ) and constraint_expression(ctx, instance, event)

            constraint.expression = combined
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
    deferred: list[Event] = dataclasses.field(default_factory=list[Event])

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
                if (
                    member.owner() == transition.target
                    or posixpath.dirname(member.owner()) == transition.target
                ):
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
            del stack, element
            transition = get(model, self.transition, TransitionElement)
            if transition is None:
                raise ErrorValidatingModel(
                    self.location,
                    f"transition '{self.transition}' not found",
                )
            source = get(model, transition.source, StateElement)
            if source is None or not kind.Is(source.kind, SubmachineStateKind):
                raise ErrorValidatingModel(
                    self.location,
                    "ExitPoint outcome can only be handled by a SubmachineState",
                )
            direct: list[ExitPointElement] = []
            nested: list[ExitPointElement] = []
            for member in model.members.values():
                if not isinstance(member, ExitPointElement):
                    continue
                if member.name() != self.qualified_name:
                    continue
                if not IsAncestor(source.qualified_name, member.qualified_name):
                    continue
                if (
                    member.owner() == source.qualified_name
                    or posixpath.dirname(member.owner()) == source.qualified_name
                ):
                    direct.append(member)
                else:
                    nested.append(member)
            exit_point = (direct or nested or [None])[0]
            if exit_point is None:
                raise ErrorValidatingModel(
                    self.location,
                    f"state '{transition.source}' has no exit point '{self.qualified_name}'",
                )
            if old_source := get(model, transition.source, VertexElement):
                old_source.transitions[:] = [
                    transition_name
                    for transition_name in old_source.transitions
                    if transition_name != transition.qualified_name
                ]
            transition.source = exit_point.qualified_name
            if transition.guard is not None:
                index = len(exit_point.transitions)
                for current_index, transition_name in enumerate(exit_point.transitions):
                    existing = get(model, transition_name, TransitionElement)
                    if (
                        existing is not None
                        and existing.guard is None
                        and existing.target != exit_point.owner()
                    ):
                        index = current_index
                        break
                exit_point.transitions.insert(index, transition.qualified_name)
            else:
                exit_point.transitions.append(transition.qualified_name)
            return transition

    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: ExitPointElement | None = None,
    ) -> ExitPointElement | None:
        transition = find(stack, TransitionElement)
        if transition is not None:
            model.owned_elements.append(
                RedefinableExitPoint.Resolver(
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
    events: list[Event] = dataclasses.field(default_factory=list[Event])

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
            self.qualified_name or f"choice_{_model_member_count(model)}",
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
                self.qualified_name or f"final_{_model_member_count(model)}",
            ),
            location=self.location,
        )
        model.members[final_state.qualified_name] = final_state
        return final_state


@dataclasses.dataclass(kw_only=True)
class RedefinableAttribute(RedefinableElement[AttributeElement]):
    declared_name: str = ""
    default: typing.Any = None
    value_type: type[typing.Any] | None = None
    dynamic: bool = False
    implicit: bool = False

    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: AttributeElement | None = None,
    ) -> AttributeElement | None:
        del element
        if self.qualified_name == "":
            raise ErrorValidatingModel(self.location, "attribute name cannot be empty")
        if "/" in self.qualified_name:
            raise ErrorValidatingModel(
                self.location,
                f'attribute name "{self.qualified_name}" cannot contain "/"',
            )
        namespace = find(stack, StateElement)
        if namespace is None:
            raise ErrorValidatingModel(
                self.location,
                "attribute must be called within Define()",
            )
        if not stack or namespace is not stack[-1]:
            raise ErrorValidatingModel(
                self.location,
                "attribute must be called within Define()",
            )
        if self.dynamic and not _attribute_accepts(self.value_type, self.default):
            expected = getattr(self.value_type, "__name__", str(self.value_type))
            actual = type(self.default).__name__
            raise ErrorValidatingModel(
                self.location,
                f'attribute "{self.qualified_name}" requires default of type {expected}, got {actual}',
            )
        qualified_name = join(model.qualified_name, self.qualified_name)
        existing = model.members.get(qualified_name)
        if existing is not None and not isinstance(existing, AttributeElement):
            raise ErrorValidatingModel(
                self.location,
                f"attribute '{qualified_name}' conflicts with existing model member",
            )
        attribute = AttributeElement(
            qualified_name=qualified_name,
            declared_name=self.qualified_name,
            default=self.default,
            value_type=self.value_type,
            dynamic=self.dynamic,
            implicit=self.implicit,
            location=self.location,
        )
        model.members[qualified_name] = attribute
        return attribute


@dataclasses.dataclass(kw_only=True)
class RedefinableOperation(RedefinableElement[OperationElement[TInstance]]):
    @typing.override
    def redefine(
        self,
        model: Model,
        stack: list[Element],
        element: OperationElement[TInstance] | None = None,
    ) -> OperationElement[TInstance] | None:
        del element
        if self.qualified_name == "":
            raise ErrorValidatingModel(self.location, "operation name cannot be empty")
        if "/" in self.qualified_name:
            raise ErrorValidatingModel(
                self.location,
                f'operation name "{self.qualified_name}" cannot contain "/"',
            )
        namespace = find(stack, StateElement)
        if namespace is None:
            raise ErrorValidatingModel(
                self.location,
                "operation must be called within Define() or State()",
            )
        qualified_name = join(model.qualified_name, self.qualified_name)
        existing = model.members.get(qualified_name)
        if existing is not None and not isinstance(existing, OperationElement):
            raise ErrorValidatingModel(
                self.location,
                f"operation '{qualified_name}' conflicts with existing model member",
            )
        method: OperationMethod | None = None
        if self.owned_elements:
            payload = self.owned_elements[0]
            if isinstance(payload, OperationElement):
                method = payload.method

        def operation_method(
            ctx: context.Context, instance: TInstance, event: Event
        ) -> typing.Any:
            data = event.data
            args = data.args if isinstance(data, CallData) else (event,)
            if method is not None:
                return method(ctx, instance, *args)
            instance_method = getattr(instance, self.qualified_name, None)
            if not callable(instance_method):
                raise ErrorValidatingModel(
                    self.location,
                    f'missing operation "{self.qualified_name}"',
                )
            return instance_method(*args)

        operation = OperationElement[TInstance](
            qualified_name=qualified_name,
            method=method,
            operation=typing.cast(OperationExpression[TInstance], operation_method),
            location=self.location,
        )
        model.members[qualified_name] = operation
        return operation


@typing.final
class Mutex:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._signal_lock = threading.Lock()
        self._signal: generic.Awaitable[None] | None = None

    def wait(self) -> generic.Awaitable[None]:
        with self._signal_lock:
            if not self._lock.locked():
                return _done()
            signal = self._signal
            if signal is None or signal.done():
                signal = generic.Awaitable[None]()
                self._signal = signal
            return signal

    def try_lock(self) -> bool:
        return self._lock.acquire(blocking=False)

    def locked(self) -> bool:
        return self._lock.locked()

    async def lock(self) -> None:
        while not self.try_lock():
            await self.wait()

    def release(self) -> None:
        with self._signal_lock:
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
    def __init__(self, fifo: "Fifo | MultiQueue | None" = None) -> None:
        self._lock = threading.Lock()
        self._lifo: collections.deque[Event] = collections.deque()
        self._fifo: Fifo | MultiQueue = fifo or Fifo()
        for method in ("push", "pop", "len"):
            if not callable(getattr(self._fifo, method, None)):
                raise TypeError(f"fifo must define callable {method}")

    @typing.overload
    def push(
        self, ctx_or_event: Event[typing.Any], event: None = None
    ) -> QueuePushResult: ...

    @typing.overload
    def push(
        self, ctx_or_event: context.Context, event: Event[typing.Any]
    ) -> BaseException | None: ...

    def push(
        self,
        ctx_or_event: context.Context | Event[typing.Any],
        event: Event[typing.Any] | None = None,
    ) -> BaseException | None | QueuePushResult:
        direct = event is None
        if event is None:
            event = typing.cast(Event[typing.Any], ctx_or_event)
        if kind.Is(event.kind, CompletionEventKind):
            with self._lock:
                self._lifo.appendleft(event)
            return (None,) if direct else None
        with self._lock:
            result = self._fifo.push(event)
        error = result[0]
        return (error,) if direct else error

    def pop(
        self, _: context.Context | None = None
    ) -> tuple[Event, bool, BaseException | None]:
        with self._lock:
            if self._lifo:
                return (self._lifo.popleft(), True, None)
            try:
                return self._fifo.pop()
            except BaseException as error:
                return (Event(), False, error)

    def len(self, _: context.Context | None = None) -> tuple[int, BaseException | None]:
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


Queue = MultiQueue


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


@typing.runtime_checkable
class Dispatchable(typing.Protocol):
    def context(self) -> context.Context: ...

    def dispatch(
        self, ctx: context.Context, event: Event
    ) -> collections.abc.Awaitable[None]: ...


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

    def start(
        self, ctx: context.Context, data: TData = None
    ) -> collections.abc.Awaitable[typing.Self]:
        if self.__hsm is None:
            return typing.cast(
                collections.abc.Awaitable[typing.Self],
                _error(
                    ErrorValidatingModel(
                        Location.capture(), "start requires an initialized instance"
                    )
                ),
            )
        return typing.cast(
            collections.abc.Awaitable[typing.Self],
            self.__hsm._start(ctx, data),  # pyright: ignore[reportPrivateUsage]
        )

    def get(self, name: str) -> tuple[typing.Any, bool]:
        if self.__hsm is None:
            return None, False
        return self.__hsm.get(name)

    def Get(self, name: str) -> tuple[typing.Any, bool]:
        return self.get(name)

    def set(self, name: str, value: typing.Any) -> collections.abc.Awaitable[None]:
        if self.__hsm is None:
            return _error(
                ErrorValidatingModel(
                    Location.capture(), "operation requires a started HSM"
                )
            )
        return self.__hsm.set(self.context(), name, value)

    def Set(self, name: str, value: typing.Any) -> collections.abc.Awaitable[None]:
        return self.set(name, value)

    def call(
        self, name: str, *args: typing.Any
    ) -> collections.abc.Awaitable[typing.Any]:
        if self.__hsm is None:
            return _error(
                ErrorValidatingModel(
                    Location.capture(), "operation requires a started HSM"
                )
            )
        return self.__hsm.call(self.context(), name, *args)

    def Call(
        self, name: str, *args: typing.Any
    ) -> collections.abc.Awaitable[typing.Any]:
        return self.call(name, *args)

    def stop(self, ctx: context.Context) -> collections.abc.Awaitable[None]:
        if self.__hsm is None:
            return _done()
        return self.__hsm.stop(ctx)

    def restart(
        self, ctx: context.Context, data: TData = None
    ) -> collections.abc.Awaitable[typing.Self | None]:
        if self.__hsm is None:
            return typing.cast(
                collections.abc.Awaitable[typing.Self | None],
                _error(
                    ErrorValidatingModel(
                        Location.capture(), "restart requires a started HSM"
                    )
                ),
            )
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
    _attributes: generic.Map[str, typing.Any]

    def __init__(
        self,
        instance: TInstance,
        model: Model,
        ctx: context.Context | None = None,
        config: Config | None = None,
    ):
        existing = getattr(instance, "_Instance__hsm", None)
        processing = getattr(existing, "_processing", None)
        if isinstance(existing, HSM) and (
            existing.state() != existing.model.qualified_name
            or (isinstance(processing, Mutex) and processing.locked())
        ):
            raise ErrorValidatingModel(
                Location.capture(), "instance already has a running HSM"
            )
        config = config or Config()

        def operation(ctx: context.Context, instance: TInstance, event: Event) -> None:
            del instance

            async def startup() -> None:
                await self._processing.lock()
                try:
                    state = await self._enter(
                        ctx, self.model, InitialEvent.WithData(event.data), False
                    )
                    if state is not None:
                        self._state = state
                    await self._process(ctx)
                except BaseException:
                    self._processing.release()
                    raise

            task = asyncio.Task(
                startup(),
                loop=asyncio.get_running_loop(),
                eager_start=True,
            )
            task.add_done_callback(
                lambda done: None if done.cancelled() else done.exception()
            )
            return None

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
            config.Queue
            if isinstance(config.Queue, MultiQueue)
            else MultiQueue(config.Queue)
        )
        # self._after = _AfterWaiters()
        self._state = model
        self._active = {}
        self._cancel = lambda: None
        self._attributes = generic.Map[str, typing.Any]()
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

    def get(
        self, name: str, ctx: context.Context | None = None
    ) -> tuple[typing.Any, bool]:
        del ctx
        qualified_name = (
            join(name)
            if posixpath.isabs(name)
            else join(self.model.qualified_name, name)
        )
        attribute = self.model.members.get(qualified_name)
        if not isinstance(attribute, AttributeElement):
            return None, False
        return self._attributes.load(attribute.qualified_name)

    def set(
        self,
        ctx: context.Context,
        name: str,
        value: typing.Any,
    ) -> collections.abc.Awaitable[None]:
        if self._state == self.model and not self._processing.locked():
            return _error(RuntimeError("set requires a started HSM"))
        qualified_name = (
            join(name)
            if posixpath.isabs(name)
            else join(self.model.qualified_name, name)
        )
        attribute = self.model.members.get(qualified_name)
        if not isinstance(attribute, AttributeElement):
            return _error(RuntimeError(f'missing attribute "{name}"'))
        if not _attribute_accepts(attribute.value_type, value):
            expected = getattr(
                attribute.value_type, "__name__", str(attribute.value_type)
            )
            actual = type(value).__name__
            return _error(
                RuntimeError(
                    f'attribute "{attribute.declared_name or attribute.name()}" requires value of type {expected}, got {actual}',
                )
            )
        qualified_name = attribute.qualified_name
        old_value, old_exists = self._attributes.swap(qualified_name, value)
        if old_exists and old_value == value:
            return _done()
        return self.dispatch(
            ctx,
            Event(
                name=qualified_name,
                kind=ChangeEventKind,
                source=qualified_name,
                data=AttributeChange(
                    name=qualified_name,
                    old_value=old_value,
                    value=value,
                ),
            ),
        )

    def call(
        self,
        ctx: context.Context,
        name: str,
        *args: typing.Any,
    ) -> collections.abc.Awaitable[typing.Any]:
        if self._state == self.model and ctx is not self._context:
            return _error(RuntimeError("operation requires a started HSM"))
        if name == "":
            return _error(RuntimeError("operation name cannot be empty"))
        qualified_name = (
            join(name)
            if posixpath.isabs(name)
            else join(self.model.qualified_name, name)
        )
        operation = _operation_element(self.model, qualified_name)
        if operation is None:
            return _error(RuntimeError(f'missing operation "{name}"'))
        event = Event(
            name=operation.qualified_name,
            kind=CallEventKind,
            source=operation.qualified_name,
            data=CallData(name=operation.qualified_name, args=args),
        )
        try:
            result = self._execute(ctx, operation, event)
        except BaseException as error:
            return _error(error)
        if isinstance(result, collections.abc.Awaitable):
            future = asyncio.ensure_future(
                typing.cast(collections.abc.Awaitable[typing.Any], result)
            )
        else:
            future = asyncio.get_running_loop().create_future()
            future.set_result(result)

        def dispatch_when_done(done: asyncio.Future[typing.Any]) -> None:
            if done.cancelled() or done.exception() is not None:
                return
            asyncio.ensure_future(self.dispatch(ctx, event)).add_done_callback(
                lambda done: None if done.cancelled() else done.exception()
            )

        future.add_done_callback(dispatch_when_done)
        return future

    async def _start(self, ctx: context.Context, data: TData = None) -> TInstance:
        if self._state != self.model or self._processing.locked():
            raise ErrorValidatingModel(Location.capture(), "already started HSM")
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
        self._attributes.clear()
        for member in self.model.members.values():
            if isinstance(member, AttributeElement) and member.dynamic:
                self._attributes.store(
                    member.qualified_name, copy.deepcopy(member.default)
                )
        _ = self.operation(self._context, self._instance, InitialEvent.WithData(data))
        await self._processing.wait()
        return self._instance

    async def _enter(
        self,
        ctx: context.Context,
        vertex: VertexElement,
        event: Event[TData],
        explicit_entry: bool,
    ) -> VertexElement | None:
        if isinstance(vertex, StateElement):
            state = vertex
            for behavior_name in state.entry:
                behavior = typing.cast(
                    BehaviorElement[TInstance], self.model.members[behavior_name]
                )
                self._execute(ctx, behavior, event)
            for behavior_name in state.activity:
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
            if explicit_entry or state.initial == "":
                return state
            initial = self.model.members[state.initial]
            if isinstance(initial, VertexElement) and initial.transitions:
                transition_name = initial.transitions[0]
                transition = typing.cast(
                    TransitionElement, self.model.members[transition_name]
                )
                return await self._transition(ctx, state, transition, event)
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
                return await self._transition(ctx, vertex, transition, event)
            return vertex
        elif isinstance(vertex, EntryPointElement):
            return await self._enter_entry_point(ctx, vertex, event)
        elif isinstance(vertex, ExitPointElement):
            return await self._enter_exit_point(ctx, vertex, event)
        elif isinstance(vertex, (ShallowHistoryElement, DeepHistoryElement)):
            owner = vertex.owner()
            remembered = self._history.get(vertex.qualified_name)
            if remembered is not None:
                current: VertexElement | None = None
                for entering in self.model.history_paths.get((owner, remembered), []):
                    entry_vertex = typing.cast(
                        VertexElement, self.model.members[entering]
                    )
                    current = await self._enter(
                        ctx, entry_vertex, event, entering != remembered
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
            return await self._transition(ctx, vertex, transition, event)
        return None

    async def _enter_entry_point(
        self,
        ctx: context.Context,
        vertex: EntryPointElement,
        event: Event[TData],
    ) -> VertexElement | None:
        if not vertex.transitions:
            return vertex
        transition = typing.cast(
            TransitionElement, self.model.members[vertex.transitions[0]]
        )
        return await self._transition(ctx, vertex, transition, event)

    async def _enter_exit_point(
        self,
        ctx: context.Context,
        vertex: ExitPointElement,
        event: Event[TData],
    ) -> VertexElement | None:
        for transition_name in vertex.transitions:
            transition = typing.cast(
                TransitionElement, self.model.members[transition_name]
            )
            if transition.target != vertex.owner():
                continue
            if transition.guard is not None:
                guard = typing.cast(
                    ConstraintElement[TInstance],
                    self.model.members[transition.guard],
                )
                if not self._evaluate(ctx, guard, event):
                    continue
            _ = await self._transition(ctx, vertex, transition, event)
        for transition_name in vertex.transitions:
            transition = typing.cast(
                TransitionElement, self.model.members[transition_name]
            )
            if transition.target == vertex.owner():
                continue
            if transition.guard is not None:
                guard = typing.cast(
                    ConstraintElement[TInstance],
                    self.model.members[transition.guard],
                )
                if not self._evaluate(ctx, guard, event):
                    continue
            return await self._transition(ctx, vertex, transition, event)
        raise RuntimeError(f'unhandled exit point "{vertex.name()}"')

    async def _exit(
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
                await self._terminate(ctx, behavior)
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
    ) -> collections.abc.Awaitable[typing.Any] | typing.Any:
        if isinstance(behavior, ConcurrentBehaviorElement):
            activity_ctx = context.Context(ctx)

            async def activity() -> None:
                try:
                    await typing.cast(
                        collections.abc.Coroutine[None, None, None],
                        behavior.operation(activity_ctx, self._instance, event),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    if not activity_ctx.is_done():
                        _ = self.dispatch(ctx, ErrorEvent.WithData(error))

            task = asyncio.Task(
                activity(),
                loop=asyncio.get_running_loop(),
                name=behavior.qualified_name,
                eager_start=True,
            )
            self._active[behavior.qualified_name] = ActiveBehavior(
                context=activity_ctx,
                task=task,
            )
            return task
        else:
            return behavior.operation(ctx, self._instance, event)

    async def _terminate(
        self, ctx: context.Context, behavior: BehaviorElement[TInstance]
    ) -> None:
        active = self._active.pop(behavior.qualified_name, None)
        if active is None:
            return
        active.context.cancel()
        _ = active.task.cancel()
        try:
            await active.task
        except asyncio.CancelledError:
            pass
        except Exception as error:
            if queue_error := self._queue.push(ctx, ErrorEvent.WithData(error)):
                raise queue_error
        return

    def _select_transition(
        self,
        ctx: context.Context,
        current_state: VertexElement,
        event: Event,
    ) -> TransitionElement | None:
        transition_map = self.model.transition_map.get(current_state.qualified_name)
        if not transition_map:
            return None
        transitions = transition_map.get(event.name)
        if transitions:
            for transition in transitions:
                if transition.guard is not None:
                    guard = typing.cast(
                        ConstraintElement[TInstance],
                        self.model.members[transition.guard],
                    )
                    if not self._evaluate(ctx, guard, event):
                        continue
                return transition
        if event.name == AnyEvent.name:
            return None
        transitions = transition_map.get(AnyEvent.name)
        if transitions:
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

    async def _process_event(
        self, ctx: context.Context, event: Event[TData]
    ) -> tuple[bool, str | None, str | None]:
        if not event.id:
            event = Event(
                name=event.name,
                data=event.data,
                kind=event.kind,
                id=muid.make(),
                source=event.source,
                target=event.target,
                schema=event.schema,
                metadata=event.metadata,
            )
        current_state = self._state
        current_qualified_name = current_state.qualified_name

        transition = self._select_transition(ctx, current_state, event)
        if transition is not None:
            state = await self._transition(ctx, current_state, transition, event)
            if state is not None:
                self._state = state
                return True, None, transition.source
        defer_owner = self.model.deferred_map.get(current_qualified_name, {}).get(
            event.name
        )
        if defer_owner is not None:
            return False, defer_owner, None
        return False, None, None

    async def _process(
        self,
        ctx: context.Context,
        current_event_id: str | None = None,
    ) -> None:
        deferred: list[tuple[str, Event]] = []
        while True:
            event, ok, error = self._queue.pop(ctx)
            if error is not None:
                event = ErrorEvent.WithData(error)
                ok = True
            if not ok:
                break
            current_qualified_name = self._state.qualified_name
            defer_owner = self.model.deferred_map.get(current_qualified_name, {}).get(
                event.name
            )
            if (
                current_event_id is not None
                and event.id != current_event_id
                and defer_owner is not None
            ):
                deferred.append((defer_owner, event))
                continue
            transitioned, defer_owner, transition_source = await self._process_event(
                ctx, event
            )
            if defer_owner is not None:
                deferred.append((defer_owner, event))
                continue
            if transitioned and deferred:
                active_state = self._state.qualified_name
                for defer_owner, deferred_event in deferred:
                    discard = False
                    current = posixpath.dirname(defer_owner)
                    while current not in ("", ".", "/"):
                        current_state = self.model.members.get(current)
                        if isinstance(current_state, StateElement) and kind.Is(
                            current_state.kind, SubmachineStateKind
                        ):
                            discard = (
                                active_state != current
                                and not IsAncestor(current, active_state)
                                and not (
                                    transition_source is not None
                                    and IsAncestor(current, transition_source)
                                )
                            )
                            break
                        if current == self.model.qualified_name:
                            break
                        current = posixpath.dirname(current)
                    if discard:
                        continue
                    if error := self._queue.push(ctx, deferred_event):
                        _ = await self._process_event(ctx, ErrorEvent.WithData(error))
                deferred = []
        for _, deferred_event in deferred:
            if error := self._queue.push(ctx, deferred_event):
                _ = await self._process_event(ctx, ErrorEvent.WithData(error))
        self._processing.release()
        return

    async def _transition(
        self,
        ctx: context.Context,
        current: VertexElement | None,
        transition: TransitionElement,
        event: Event[TData],
    ) -> VertexElement | None:
        if current is None:
            return None
        path = self.model.transition_paths[transition.qualified_name][
            current.qualified_name
        ]
        target = (
            None
            if transition.target == ""
            else typing.cast(VertexElement, self.model.members[transition.target])
        )
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
            exit_vertex = typing.cast(StateElement, self.model.members[exiting])
            if not await self._exit(ctx, exit_vertex, event):
                return None
        for effect in transition.effect:
            behavior = typing.cast(
                BehaviorElement[TInstance], self.model.members[effect]
            )
            self._execute(ctx, behavior, event)
        if kind.Is(transition.kind, InternalKind):
            return current
        for entering in path.enter:
            enter_vertex = typing.cast(VertexElement, self.model.members[entering])
            explicit_entry = entering != transition.target
            current = await self._enter(ctx, enter_vertex, event, explicit_entry)
            if not explicit_entry:
                return current
        return target

    async def _restart(
        self, ctx: context.Context, data: TData = None
    ) -> TInstance | None:
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
        if self._state == self.model and not self._processing.locked():
            return _error(RuntimeError("dispatch requires a started HSM"))
        if not event.id:
            event = Event(
                name=event.name,
                data=event.data,
                kind=event.kind,
                id=muid.make(),
                source=event.source,
                target=event.target,
                schema=event.schema,
                metadata=event.metadata,
            )
        if error := self._queue.push(ctx, event):
            return _error(error)
        if self._processing.try_lock():
            task = asyncio.Task(
                self._process(ctx, event.id),
                loop=asyncio.get_running_loop(),
                eager_start=True,
            )
            task.add_done_callback(
                lambda done: None if done.cancelled() else done.exception()
            )
            return asyncio.shield(task)
        return self._processing.wait()

    async def _stop(self, ctx: context.Context) -> None:
        async with self._processing:
            event = Event(name="stop", source=self._state.qualified_name)
            exiting = self._state.qualified_name
            while exiting not in (self.model.qualified_name, "", "."):
                vertex = get(self.model, exiting, StateElement)
                if vertex is not None:
                    _ = await self._exit(ctx, vertex, event)
                exiting = posixpath.dirname(exiting)
            self._queue.clear()
            self._cancel()
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
            queue_len = 0
            if queue_error := self._queue.push(
                self._context, ErrorEvent.WithData(error)
            ):
                raise queue_error
        transitions: dict[str, TransitionElement] = {}
        for transition_list in self.model.transition_map[
            self._state.qualified_name
        ].values():
            for transition in transition_list:
                transitions.setdefault(transition.qualified_name, transition)
        return Snapshot(
            ID=self.id,
            QualifiedName=self.qualified_name,
            State=self._state.qualified_name,
            Attributes=types.MappingProxyType(dict(self._attributes.items())),
            QueueLen=queue_len,
            Transitions=tuple(transitions.values()),
        )

    def stop(self, ctx: context.Context) -> collections.abc.Awaitable[None]:
        return asyncio.Task(
            self._stop(ctx),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )

    def restart(
        self, ctx: context.Context, data: TData = None
    ) -> collections.abc.Awaitable[TInstance | None]:
        if self._state == self.model and not self._processing.locked():
            return typing.cast(
                collections.abc.Awaitable[TInstance | None],
                _error(
                    ErrorValidatingModel(
                        Location.capture(), "restart requires a started HSM"
                    )
                ),
            )
        if ctx is self._context:
            values: dict[typing.Hashable, object] = {}
            instances = self._context.value(Keys.Instances)
            owner = self._context.value(Keys.Owner)
            if instances is not None:
                values[Keys.Instances] = instances
            if owner is not None:
                values[Keys.HSM] = owner
            ctx = context.Context(values=values)
        return asyncio.Task(
            self._restart(ctx, data),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )


class Group(BehaviorElement[Instance]):
    _id: str = ""
    _context: context.Context
    _instances: list[Instance]

    def __init__(
        self,
        *instances: str | Instance | "Group" | None,
        ctx: context.Context | None = None,
        id: str | None = None,
    ):
        values = list(instances)
        if id is None and values and isinstance(values[0], str):
            id = typing.cast(str, values.pop(0))
        group_id = id or muid.make()

        def operation(ctx: context.Context, instance: Instance, event: Event) -> None:
            del instance
            _ = self.dispatch(ctx, event)

        super().__init__(
            kind=BehaviorKind,
            id=group_id,
            qualified_name=group_id,
            operation=operation,
        )
        self._instances = []
        self._id = group_id
        for instance in values:
            if instance is None:
                continue
            if isinstance(instance, Group):
                self._instances.extend(instance._instances)
            elif isinstance(instance, Instance):
                self._instances.append(instance)
            else:
                raise TypeError(f"expected hsm.Instance, got {type(instance)!r}")
        if ctx is not None:
            self._context = ctx
        elif self._instances:
            self._context = self._instances[0].context()
        else:
            self._context = context.Context()

    def state(self) -> list[str]:
        if not self._instances:
            return []
        return [instance.take_snapshot().State for instance in self._instances]

    def context(self) -> context.Context:
        return self._context

    def dispatch(
        self,
        ctx: context.Context,
        event: Event[TData],
    ) -> collections.abc.Awaitable[None]:
        async def dispatch_all():
            completions: list[collections.abc.Awaitable[None]] = []
            for instance in self._instances:
                machine = getattr(instance, "_Instance__hsm", None)
                if (
                    not isinstance(machine, HSM)
                    or machine.state() == machine.model.qualified_name
                ):
                    continue
                completions.append(instance.dispatch(ctx, event))
            if not completions:
                return
            _ = await asyncio.gather(
                *(asyncio.shield(completion) for completion in completions)
            )

        return asyncio.ensure_future(dispatch_all())

    def stop(self, ctx: context.Context) -> collections.abc.Awaitable[None]:
        async def stop_all():
            _ = await asyncio.gather(
                *[instance.stop(ctx) for instance in self._instances]
            )

        return asyncio.ensure_future(stop_all())

    def restart(
        self, ctx: context.Context, data: TData = None
    ) -> collections.abc.Awaitable[None]:
        if ctx is self._context:
            values: dict[typing.Hashable, object] = {}
            instances = ctx.value(Keys.Instances)
            if instances is not None:
                values[Keys.Instances] = instances
            ctx = context.Context(values=values)

        async def restart_all():
            _ = await asyncio.gather(
                *[
                    instance.restart(ctx, None if data is None else copy.deepcopy(data))
                    for instance in self._instances
                ]
            )

        return asyncio.ensure_future(restart_all())

    def take_snapshot(self) -> list[Snapshot]:
        return [instance.take_snapshot() for instance in self._instances]


def Define(name: str, *elements: Element) -> Model:
    qualified_name = join("/", name)
    model = RedefinableModel(
        qualified_name=qualified_name, owned_elements=list(elements)
    ).redefine(Model(qualified_name=""), [])

    if model is None:
        raise ErrorValidatingModel(
            Location.capture(),
            "failed to define model",
        )

    return model


def Redefine(
    model: Model,
    name_or_element: str | Element | None = None,
    *elements: Element,
) -> Model:
    if isinstance(name_or_element, str):
        qualified_name = join("/", name_or_element)
        owned_elements = [*model.owned_elements, *elements]
        redefined = RedefinableModel(
            qualified_name=qualified_name,
            owned_elements=owned_elements,
            location=model.location,
        ).redefine(model, [])
    else:
        owned_elements = (
            elements if name_or_element is None else (name_or_element, *elements)
        )
        redefined = model.redefine(model, typing.cast(list[Element], owned_elements))
    if redefined is None:
        raise ErrorValidatingModel(
            Location.capture(),
            "failed to redefine model",
        )
    return redefined


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


def Attribute(
    name: str,
    *type_or_default: type[typing.Any] | typing.Any,
) -> RedefinableAttribute:
    if len(type_or_default) > 2:
        raise ErrorValidatingModel(
            Location.capture(), "Attribute() accepts at most three arguments"
        )
    value_type: type[typing.Any] | None = None
    default: typing.Any = None
    has_default = False
    if len(type_or_default) == 1:
        item = type_or_default[0]
        if isinstance(item, type):
            value_type = typing.cast(type[typing.Any], item)
        else:
            default = item
            has_default = True
            value_type = (
                None if item is None else typing.cast(type[typing.Any], type(item))
            )
    elif len(type_or_default) == 2:
        maybe_type, default = type_or_default
        if maybe_type is not None and not isinstance(maybe_type, type):
            raise ErrorValidatingModel(
                Location.capture(), "Attribute() type must be a type or None"
            )
        value_type = typing.cast(type[typing.Any] | None, maybe_type)
        has_default = True
    return RedefinableAttribute(
        qualified_name=name,
        declared_name=name,
        default=default,
        value_type=value_type,
        dynamic=has_default,
    )


def Operation(
    name: str,
    method: OperationMethod | None = None,
) -> RedefinableOperation[typing.Any]:
    payload: OperationElement[typing.Any] = OperationElement(
        qualified_name=name, method=method
    )
    return RedefinableOperation(qualified_name=name, owned_elements=[payload])


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
    *operations: str | OperationExpression[TInstance] | BehaviorElement[TInstance],
) -> RedefinableEntryBehavior[TInstance]:
    return RedefinableEntryBehavior(behaviors=list(operations))


def Exit(
    *operations: str | OperationExpression[TInstance] | BehaviorElement[TInstance],
) -> RedefinableExitBehavior[TInstance]:
    return RedefinableExitBehavior(behaviors=list(operations))


def Activity(
    *operations: str | OperationExpression[TInstance] | BehaviorElement[TInstance],
) -> RedefinableActivityBehavior[TInstance]:
    return RedefinableActivityBehavior(behaviors=list(operations))


def Effect(
    *operations: str | OperationExpression[TInstance] | BehaviorElement[TInstance],
) -> RedefinableEffectBehavior[TInstance]:
    return RedefinableEffectBehavior(behaviors=list(operations))


def Observe(
    *targets_or_operation: str | Event | Element | ObservationExpression[TInstance],
) -> ObservationElement[TInstance]:
    operation: ObservationExpression[TInstance] | None = None
    targets: list[str] = []
    for target_or_operation in targets_or_operation:
        if isinstance(target_or_operation, str):
            targets.append(target_or_operation)
        elif isinstance(target_or_operation, Event):
            targets.append(target_or_operation.name)
        elif isinstance(target_or_operation, Element):
            targets.append(target_or_operation.qualified_name)
        elif callable(target_or_operation):
            operation = target_or_operation
    return ObservationElement(
        qualified_name=getattr(operation, "__name__", ""),
        operation=operation or (lambda ctx, instance, event: None),
        targets=targets,
    )


def Guard(
    expression: str | Expression[TInstance, bool],
) -> RedefinableConstraint[TInstance]:
    return RedefinableConstraint(
        qualified_name=getattr(expression, "__name__", ".guard")
        if not isinstance(expression, str)
        else ".guard",
        expression=expression,
    )


def On(*events: str | Event) -> RedefinableTransitionWithEvents:
    return RedefinableTransitionWithEvents(
        events=[
            Event(name=event) if isinstance(event, str) else event for event in events
        ]
    )


def OnSet(name: str) -> RedefinableTransition.OnSetEvent:
    return RedefinableTransition.OnSetEvent(qualified_name=name)


def OnCall(name: str) -> RedefinableTransition.OnCallEvent:
    return RedefinableTransition.OnCallEvent(qualified_name=name)


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
    expression: str | Expression[TInstance, typing.Any],
) -> (
    RedefinableTransition.WhenAttribute | RedefinableTransition.WhenPredicate[TInstance]
):
    if isinstance(expression, str):
        return RedefinableTransition.WhenAttribute(qualified_name=expression)
    return RedefinableTransition.WhenPredicate(
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


def New(
    instance: TInstance,
    model: Model,
    config: Config | None = None,
) -> TInstance:
    _ = HSM(instance=instance, model=model, config=config)
    return instance


def Start(
    ctx: context.Context | None,
    instance: TInstance,
    data: TData = None,
) -> collections.abc.Awaitable[TInstance]:
    return instance.start(ctx or instance.context(), data)


def Started(
    ctx: context.Context | None,
    instance: TInstance,
    model: Model,
    config: Config | None = None,
) -> collections.abc.Awaitable[TInstance]:
    instance = New(instance, model, config)
    return Start(ctx, instance, config.Data if config is not None else None)


def Stop(
    sm: Instance | Group, ctx: context.Context | None = None
) -> collections.abc.Awaitable[None]:
    return sm.stop(ctx or sm.context())


@typing.overload
def Restart(
    sm: TInstance,
    data: TData = None,
    ctx: context.Context | None = None,
) -> collections.abc.Awaitable[TInstance | None]: ...


@typing.overload
def Restart(
    sm: Group,
    data: TData = None,
    ctx: context.Context | None = None,
) -> collections.abc.Awaitable[Group]: ...


def Restart(
    sm: TInstance | Group,
    data: TData = None,
    ctx: context.Context | None = None,
) -> collections.abc.Awaitable[TInstance | Group | None]:
    if isinstance(sm, Group):

        async def restart_group() -> Group:
            await sm.restart(ctx or sm.context(), data)
            return sm

        return asyncio.Task(
            restart_group(),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )
    return sm.restart(ctx or sm.context(), data)


@typing.overload
def TakeSnapshot(ctx: context.Context | None, sm: Group) -> list[Snapshot]: ...


@typing.overload
def TakeSnapshot(ctx: context.Context | None, sm: Instance) -> Snapshot: ...


def TakeSnapshot(
    ctx: context.Context | None,
    sm: Instance | Group,
) -> Snapshot | list[Snapshot]:
    del ctx
    if isinstance(sm, Group):
        return sm.take_snapshot()
    snapshot = sm.take_snapshot()
    if not snapshot.ID and not snapshot.QualifiedName and not snapshot.State:
        raise ErrorValidatingModel(
            Location.capture(), "take snapshot requires a started HSM"
        )
    return snapshot


def ID(sm: Instance | Group) -> str:
    if isinstance(sm, Group):
        return typing.cast(typing.Any, sm)._id
    return TakeSnapshot(None, sm).ID


def QualifiedName(sm: Instance | Group) -> str:
    if isinstance(sm, Group):
        return ""
    return TakeSnapshot(None, sm).QualifiedName


def Name(sm: Instance | Group) -> str:
    if isinstance(sm, Group):
        return ""
    return posixpath.basename(QualifiedName(sm))


def NewGroup(*instances: str | Instance | Group | None) -> Group:
    return Group(*instances)


MakeGroup = NewGroup


def Dispatch(
    ctx: context.Context | None,
    hsm: Dispatchable | None,
    event: Event,
) -> collections.abc.Awaitable[None]:
    if hsm is not None:
        return hsm.dispatch(ctx or hsm.context(), event)
    if ctx is not None:
        maybe_hsm = ctx.value(Keys.HSM)
        if isinstance(maybe_hsm, HSM):
            return maybe_hsm.dispatch(ctx, event)
    return _error(RuntimeError("dispatch requires a started HSM"))


def Get(
    ctx: context.Context | None,
    sm: Instance | None,
    name: str,
) -> tuple[typing.Any, bool]:
    if sm is not None:
        return sm.get(name)
    if ctx is not None:
        maybe_hsm = ctx.value(Keys.HSM)
        if isinstance(maybe_hsm, HSM):
            return maybe_hsm.get(name, ctx)
    return None, False


def Set(
    ctx: context.Context | None,
    sm: Instance | None,
    name: str,
    value: typing.Any,
) -> collections.abc.Awaitable[None]:
    if sm is not None:
        return sm.set(name, value)
    if ctx is not None:
        maybe_hsm = ctx.value(Keys.HSM)
        if isinstance(maybe_hsm, HSM):
            return maybe_hsm.set(ctx, name, value)
    return _error(
        ErrorValidatingModel(Location.capture(), "operation requires a started HSM")
    )


def Call(
    ctx: context.Context | None,
    sm: Instance | None,
    name: str,
    *args: typing.Any,
) -> collections.abc.Awaitable[typing.Any]:
    if sm is not None:
        return sm.call(name, *args)
    if ctx is not None:
        maybe_hsm = ctx.value(Keys.HSM)
        if isinstance(maybe_hsm, HSM):
            return maybe_hsm.call(ctx, name, *args)
    return _error(
        ErrorValidatingModel(Location.capture(), "operation requires a started HSM")
    )


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
                    metadata=event.metadata,
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
redefine = Redefine
state = State
submachine_state = SubmachineState
attribute = Attribute
operation = Operation
initial = Initial
transition = Transition
source = Source
target = Target
entry = Entry
exit = Exit
activity = Activity
effect = Effect
observe = Observe
guard = Guard
on = On
on_set = OnSet
on_call = OnCall
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
new = New
start = Start
started = Started
stop = Stop
restart = Restart
take_snapshot = TakeSnapshot
id = ID
qualified_name = QualifiedName
name = Name
new_group = NewGroup
make_group = MakeGroup
dispatch = Dispatch
call = Call
dispatch_all = DispatchAll
dispatch_to = DispatchTo
dispatchable = Dispatchable
group = Group
queue = Queue

__all__ = [
    "Activity",
    "After",
    "AnyEvent",
    "At",
    "Attribute",
    "AttributeChange",
    "AttributeElement",
    "AttributeKind",
    "BehaviorElement",
    "BehaviorKind",
    "Call",
    "CallData",
    "CallEventKind",
    "Choice",
    "ChoiceElement",
    "ChoiceKind",
    "Clock",
    "CompletionEvent",
    "CompletionEventKind",
    "ConcurrentBehaviorElement",
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
    "Dispatchable",
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
    "Fifo",
    "Final",
    "Finalizer",
    "FinalizerElement",
    "FinalEvent",
    "FinalizedModel",
    "FinalStateElement",
    "FinalStateKind",
    "Get",
    "Guard",
    "Group",
    "HSM",
    "ID",
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
    "MakeGroup",
    "Model",
    "ModelFinalizer",
    "ModelValidator",
    "MultiQueue",
    "Name",
    "NamespaceElement",
    "New",
    "NewGroup",
    "NamespaceKind",
    "NullKind",
    "On",
    "OnCall",
    "OnSet",
    "Operation",
    "OperationElement",
    "OperationKind",
    "ObservationElement",
    "ObservationKind",
    "Observe",
    "PseudostateElement",
    "PseudostateKind",
    "RedefinableElement",
    "Redefine",
    "QualifiedName",
    "Queue",
    "QueueLenResult",
    "QueuePopResult",
    "QueuePushResult",
    "Restart",
    "SelfKind",
    "SequentialKind",
    "Set",
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
    "TakeSnapshot",
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
    "attribute",
    "at",
    "choice",
    "deep_history",
    "define",
    "defer",
    "dispatch",
    "dispatch_all",
    "dispatch_to",
    "dispatchable",
    "effect",
    "entry",
    "entry_point",
    "every",
    "exit",
    "exit_point",
    "final",
    "finalizer",
    "group",
    "guard",
    "id",
    "initial",
    "make_group",
    "name",
    "new",
    "new_group",
    "observe",
    "on",
    "on_call",
    "on_set",
    "operation",
    "qualified_name",
    "queue",
    "redefine",
    "restart",
    "shallow_history",
    "source",
    "start",
    "started",
    "state",
    "stop",
    "submachine_state",
    "target",
    "take_snapshot",
    "transition",
    "validator",
    "when",
]
