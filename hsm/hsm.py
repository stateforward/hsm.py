from .kind import kind, is_kind
import enum
import typing
from dataclasses import dataclass, field
import os
from enum import IntEnum
import asyncio
import sys
import collections
from datetime import timedelta

# fnmatch removed - wildcard support not needed
import threading
import time

TElement = typing.TypeVar("TElement", bound="Element")
TInstance = typing.TypeVar("TInstance", bound="Instance")
TData = typing.TypeVar("TData", default=None)

def traceback() -> tuple[str, int]:
    frame = sys._getframe(3)  # type: ignore
    return frame.f_code.co_filename, frame.f_lineno


def join(path: str, *paths: str) -> str:
    return os.path.normpath(os.path.join(path, *paths))


def match(string: str, pattern: str) -> bool:
    """Exact string matching - wildcards not supported"""
    return string == pattern


class ValidationError(RuntimeError):
    pass


Operation = typing.Callable[["Context", TInstance, "Event"], typing.Awaitable[None]]
Expression = typing.Callable[["Context", TInstance, "Event"], typing.Awaitable[bool]]
Duration = typing.Callable[["Context", TInstance, "Event"], typing.Awaitable[timedelta]]


class Counter:
    def __init__(self, value: int = 0):
        self.value = value

    def next(self) -> int:
        value = self.value
        self.value += 1
        return value


id = Counter()


class Context:
    """Context for cancellation and lifecycle management, matching JavaScript/Go API"""

    def __init__(self):
        self._done = False
        self._listeners: list[typing.Callable[[], None]] = []
        self._done_future: asyncio.Future[None] | None = None

    @property
    def done(self) -> bool:
        """Check if the context is cancelled"""
        return self._done

    def is_done(self) -> bool:
        """Check if the context is cancelled (method form)"""
        return self._done

    def cancel(self) -> None:
        """Cancel the context and notify listeners"""
        if self._done:
            return
        self._done = True
        # Notify all listeners
        for listener in self._listeners:
            try:
                listener()
            except Exception:
                pass  # Ignore listener errors
        self._listeners.clear()
        # Set the done future if it exists
        if self._done_future and not self._done_future.done():
            self._done_future.set_result(None)

    def add_listener(self, event: str, callback: typing.Callable[[], None]) -> None:
        """Add a done event listener (only 'done' event supported)"""
        if event == "done":
            if self._done:
                # Already done, call immediately
                try:
                    callback()
                except Exception:
                    pass
            else:
                self._listeners.append(callback)

    def remove_listener(self, event: str, callback: typing.Callable[[], None]) -> None:
        """Remove a done event listener"""
        if event == "done" and callback in self._listeners:
            self._listeners.remove(callback)

    async def wait_done(self) -> None:
        """Wait for the context to be cancelled (async)"""
        if self._done:
            return
        if self._done_future is None:
            self._done_future = asyncio.Future()
        await self._done_future


class Kinds(IntEnum):
    Null = kind(id.next())
    Element = kind(id.next())
    Partial = kind(id.next())
    Namespace = kind(id.next())
    NamedElement = kind(id.next())
    Vertex = kind(id.next())
    State = kind(id.next(), Vertex, NamedElement)
    FinalState = kind(id.next(), State)
    Transition = kind(id.next(), NamedElement)
    Pseudostate = kind(id.next(), Vertex)
    Initial = kind(id.next(), Pseudostate)
    Choice = kind(id.next(), Pseudostate)
    External = kind(id.next())
    Self = kind(id.next())
    Internal = kind(id.next())
    Local = kind(id.next())
    Behavior = kind(id.next())
    StateMachine = kind(id.next())
    Concurrent = kind(id.next())
    Sequential = kind(id.next())
    Constraint = kind(id.next())
    Event = kind(id.next())
    CompletionEvent = kind(id.next(), Event)
    ErrorEvent = kind(id.next(), CompletionEvent)
    TimeEvent = kind(id.next(), Event)


@dataclass
class Element:
    kind: Kinds = Kinds.Element
    id: typing.Optional[str] = None
    owned_elements: list["NamedElement"] = field(default_factory=list)

    def owner(self) -> str:
        return ""


@dataclass
class Namespace(Element):
    kind: Kinds = Kinds.Namespace
    members: dict[str, typing.Union["Element", "Event"]] = field(default_factory=dict)


def find(
    stack: list["NamedElement"], *kinds: typing.Type[TElement]
) -> typing.Optional[TElement]:
    for element in reversed(stack):
        if isinstance(element, kinds):
            return element
    return None


@dataclass
class NamedElement(Element):
    kind: Kinds = Kinds.NamedElement
    qualified_name: str = field(default_factory=str)

    def owner(self) -> str:
        if self.qualified_name == "/":
            return super().owner()
        return os.path.dirname(self.qualified_name)

    def name(self) -> str:
        return os.path.basename(self.qualified_name)


def apply(
    element: "NamedElement",
    model: "Model",
    stack: list["NamedElement"],
    elements: list["NamedElement"],
) -> "NamedElement":
    stack = [*stack, element]
    for element in elements:
        if isinstance(element, PartialElement):
            element.apply(model, stack)
    return element


@dataclass
class PartialElement(NamedElement):
    kind: Kinds = Kinds.Partial
    traceback: tuple[str, int] = field(default_factory=traceback)

    def apply(
        self, model: "Model", stack: list["NamedElement"]
    ) -> typing.Optional["NamedElement"]:
        pass


async def noop_operation(ctx: Context, instance: Element, event: "Event") -> None:
    pass


@dataclass
class Behavior(typing.Generic[TInstance], NamedElement, Namespace):
    kind: Kinds = Kinds.Behavior
    operation: Operation[TInstance] = field(default=noop_operation)


@dataclass
class StateMachine(Behavior[TInstance]):
    kind: Kinds = Kinds.StateMachine


@dataclass
class Vertex(NamedElement):
    kind: Kinds = Kinds.Vertex
    transitions: list[str] = field(default_factory=list)


@dataclass
class State(Vertex, Namespace):
    kind: Kinds = Kinds.State
    initial: str = field(default_factory=str)
    entry: list[str] = field(default_factory=list)
    exit: list[str] = field(default_factory=list)
    activity: list[str] = field(default_factory=list)
    deferred: list[str] = field(default_factory=list)


@dataclass
class Model(State):
    transition_map: dict[str, dict[str, list["Transition"]]] = field(
        default_factory=dict
    )
    deferred_map: dict[str, dict[str, bool]] = field(default_factory=dict)

    def add(self, partial: PartialElement) -> None:
        self.owned_elements.append(partial)

    def get(
        self, name: str, *kinds: typing.Type[TElement]
    ) -> typing.Optional[TElement]:
        element = self.members.get(name)
        if element is None:
            return None
        bases = tuple(getattr(kind, "__origin__", kind) for kind in kinds)
        if not isinstance(element, bases):
            return None
        return typing.cast(TElement, element)

    def set(self, qualified_name: str, element: typing.Union[Element, "Event"]) -> None:
        self.members[qualified_name] = element


# Wildcard support removed - no longer needed
def transition_has_wildcard_event(transition: "Transition") -> bool:
    return False


@dataclass
class SortTransitions(PartialElement):
    vertex: "Vertex" = field(default_factory=Vertex)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        self.vertex.transitions.sort(
            key=lambda t: (transition := model.get(t, Transition)) is not None
            and transition_has_wildcard_event(transition)
        )


@dataclass
class PartialState(PartialElement):
    def apply(self, model: "Model", stack: list[NamedElement]) -> "State":
        namespace = find(stack, State)
        if namespace is None:
            raise RuntimeError(
                f"{self.traceback[0]}:{self.traceback[1]}: Namespace not found"
            )
        state = State(
            qualified_name=join(namespace.qualified_name, self.qualified_name)
        )
        model.set(state.qualified_name, state)
        apply(state, model, stack, self.owned_elements)
        model.add(
            SortTransitions(vertex=state, traceback=self.traceback),
        )
        return state


@dataclass
class FinalState(State):
    kind: Kinds = Kinds.FinalState


class PseudostateKind(enum.Enum):
    Initial = Kinds.Initial
    Choice = Kinds.Choice


@dataclass
class Pseudostate(Vertex):
    kind: Kinds = Kinds.Pseudostate


@dataclass
class Initial(Pseudostate):
    kind: Kinds = Kinds.Initial


@dataclass
class PartialInitial(PartialElement):
    def apply(self, model: "Model", stack: list["NamedElement"]) -> "Initial":
        if (state := find(stack, State)) is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: State {self.qualified_name} not found"
            )
        initial = Initial(
            qualified_name=join(state.qualified_name, self.qualified_name)
        )
        model.set(initial.qualified_name, initial)
        if state.initial != "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: State {state.qualified_name} already has an initial state {state.initial}"
            )
        state.initial = initial.qualified_name
        initial_transition = transition(
            source(initial.qualified_name), on(initial_event), *self.owned_elements
        ).apply(model, [*stack, initial])
        if initial_transition.guard is not None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Initial transition {initial_transition.qualified_name} cannot have a guard"
            )
        if initial_transition.events[0] != initial_event.name:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Initial transition {initial_transition.qualified_name} must have a trigger {initial_event.name}"
            )
        if not is_ancestor(state.qualified_name, initial_transition.target):
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Initial transition {initial_transition.qualified_name} must target a nested state not {initial_transition.target}"
            )
        if len(initial.transitions) > 1:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Initial {initial.qualified_name} cannot have multiple transitions {initial.transitions}"
            )
        return initial


def initial(
    name_or_element: typing.Union[str, NamedElement], *elements: NamedElement
) -> PartialInitial:
    name = ".initial"
    owned_elements: list[NamedElement] = list(elements)
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.insert(0, name_or_element)
    return PartialInitial(qualified_name=name, owned_elements=owned_elements)


@dataclass
class Choice(Pseudostate):
    kind: Kinds = Kinds.Choice


@dataclass
class Event(typing.Generic[TData]):
    name: str = field(default_factory=str)
    data: typing.Optional[TData] = field(default=None)
    kind: Kinds = field(default=Kinds.Event)
    qualified_name: str = field(default_factory=str)

    def __post_init__(self):
        if self.qualified_name == "":
            self.qualified_name = self.name


class CompletionEvent(Event):
    kind: Kinds = Kinds.CompletionEvent


initial_event = Event(name="hsm_initial")


@dataclass
class TransitionPaths:
    enter: list[str] = field(default_factory=list)
    exit: list[str] = field(default_factory=list)


@dataclass
class Transition(NamedElement):
    kind: Kinds = Kinds.Transition
    source: str = field(default_factory=str)
    target: str = field(default_factory=str)
    guard: typing.Optional[str] = field(default=None)
    effect: list[str] = field(default_factory=list)
    events: list[str] = field(default_factory=list)
    paths: dict[str, TransitionPaths] = field(default_factory=dict)


def least_common_ancestor(source: str, target: str) -> str:
    if source == target:
        return os.path.dirname(source)
    if source == "":
        return target
    if target == "":
        return source
    if os.path.dirname(source) == os.path.dirname(target):
        return os.path.dirname(source)
    if is_ancestor(source, target):
        return source
    if is_ancestor(target, source):
        return target
    return least_common_ancestor(os.path.dirname(source), os.path.dirname(target))


def is_ancestor(source: str, target: str) -> bool:
    """Check if target is a descendant of source (i.e., source is an ancestor of target)"""
    source_norm = os.path.normpath(source)
    target_norm = os.path.normpath(target)
    if source_norm == target_norm or source_norm == "." or target_norm == ".":
        return False
    if source_norm == "/":
        return True
    parent = os.path.dirname(target_norm)
    while parent != "/":
        if parent == source_norm:
            return True
        parent = os.path.dirname(parent)
    return False


@dataclass
class ResolvePaths(PartialElement):
    transition: "Transition" = field(default_factory=Transition)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        enter: list[str] = []
        entering = self.transition.target
        lca = least_common_ancestor(self.transition.source, self.transition.target)
        while entering != lca and entering != "/" and entering != "":
            enter.insert(0, entering)
            entering = os.path.dirname(entering)
        if is_kind(self.transition.kind, Kinds.Self):
            enter.append(self.transition.source)
        source_element = model.get(self.transition.source, Vertex)
        if isinstance(source_element, Initial):
            self.transition.paths[os.path.dirname(self.transition.source)] = (
                TransitionPaths(enter=enter, exit=[])
            )
            return
        if self.transition.source == "/" and self.transition.target != "":
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Top level transitions must have a source and target, or no source and target"
            )
        if (
            is_kind(self.transition.kind, Kinds.Internal)
            and len(self.transition.effect) == 0
        ):
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Internal transitions require an effect"
            )
        for qualified_name, element in model.members.items():
            if qualified_name.startswith(self.transition.source) and isinstance(
                element, (StateMachine, Vertex)
            ):
                exit: list[str] = []
                if self.transition.kind != Kinds.Internal:
                    exiting = element.qualified_name
                    while exiting != lca and exiting != "":
                        exit.append(exiting)
                        if exiting == "/":
                            break
                        exiting = os.path.dirname(exiting)
                self.transition.paths[element.qualified_name] = TransitionPaths(
                    enter=enter, exit=exit
                )


@dataclass
class PartialTransition(PartialElement):
    def apply(self, model: "Model", stack: list["NamedElement"]) -> Transition:
        vertex = find(stack, Vertex)
        if vertex is None:
            raise SyntaxError(
                f"{self.traceback[0]}:{self.traceback[1]}: Vertex not found"
            )
        name = self.qualified_name
        if name == "":
            name = f"transition_{len(model.members)}"
        transition = Transition(
            qualified_name=join(vertex.qualified_name, name), source="."
        )
        model.set(transition.qualified_name, transition)
        apply(transition, model, stack, self.owned_elements)
        if transition.source == "." or transition.source == "":
            transition.source = vertex.qualified_name
        source_element = model.get(transition.source, Vertex)
        if source_element is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Source "{transition.source}" not found for transition "{transition.qualified_name}"'
            )
        source_element.transitions.append(transition.qualified_name)
        if len(transition.events) == 0 and not isinstance(source_element, Pseudostate):
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Transition "{transition.qualified_name}" has no events'
            )
        if transition.target == transition.source:
            transition.kind = Kinds.Self
        elif transition.target == "":
            transition.kind = Kinds.Internal
        elif is_ancestor(transition.source, transition.target):
            transition.kind = Kinds.Local
        else:
            transition.kind = Kinds.External
        # resolve the paths after the model is built
        model.add(ResolvePaths(transition=transition))
        return transition


class ValidateVertex(PartialElement):
    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        if model.get(self.qualified_name, Vertex) is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Vertex "{self.qualified_name}" not found'
            )


class PartialSource(PartialElement):
    def apply(self, model: "Model", stack: list["NamedElement"]) -> Transition:
        transition = find(stack, Transition)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Transition not found"
            )
        if transition.source != "." and transition.source != "":
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Transition "{transition.qualified_name}" already has a source "{transition.source}"'
            )
        if len(self.owned_elements) > 0:
            source = self.owned_elements[0]
            if isinstance(source, PartialElement):
                if (source := source.apply(model, stack)) is None:
                    raise ValidationError(
                        f'{self.traceback[0]}:{self.traceback[1]}: Source "{self.qualified_name}" not found for transition "{transition.qualified_name}"'
                    )
            self.qualified_name = source.qualified_name
        elif not os.path.isabs(self.qualified_name):
            if state := find(stack, State):
                # Only join if the qualified_name is truly relative (doesn't already contain the state name)
                if not self.qualified_name.startswith(state.qualified_name):
                    self.qualified_name = join(
                        state.qualified_name, self.qualified_name
                    )
            # validate the source after the model is built
            model.add(
                ValidateVertex(
                    qualified_name=self.qualified_name, traceback=self.traceback
                ),
            )
        elif not is_path_in_path(self.qualified_name, model.qualified_name):
            self.qualified_name = join(model.qualified_name, self.qualified_name[1:])
        transition.source = self.qualified_name
        return transition


class PartialTarget(PartialElement):
    def apply(self, model: "Model", stack: list["NamedElement"]) -> Transition:
        transition = find(stack, Transition)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Transition not found"
            )
        if transition.target != "":
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Transition "{transition.qualified_name}" already has a target "{transition.target}"'
            )
        if len(self.owned_elements) > 0:
            target = self.owned_elements[0]
            if isinstance(target, PartialElement):
                if (target := target.apply(model, stack)) is None:
                    raise ValidationError(
                        f'{self.traceback[0]}:{self.traceback[1]}: Target "{self.qualified_name}" not found for transition "{transition.qualified_name}"'
                    )
            self.qualified_name = target.qualified_name
        elif not os.path.isabs(self.qualified_name):
            if state := find(stack, State):
                # Only join if the qualified_name is truly relative (doesn't already contain the state name)
                if not self.qualified_name.startswith(state.qualified_name):
                    self.qualified_name = join(
                        state.qualified_name, self.qualified_name
                    )
            # validate the target after the model is built
            model.add(
                ValidateVertex(
                    qualified_name=self.qualified_name, traceback=self.traceback
                ),
            )
        elif not is_ancestor(self.qualified_name, model.qualified_name):
            self.qualified_name = join(model.qualified_name, self.qualified_name[1:])
        transition.target = self.qualified_name
        return transition


@dataclass
class PartialBehaviors(typing.Generic[TInstance], PartialElement):
    operations: list[Operation[TInstance]] = field(default_factory=list)
    type: typing.Type[NamedElement] = field(default=NamedElement)
    concurrent: bool = field(default=False)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> NamedElement:
        element = find(stack, self.type)
        if element is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: {self.type.__name__} not found"
            )
        behaviors = getattr(element, self.qualified_name, None)
        if behaviors is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: {self.type.__name__} {element.qualified_name} has no {self.qualified_name}"
            )
        for operation in self.operations:
            behavior = Behavior(
                qualified_name=join(
                    element.qualified_name,
                    self.qualified_name,
                    operation.__name__,
                    str(len(behaviors)),
                ),
                operation=operation,
                kind=Kinds.Concurrent if self.concurrent else Kinds.Sequential,
            )
            behaviors.append(behavior.qualified_name)
            model.set(behavior.qualified_name, behavior)
        return element


async def noop_expression(ctx: Context, instance: "Instance", event: Event) -> bool:
    return True


@dataclass
class Guard(typing.Generic[TInstance], NamedElement):
    kind: Kinds = Kinds.Constraint
    expression: Expression[TInstance] = field(default=noop_expression)


@dataclass
class PartialGuard(typing.Generic[TInstance], PartialElement):
    expression: Expression[TInstance] = field(default=noop_expression)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        transition = find(stack, Transition)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Transition not found"
            )
        guard = Guard(
            qualified_name=join(transition.qualified_name, self.qualified_name),
            expression=self.expression,
        )
        model.set(guard.qualified_name, guard)
        transition.guard = guard.qualified_name


def define(name: str, *elements: NamedElement) -> Model:
    # Match JavaScript behavior: use absolute paths with / prefix
    qualified_name = join("/", name)
    model = Model(qualified_name=qualified_name)
    model.members[qualified_name] = model
    stack: list[NamedElement] = []
    apply(model, model, stack, list(elements))
    while len(model.owned_elements) > 0:
        partial = model.owned_elements.pop()
        if isinstance(partial, PartialElement):
            partial.apply(model, stack)

    # Build the optimized transition and deferred lookup tables
    build_transition_table(model)
    build_deferred_table(model)

    return model


def state(name: str, *elements: NamedElement) -> PartialState:
    return PartialState(qualified_name=name, owned_elements=list(elements))


def entry(*operations: Operation[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors[TInstance](
        operations=list(operations), type=State, qualified_name="entry"
    )


def exit(*operations: Operation[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors[TInstance](
        operations=list(operations), type=State, qualified_name="exit"
    )


def activity(*operations: Operation[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors[TInstance](
        operations=list(operations),
        type=State,
        concurrent=True,
        qualified_name="activity",
    )


def transition(
    name_or_element: typing.Union[str, PartialElement], *elements: NamedElement
) -> PartialTransition:
    name = ""
    owned_elements: list[NamedElement] = list(elements)
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.insert(0, name_or_element)
    return PartialTransition(qualified_name=name, owned_elements=owned_elements)


def is_path_in_path(child: str, parent: str) -> bool:
    parent = os.path.abspath(parent)
    child = os.path.abspath(child)
    return os.path.commonpath([parent]) == os.path.commonpath([parent, child])


def source(name_or_element: typing.Union[str, NamedElement]) -> PartialSource:
    name = ""
    owned_elements: list[NamedElement] = []
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.append(name_or_element)
    return PartialSource(qualified_name=name, owned_elements=owned_elements)


async def noop_duration(ctx: Context, instance: "Instance", event: Event) -> timedelta:
    return timedelta(seconds=0)


@dataclass
class AfterBehavior(typing.Generic[TInstance], PartialElement):
    event: Event = field(default_factory=Event)
    duration: Duration[TInstance] = field(default=noop_duration)
    transition: Transition = field(default_factory=Transition)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        source = model.get(self.transition.source, State)
        if source is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Source "{self.transition.source}" not found for transition "{self.transition.qualified_name}"'
            )
        duration = self.duration

        async def operation(ctx: Context, instance: TInstance, event: Event) -> None:
            try:
                delta = await duration(ctx, instance, event)
                if delta.total_seconds() > 0:
                    await asyncio.sleep(delta.total_seconds())
                    if not ctx.is_done():
                        instance.dispatch(self.event)
            except asyncio.CancelledError:
                pass

        activity = Behavior(
            qualified_name=join(
                source.qualified_name, self.event.name, str(len(model.members))
            ),
            kind=Kinds.Concurrent,
            operation=operation,
        )
        source.activity.append(activity.qualified_name)
        model.members[activity.qualified_name] = activity


@dataclass
class PartialAfter(typing.Generic[TInstance], PartialElement):
    duration: Duration[TInstance] = field(default=noop_duration)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        transition = find(stack, Transition)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Transition not found"
            )
        qualified_name = join(
            transition.qualified_name, self.duration.__name__, str(len(model.members))
        )
        event = Event(
            name=qualified_name, qualified_name=qualified_name, kind=Kinds.TimeEvent
        )
        transition.events.append(event.qualified_name)
        model.set(event.qualified_name, event)
        model.add(
            AfterBehavior(event=event, transition=transition, duration=self.duration),
        )


def after(duration: Duration[TInstance]) -> PartialAfter[TInstance]:
    return PartialAfter[TInstance](duration=duration)


@dataclass
class EveryBehavior(typing.Generic[TInstance], PartialElement):
    event: Event = field(default_factory=Event)
    duration: Duration[TInstance] = field(default=noop_duration)
    transition: Transition = field(default_factory=Transition)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        source = model.get(self.transition.source, State)
        if source is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Source "{self.transition.source}" not found for transition "{self.transition.qualified_name}"'
            )
        duration = self.duration

        async def operation(ctx: Context, instance: TInstance, event: Event) -> None:
            interval = await duration(ctx, instance, event)
            while not ctx.is_done():
                try:
                    await asyncio.sleep(interval.total_seconds())
                    if not ctx.is_done():
                        instance.dispatch(self.event)
                except asyncio.CancelledError:
                    break

        activity = Behavior(
            qualified_name=join(
                source.qualified_name, self.event.name, str(len(model.members))
            ),
            kind=Kinds.Concurrent,
            operation=operation,
        )
        source.activity.append(activity.qualified_name)
        model.members[activity.qualified_name] = activity


@dataclass
class PartialEvery(typing.Generic[TInstance], PartialElement):
    duration: Duration[TInstance] = field(default=noop_duration)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        transition = find(stack, Transition)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Transition not found"
            )
        qualified_name = join(
            transition.qualified_name,
            self.duration.__name__,
            str(len(model.members)),
        )
        event = Event(
            name=qualified_name,
            qualified_name=qualified_name,
            kind=Kinds.TimeEvent,
        )
        transition.events.append(event.qualified_name)
        model.members[event.qualified_name] = event
        model.add(
            EveryBehavior(event=event, transition=transition, duration=self.duration),
        )


def every(duration: Duration[TInstance]) -> PartialEvery[TInstance]:
    return PartialEvery[TInstance](duration=duration)


def target(name_or_element: typing.Union[str, NamedElement]) -> PartialTarget:
    name = ""
    owned_elements: list[NamedElement] = []
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.insert(0, name_or_element)
    return PartialTarget(qualified_name=name, owned_elements=owned_elements)


def effect(*operations: Operation[TInstance]) -> PartialBehaviors[TInstance]:
    return PartialBehaviors[TInstance](
        operations=list(operations), type=Transition, qualified_name="effect"
    )


def guard(expression: Expression[TInstance]) -> PartialGuard[TInstance]:
    return PartialGuard[TInstance](
        qualified_name=expression.__name__, expression=expression
    )


@dataclass
class PartialTrigger(PartialElement):
    events: list[Event] = field(default_factory=list)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> Transition:
        transition = find(stack, Transition)
        if transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Transition not found"
            )
        transition.events.extend([event.qualified_name for event in self.events])
        return transition


def on(*events: typing.Union[str, Event]) -> PartialTrigger:
    return PartialTrigger(
        events=[
            Event(name=event) if isinstance(event, str) else event for event in events
        ]
    )


@dataclass
class PartialDefer(PartialElement):
    events: list[Event] = field(default_factory=list)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        state = find(stack, State)
        if state is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: defer must be called within a state"
            )
        state.deferred.extend([event.qualified_name for event in self.events])


def defer(*events: Event) -> PartialDefer:
    return PartialDefer(events=list(events))


@dataclass
class PartialChoice(PartialElement):
    def apply(self, model: "Model", stack: list["NamedElement"]) -> Choice:
        state_or_transition = find(stack, State, Transition)
        if state_or_transition is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: choice must be called within a state or transition"
            )
        if isinstance(state_or_transition, Transition):
            source = state_or_transition.source
            if source == "." or source == "":
                maybe_source = find(stack, Vertex)
                if maybe_source is None:
                    raise ValidationError(
                        f"{self.traceback[0]}:{self.traceback[1]}: choice must be called within a state or transition"
                    )
                source = maybe_source.qualified_name
            maybe_state = model.get(source, State, Pseudostate)
            if maybe_state is None:
                raise ValidationError(
                    f'{self.traceback[0]}:{self.traceback[1]}: source "{source}" not found for transition "{state_or_transition.qualified_name}"'
                )
            elif isinstance(maybe_state, Pseudostate):
                state_or_transition = find(stack, State)
                if state_or_transition is None:
                    raise ValidationError(
                        f"{self.traceback[0]}:{self.traceback[1]}: choice must be called within a state"
                    )
        if self.qualified_name == "":
            self.qualified_name = f"choice_{len(model.members)}"
        choice = Choice(
            qualified_name=join(state_or_transition.qualified_name, self.qualified_name)
        )
        model.members[choice.qualified_name] = choice
        apply(choice, model, stack, self.owned_elements)
        if len(choice.transitions) == 0:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: choice "{choice.qualified_name}" has no transitions'
            )
        if default_transition := model.get(
            choice.transitions[len(choice.transitions) - 1], Transition
        ):
            if default_transition.guard is not None:
                raise ValidationError(
                    f'{self.traceback[0]}:{self.traceback[1]}: the last transition of choice state "{choice.qualified_name}" cannot have a guard'
                )
        return choice


def choice(
    element_or_name: typing.Union[str, PartialTransition],
    *transitions: PartialTransition,
) -> PartialChoice:
    name = ""
    owned_elements: list[NamedElement] = list(transitions)
    if isinstance(element_or_name, str):
        name = element_or_name
    else:
        owned_elements.insert(0, element_or_name)
    return PartialChoice(qualified_name=name, owned_elements=owned_elements)


class ValidateFinalState(PartialElement):
    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        final_state = model.get(self.qualified_name, FinalState)
        if final_state is None:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" not found'
            )
        if len(final_state.transitions) > 0:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" cannot have transitions'
            )
        if len(final_state.entry) > 0:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" cannot have an entry action'
            )
        if len(final_state.exit) > 0:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" cannot have an exit action'
            )
        if len(final_state.activity) > 0:
            raise ValidationError(
                f'{self.traceback[0]}:{self.traceback[1]}: Final state "{self.qualified_name}" cannot have an activity'
            )


@dataclass
class PartialFinal(PartialElement):
    target: str = field(default_factory=str)

    def apply(self, model: "Model", stack: list["NamedElement"]) -> None:
        namespace = find(stack, State, StateMachine[typing.Any])
        if namespace is None:
            raise ValidationError(
                f"{self.traceback[0]}:{self.traceback[1]}: Final must be called within a namespace"
            )
        final_state = FinalState(
            qualified_name=join(namespace.qualified_name, self.qualified_name)
        )
        model.members[final_state.qualified_name] = final_state
        model.add(
            ValidateFinalState(qualified_name=final_state.qualified_name),
        )


def final(name_or_element: typing.Union[str, NamedElement]) -> PartialFinal:
    name = ""
    owned_elements: list[NamedElement] = []
    if isinstance(name_or_element, str):
        name = name_or_element
    else:
        owned_elements.append(name_or_element)
    return PartialFinal(qualified_name=name, owned_elements=owned_elements)


def done_future() -> asyncio.Future[None]:
    future = asyncio.Future[None]()
    future.set_result(None)
    return future


class Mutex:
    __lock: threading.Lock

    def __init__(self):
        self.__lock = threading.Lock()

    def try_acquire(self) -> bool:
        if self.__lock.locked():
            return False
        self.__lock.acquire()
        return True

    def acquire(self) -> None:
        self.__lock.acquire()

    def release(self) -> None:
        self.__lock.release()

    def __enter__(self) -> None:
        self.acquire()

    def __exit__(self, *_: typing.Any) -> None:
        self.release()


class Instance(Element):
    __hsm: typing.Optional["HSM[typing.Self]"] = None

    def dispatch(self, event: Event) -> typing.Awaitable[None]:
        if self.__hsm is None:
            return done_future()
        return self.__hsm.dispatch(event)

    def state(self) -> str:
        if self.__hsm is None:
            return ""
        return self.__hsm.state()

    async def stop(self) -> None:
        """Stop the state machine gracefully"""
        if self.__hsm is not None:
            stop_method = getattr(self.__hsm, "_HSM__stop", None)
            if stop_method is not None:
                await stop_method()


class Queue:
    """Event queue implementation matching JavaScript behavior:
    - Completion events: LIFO (stack) - newer events processed first
    - Regular events: FIFO (queue) - older events processed first
    - Completion events always have priority over regular events
    """

    def __init__(self):
        self.lock = threading.Lock()
        # Separate collections for different event types
        self.completion_events = collections.deque[
            Event
        ]()  # LIFO stack for completion events
        self.regular_events = collections.deque[
            Event
        ]()  # FIFO queue for regular events

    def push(self, event: Event) -> None:
        with self.lock:
            if isinstance(event, CompletionEvent):
                # Completion events: LIFO (newest first)
                self.completion_events.appendleft(event)
            else:
                # Regular events: FIFO (oldest first)
                self.regular_events.append(event)

    async def pop(self) -> typing.Optional[Event]:
        with self.lock:
            # Always process completion events first (they have priority)
            if len(self.completion_events) > 0:
                return self.completion_events.popleft()  # LIFO for completion events
            elif len(self.regular_events) > 0:
                return self.regular_events.popleft()  # FIFO for regular events
            return None


@dataclass
class Activity:
    context: Context
    task: asyncio.Task[None]


class HSM(Behavior[TInstance]):
    __instance: TInstance
    __state: Vertex
    __processing: Mutex
    __queue: Queue
    __active: dict[str, Activity]
    __lock: asyncio.Lock
    __context: Context
    __awaitable: typing.Awaitable[None]

    def state(self) -> str:
        return self.__state.qualified_name

    def __init__(
        self, instance: TInstance, model: Model, ctx: typing.Optional[Context] = None
    ):
        super().__init__(kind=Kinds.StateMachine, qualified_name=model.qualified_name)
        self.model = model
        self.__instance = instance
        self.__processing = Mutex()
        self.__queue = Queue()
        self.__active = {}
        self.__lock = asyncio.Lock()
        self.__context = ctx or Context()  # Main context for the HSM
        self.__awaitable = done_future()

        async def operation(ctx: Context, inst: TInstance, event: Event) -> None:
            self.__state = await self.__enter(self.model, event, True)
            await self.__process()

        setattr(self.__instance, "_Instance__hsm", self)
        self.operation = operation

    async def __start(self) -> None:  # type: ignore
        self.__processing.acquire()
        await self.__execute(self, initial_event)

    async def __enter(
        self, vertex: Vertex, event: Event, default_entry: bool
    ) -> Vertex:
        if isinstance(vertex, State):
            for entry in [
                entry
                for entry in [
                    self.model.get(entry, Behavior[TInstance]) for entry in vertex.entry
                ]
                if entry is not None
            ]:  # type: ignore
                await self.__execute(entry, event)
            for activity in [
                activity
                for activity in [
                    self.model.get(activity, Behavior[TInstance])
                    for activity in vertex.activity
                ]
                if activity is not None
            ]:
                await self.__execute(activity, event)
            if not default_entry or vertex.initial == "":
                return vertex
            if (initial := self.model.get(vertex.initial, Vertex)) is not None:
                if (
                    len(initial.transitions) > 0
                    and (
                        transition := self.model.get(initial.transitions[0], Transition)
                    )
                    is not None
                ):
                    return await self.__transition(vertex, transition, event)
        elif isinstance(vertex, Choice):
            for qualified_name in vertex.transitions:
                if transition := self.model.get(qualified_name, Transition):
                    if constraint := self.model.get(
                        transition.guard or "", Guard[TInstance]
                    ):
                        if not await self.__evaluate(constraint, event):
                            continue
                    return await self.__transition(vertex, transition, event)
        return vertex

    async def __exit(self, vertex: Vertex, event: Event) -> Vertex:
        if isinstance(vertex, State):
            for activity in [
                activity
                for activity in [
                    self.model.get(activity, Behavior[TInstance])
                    for activity in vertex.activity
                ]
                if activity is not None
            ]:
                await self.__terminate(activity)
            for exit in [
                exit
                for exit in [
                    self.model.get(exit, Behavior[TInstance]) for exit in vertex.exit
                ]
                if exit is not None
            ]:
                await self.__execute(exit, event)
        return vertex

    async def __enabled(
        self, source: Vertex, event: Event
    ) -> typing.Optional[Transition]:
        # Use O(1) transition_map lookup instead of iterating through all transitions
        source_transitions = self.model.transition_map.get(source.qualified_name, {})
        transitions = source_transitions.get(event.qualified_name, [])

        for transition in transitions:
            if (
                maybe_guard := self.model.get(transition.guard or "", Guard[TInstance])
            ) is not None:
                # Guard exists - only return transition if guard passes
                if await self.__evaluate(maybe_guard, event):
                    return transition
                # Guard failed - continue to next transition
                continue
            else:
                # No guard - transition is always enabled
                return transition
        return None

    async def __process(self) -> None:
        event = await self.__queue.pop()
        deferred: list[Event] = []
        while event is not None:
            qualified_name = self.__state.qualified_name
            while qualified_name != "":
                source = self.model.get(qualified_name, State)
                if source is None:
                    break
                if transition := await self.__enabled(source, event):
                    state = await self.__transition(source, transition, event)
                    await self.__lock.acquire()
                    self.__state = state
                    self.__lock.release()
                    break
                # Use O(1) deferred_map lookup instead of iterating through deferred list
                source_deferred = self.model.deferred_map.get(qualified_name, {})
                if source_deferred.get(event.qualified_name, False):
                    deferred.append(event)
                    break
                qualified_name = source.owner()

            event = await self.__queue.pop()
        # Re-queue deferred events by pushing them back
        for deferred_event in deferred:
            self.__queue.push(deferred_event)
        self.__processing.release()

    async def __execute(self, behavior: Behavior[TInstance], event: Event) -> None:
        try:
            if behavior.kind == Kinds.Concurrent:
                # Create a new context for this activity
                activity_ctx = Context()

                # Create wrapped operation that handles context cancellation
                async def wrapped_operation():
                    try:
                        await behavior.operation(activity_ctx, self.__instance, event)
                    except asyncio.CancelledError:
                        # Cancel the context when task is cancelled
                        activity_ctx.cancel()
                    except Exception as e:
                        # Dispatch error event when activity throws exception
                        error_event = Event(
                            name="hsm_error", kind=Kinds.ErrorEvent, data=e
                        )
                        self.dispatch(error_event)

                task = asyncio.create_task(
                    wrapped_operation(),
                    name=behavior.qualified_name,
                )
                self.__active[behavior.qualified_name] = Activity(
                    context=activity_ctx, task=task
                )
            else:
                # Use main context for non-concurrent behaviors
                await behavior.operation(self.__context, self.__instance, event)
        except Exception as e:
            # For synchronous behaviors, dispatch error event immediately
            error_event = Event(name="hsm_error", kind=Kinds.ErrorEvent, data=e)
            self.dispatch(error_event)

    async def __evaluate(self, guard: Guard[TInstance], event: Event) -> bool:
        try:
            return await guard.expression(self.__context, self.__instance, event)
        except Exception as e:
            # Log the error and treat as guard failure
            print(f"Error evaluating guard {guard.qualified_name}: {e}")
            # In a production system, this would use proper logging
            return False

    async def __terminate(self, behavior: Behavior[TInstance]) -> None:
        active = self.__active.pop(behavior.qualified_name, None)
        if active is not None:
            # Cancel the task
            active.task.cancel()
            try:
                await active.task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                # Dispatch error event when activity throws exception
                error_event = Event(name="hsm_error", kind=Kinds.ErrorEvent, data=e)
                self.dispatch(error_event)

    async def __transition(
        self, current: Vertex, transition: Transition, event: Event
    ) -> Vertex:
        if (path := transition.paths.get(current.qualified_name)) is None:
            return current
        for exiting in path.exit:
            maybe_exiting = self.model.get(exiting, Vertex)
            if maybe_exiting is None:
                continue
            await self.__exit(maybe_exiting, event)
        for effect in transition.effect:
            maybe_effect = self.model.get(effect, Behavior[TInstance])
            if maybe_effect is None:
                continue
            await self.__execute(maybe_effect, event)
        if transition.kind == Kinds.Internal:
            return current
        for entering in path.enter:
            maybe_entering = self.model.get(entering, Vertex)
            if maybe_entering is None:
                continue
            default_entry = entering == transition.target
            current = await self.__enter(maybe_entering, event, default_entry)
            if default_entry:
                return current
        maybe_target = self.model.get(transition.target, Vertex)
        if maybe_target is None:
            return current
        return maybe_target

    def dispatch(self, event: Event) -> typing.Awaitable[None]:
        self.__queue.push(event)
        if self.__processing.try_acquire():
            self.__awaitable = asyncio.create_task(self.__process())
            return self.__awaitable
        return self.__awaitable

    async def __stop(self) -> None:  # type: ignore
        """Stop the state machine gracefully"""
        # Set processing to prevent new events from being processed
        self.__processing.acquire()

        # Create a final event for exit actions
        final_event = Event(name="hsm_final", kind=Kinds.CompletionEvent)

        # Exit all states from current state up to root
        while self.__state and self.__state.qualified_name != self.model.qualified_name:
            await self.__exit(self.__state, final_event)

            # Move up to parent state
            parent_path = os.path.dirname(self.__state.qualified_name)
            parent_state = self.model.get(parent_path, Vertex)
            if parent_state is None:
                break
            self.__state = parent_state

        # Cancel all active tasks
        for active in list(self.__active.values()):
            active.task.cancel()
            try:
                await active.task
            except asyncio.CancelledError:
                pass
        self.__active.clear()

        # Cancel the main context
        self.__context.cancel()

        self.__processing.release()


async def start(
    ctx: typing.Optional[Context],
    instance: TInstance,
    model: Model,
) -> HSM[TInstance]:
    sm = HSM(instance=instance, model=model, ctx=ctx)
    start = getattr(sm, "_HSM__start", None)
    if start is None:
        raise AttributeError("HSM has no __start method")
    await start()
    return sm


def build_transition_table(model: Model) -> None:
    """Build a transition lookup table for O(1) event dispatch"""
    from .kind import is_kind

    # For each state in the model
    for state_name, state in model.members.items():
        if not is_kind(state.kind, Kinds.State):
            continue

        # Initialize tables for this state
        model.transition_map[state_name] = {}

        # Collect all transitions accessible from this state by walking up hierarchy
        transitions_by_event: dict[str, list[tuple[Transition, int]]] = {}
        current_path = state_name
        depth = 0

        while current_path:
            current_state = model.members.get(current_path)
            if current_state and is_kind(current_state.kind, Kinds.State):
                state_obj = typing.cast(State, current_state)
                # Process transitions at this level
                for transition_name in state_obj.transitions:
                    transition = model.get(transition_name, Transition)

                    if transition and transition.events:
                        # Process each event this transition handles
                        for event_name in transition.events:
                            # Skip wildcard events - not supported
                            if "*" in event_name:
                                continue

                            # Regular event - add to lookup table
                            if event_name not in transitions_by_event:
                                transitions_by_event[event_name] = []
                            transitions_by_event[event_name].append((transition, depth))

            # Move up to parent
            if current_path == "/" or not current_path:
                break
            parent_path = os.path.dirname(current_path)
            if parent_path == current_path:  # Avoid infinite loop
                break
            current_path = parent_path
            depth += 1

        # Sort transitions by priority (lower depth = higher priority)
        for event_name, transitions in transitions_by_event.items():
            transitions.sort(key=lambda x: x[1])
            # Extract just the transition objects
            model.transition_map[state_name][event_name] = [t[0] for t in transitions]


def build_deferred_table(model: Model) -> None:
    """Build a deferred event lookup table for O(1) deferred event checking"""
    from .kind import is_kind

    # For each state in the model
    for state_name, state in model.members.items():
        if not is_kind(state.kind, Kinds.State):
            continue

        model.deferred_map[state_name] = {}
        current_path = state_name

        while current_path:
            current_state = model.members.get(current_path)
            if current_state and is_kind(current_state.kind, Kinds.State):
                state_obj = typing.cast(State, current_state)

                # Process deferred events at this level
                if state_obj.deferred:
                    for deferred_event in state_obj.deferred:
                        transitions = model.transition_map[state_name].get(
                            deferred_event, []
                        )
                        if transitions and any(
                            t.source == state_name for t in transitions
                        ):
                            continue
                        # Only support exact event names for O(1) lookup
                        # Skip wildcard patterns for performance
                        if "*" not in deferred_event:
                            model.deferred_map[state_name][deferred_event] = True

            # Move up to parent
            if current_path == "/" or not current_path:
                break
            parent_path = os.path.dirname(current_path)
            if parent_path == current_path:  # Avoid infinite loop
                break
            current_path = parent_path


async def stop(sm: typing.Union[HSM[TInstance], Instance]) -> None:
    if isinstance(sm, Instance):
        hsm: HSM[TInstance] = getattr(sm, "_Instance__hsm")
    else:
        hsm = sm
    stop = getattr(hsm, "_HSM__stop", None)
    if stop is None:
        raise AttributeError("HSM has no __stop method")
    await stop()


if __name__ == "__main__":
    sm = define("root", state("s1"), state("s2"))
    print(sm.members)
