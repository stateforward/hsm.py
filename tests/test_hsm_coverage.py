import asyncio
import re
import subprocess
import sys
from datetime import timedelta

import pytest

import hsm.hsm as core


class CoverageInstance(core.Instance):
    def __init__(self):
        super().__init__()
        self.values: list[int] = []

    def double(self, value: int) -> int:
        self.values.append(value)
        return value * 2


def _bare_model(name: str = "Bare") -> core.Model:
    model = core.Model(qualified_name=f"/{name}")
    model.set(model.qualified_name, model)
    return model


def _action(*_args: object) -> None:
    return None


def _guard_true(*_args: object) -> bool:
    return True


class _NullPartial(core.RedefinableElement):
    def redefine(
        self,
        model: core.Model,
        stack: list[core.Element],
        element: core.Element | None = None,
    ) -> core.Element | None:
        del model, stack
        return element
        return None


def test_helper_and_factory_branches(capsys: pytest.CaptureFixture[str]):
    model = _bare_model()
    state = core.StateElement(qualified_name="/Bare/idle")
    model.set(state.qualified_name, state)

    assert core.Match("value") is False
    assert core.match("value", "val*") is True
    assert core.Element().owner() == ""
    assert core.NamedElement(qualified_name="/").owner() == ""
    assert core.NamedElement(qualified_name="/Bare/idle").name() == "idle"
    assert core.find([], core.StateElement) is None
    assert core.RedefinableElement().redefine(model, []) is None
    assert model.get(state.qualified_name, core.TransitionElement) is None
    assert core.LCA("", "/Bare/idle") == "/Bare/idle"
    assert core.least_common_ancestor("/Bare/a", "/Bare/b") == "/Bare"
    assert core.IsAncestor("/", "/Bare/idle") is True
    assert core.IsAncestor("/Bare/idle", "/Bare/idle") is False
    assert core.is_ancestor("/Bare", "/Bare/idle") is True
    history_model = core.Define(
        "HistoryPaths",
        core.InitialElement(core.Target("parent")),
        core.StateElement(
            "parent",
            core.ShallowHistory("hist", core.Target("child")),
            core.StateElement("child"),
        ),
    )
    assert history_model.history_paths[
        ("/HistoryPaths/parent", "/HistoryPaths/parent/child")
    ] == ("/HistoryPaths/parent/child",)
    assert core._current_task_or_none() is None
    assert core._qualify_model_name("/Bare", "") == ""
    assert core._qualify_model_name("/Bare", "/Bare/idle") == "/Bare/idle"
    assert core._qualify_model_name("/Bare", "/outside") == "/Bare/outside"

    class Closable:
        def __init__(self):
            self.closed = False

        def close(self) -> None:
            self.closed = True

    closable = Closable()
    core._close_awaitable(closable)
    assert closable.closed is True
    with pytest.raises(RuntimeError, match="returned awaitable"):
        core._close_awaitable(Closable())
        raise RuntimeError("transition behavior returned awaitable")

    event = core.Event(
        name="go",
        data=1,
        kind=core.Kinds.ChangeEvent,
        schema=int,
    )
    assert event.Name == "go"
    assert event.Data == 1
    assert event.Kind == core.Kinds.ChangeEvent
    assert event.QualifiedName == "go"
    assert event.Schema is int
    assert core.CompletionEvent("done").kind == core.Kinds.CompletionEvent

    same_event = core._event_from_name(event)
    assert same_event is event
    assert core._event_from_name("*") is core.AnyEvent
    assert core._event_from_name("other").name == "other"

    assert isinstance(core.InitialElement("named"), core.PartialInitial)
    assert isinstance(core.TransitionElement("named"), core.PartialTransition)
    assert isinstance(core.Source(core.StateElement("idle")), core.PartialSource)
    assert isinstance(core.Target(core.StateElement("idle")), core.PartialTarget)
    assert isinstance(core.When(lambda *_: None), core.PartialWhen)
    assert isinstance(core.When("count"), core.PartialOnSet)
    assert isinstance(core.Defer(core.Event(name="later")), core.PartialDefer)
    assert isinstance(
        core.ShallowHistory(core.TransitionElement(core.Target("idle"))),
        core.PartialHistory,
    )
    assert isinstance(
        core.DeepHistory(core.TransitionElement(core.Target("idle"))),
        core.PartialHistory,
    )
    assert isinstance(core.Final(core.StateElement("done")), core.PartialFinal)

    result = subprocess.run(
        [sys.executable, "-m", "hsm.hsm"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "/root/s1" in result.stdout


@pytest.mark.asyncio
async def test_context_waitable_queue_and_instance_branches():
    assert core._current_task_or_none() is asyncio.current_task()
    await core._normalize_waitable(None)

    trigger = asyncio.Event()
    trigger.set()
    await core._normalize_waitable(trigger)
    await core._normalize_waitable(asyncio.sleep(0))
    future = asyncio.get_running_loop().create_future()
    future.set_result(None)
    await core._normalize_waitable(future)

    class FutureWaiter:
        def __init__(self):
            self.future = asyncio.get_running_loop().create_future()
            self.future.set_result(None)

        def wait(self) -> asyncio.Future[None]:
            return self.future

    await core._normalize_waitable(FutureWaiter())

    class AsyncWaiter:
        def __init__(self):
            self.seen = False

        async def wait(self) -> None:
            self.seen = True

    waiter = AsyncWaiter()
    await core._normalize_waitable(waiter)
    assert waiter.seen is True

    class InvalidWaiter:
        def wait(self) -> None:
            return None

    with pytest.raises(TypeError, match="unsupported When"):
        await core._normalize_waitable(InvalidWaiter())

    ctx = core.Context().WithValue(core.Keys.Instances, {})
    seen: list[str] = []

    def ok_listener(_) -> None:
        seen.append("ok")

    ctx.Done().add_done_callback(ok_listener)
    waiting = asyncio.wrap_future(ctx.Done())
    ctx.cancel()
    await waiting
    ctx.cancel()
    assert ctx.Done().done() is True
    assert seen == ["ok"]

    immediate: list[str] = []

    def immediate_listener(_) -> None:
        immediate.append("now")

    ctx.Done().add_done_callback(immediate_listener)
    assert immediate == ["now"]

    other = core.Context()
    other.cancel()
    await asyncio.wrap_future(other.Done())

    assert core.context is core.Context
    assert core.context_key is core.ContextKey
    assert core.keys is core.Keys
    request_key = core.context_key("request")
    alias_context = other.with_value(request_key, "req-1")
    assert alias_context.value(request_key) == "req-1"
    assert alias_context.Value(request_key) == "req-1"
    assert other.value(request_key) is None
    assert core.from_context(alias_context) == (None, False)
    instances, ok = core.instances_from_context(alias_context)
    assert ok is False
    assert instances == []

    queue = core.MultiQueue()
    queue.push(core.Event(name="regular"))
    queue.push(core.Event(name="complete", kind=core.Kinds.CompletionEvent))
    assert queue.len() == (2, None)
    complete, ok, err = queue.pop()
    assert ok and err is None and complete.name == "complete"
    regular, ok, err = queue.pop()
    assert ok and err is None and regular.name == "regular"
    _, ok, err = queue.pop()
    assert not ok and err is None

    class RecordingQueue(core.MultiQueue):
        def __init__(self):
            super().__init__()
            self.pushed: list[str] = []

        def push(self, event: core.Event) -> core.QueuePushResult:
            self.pushed.append(event.name)
            return super().push(event)

    regular_queue = RecordingQueue()
    queue = core.MultiQueue(regular_queue)
    queue.push(core.Event(name="regular"))
    queue.push(core.Event(name="complete", kind=core.Kinds.CompletionEvent))
    assert regular_queue.pushed == ["regular"]
    assert queue.len() == (2, None)
    complete, ok, err = queue.pop()
    assert ok and err is None and complete.name == "complete"
    regular, ok, err = queue.pop()
    assert ok and err is None and regular.name == "regular"
    _, ok, err = queue.pop()
    assert not ok and err is None

    class IncompleteFifo:
        def push(self, event: core.Event) -> core.QueuePushResult:
            return (None,)

    with pytest.raises(TypeError, match="callable pop"):
        core.MultiQueue(IncompleteFifo())

    class ListFifo(core.Fifo):
        def __init__(self) -> None:
            super().__init__()
            self.items: list[core.Event] = []

        def push(self, event: core.Event) -> core.QueuePushResult:
            self.items.append(event)
            return (None,)

        def pop(self) -> core.QueuePopResult:
            if not self.items:
                return (core.Event(), False, None)
            return (self.items.pop(0), True, None)

        def len(self) -> core.QueueLenResult:
            return (len(self.items), None)

    hook_queue = core.MultiQueue(ListFifo())
    hook_queue.push(core.Event(name="hooked"))
    assert hook_queue.len() == (1, None)
    hooked, ok, err = hook_queue.pop()
    assert ok and err is None and hooked.name == "hooked"

    class FailingPushQueue(core.MultiQueue):
        def __init__(self, error: RuntimeError):
            super().__init__()
            self.error = error

        def push(self, event: core.Event) -> core.QueuePushResult:
            return (self.error,)

    class QueueErrorInstance(core.Instance):
        def __init__(self):
            super().__init__()
            self.error: BaseException | None = None

    async def record_queue_error(ctx, inst: QueueErrorInstance, event: core.Event):
        inst.error = event.data

    queue_error = RuntimeError("queue push failed")
    error_model = core.Define(
        "QueuePushError",
        core.InitialElement(core.Target("idle")),
        core.StateElement(
            "idle",
            core.TransitionElement(
                core.On(core.ErrorEvent),
                core.Target("../failed"),
                core.Effect(record_queue_error),
            ),
        ),
        core.StateElement("failed"),
    )
    queue_error_instance = QueueErrorInstance()
    error_ctx = core.Context()
    queue_error_sm = core.HSM(
        instance=queue_error_instance,
        model=error_model,
        ctx=error_ctx,
        config=core.Config(Queue=FailingPushQueue(queue_error)),
    )
    await core.Start(error_ctx, queue_error_sm)
    await queue_error_instance.dispatch(core.Event(name="go"))
    assert queue_error_instance.state() == "/QueuePushError/failed"
    assert queue_error_instance.error is queue_error

    machine_free = CoverageInstance()
    with pytest.raises(core.ValidationError, match="started HSM"):
        machine_free.dispatch(core.Event(name="noop"))
    assert machine_free.state() == ""
    assert machine_free.context() is None
    await machine_free.stop(machine_free.context())
    with pytest.raises(core.ValidationError, match="started HSM"):
        await machine_free.restart(machine_free.context())

    await core.noop_operation(core.Context(), machine_free, core.Event(name="noop"))
    assert (
        await core.noop_expression(
            core.Context(), machine_free, core.Event(name="noop")
        )
        is True
    )
    assert await core.noop_duration(
        core.Context(), machine_free, core.Event(name="noop")
    ) == timedelta(seconds=0)


@pytest.mark.asyncio
async def test_operation_callback_resolution_and_invoke_contract():
    ctx = core.Context()
    instance = CoverageInstance()
    event = core.Event(name="go")

    async def behavior(
        behavior_ctx: core.Context, inst: core.Instance, behavior_event: core.Event
    ) -> str:
        assert behavior_ctx is ctx
        assert inst is instance
        assert behavior_event is event
        return "behavior"

    assert await core._maybe_await(behavior(ctx, instance, event)) == "behavior"

    async def operation(
        operation_ctx: core.Context, inst: core.Instance, value: int
    ) -> int:
        assert operation_ctx is ctx
        assert inst is instance
        return value + 1

    assert await core._maybe_await(operation(ctx, instance, 4)) == 5

    model = _bare_model("Operations")
    model.operations["fallback"] = core.OperationElement(qualified_name="fallback")
    instance.fallback = lambda fallback_event: fallback_event.name  # type: ignore[attr-defined]
    callback = core._operation_callback(model.operations["fallback"], instance)
    assert callback(ctx, instance, event) == "go"

    with pytest.raises(core.ValidationError, match='missing operation "missing"'):
        core._resolve_operation(model, "missing")


@pytest.mark.asyncio
async def test_dispatch_fanout_helper_edge_paths():
    assert await core._dispatch_machines([]) is None
    assert await core._await_all([asyncio.sleep(0)]) is None
    assert await core._await_all_shielded([core._future_done()]) is None


@pytest.mark.asyncio
async def test_dispatch_reentrant_queue_paths_notify_and_do_not_wait():
    class FanoutInstance(core.Instance):
        pass

    async def mark(ctx: core.Context, inst: FanoutInstance, event: core.Event) -> None:
        return None

    model = core.Define(
        "FanoutHelperCoverage",
        core.InitialElement(core.Target("idle")),
        core.StateElement(
            "idle",
            core.TransitionElement(
                core.On("go"), core.Target("../done"), core.Effect(mark)
            ),
        ),
        core.StateElement("done"),
    )

    ctx = core.Context()
    instance = FanoutInstance()
    machine = await core.Started(ctx, instance, model)
    dispatched = core.AfterDispatch(ctx, instance, core.Event(name="go"))

    await machine.dispatch(core.Event(name="go"))
    await dispatched
    assert instance.state() == "/FanoutHelperCoverage/done"

    await core.Stop(instance)

    done_awaitable = FanoutInstance()
    machine = await core.Started(ctx, done_awaitable, model)
    assert machine._processing.try_acquire() is True
    machine._awaitable = core._future_done()
    try:
        assert await machine.dispatch(core.Event(name="go")) is None
        assert done_awaitable.state() == "/FanoutHelperCoverage/idle"
    finally:
        machine._processing.release()
    await core.Stop(done_awaitable)

    same_task = FanoutInstance()
    machine = await core.Started(ctx, same_task, model)
    assert machine._processing.try_acquire() is True
    machine._awaitable = asyncio.current_task()
    try:
        assert await machine.dispatch(core.Event(name="go")) is None
        assert same_task.state() == "/FanoutHelperCoverage/idle"
    finally:
        machine._processing.release()
    await core.Stop(same_task)

    completed_awaitable = FanoutInstance()
    machine = await core.Started(ctx, completed_awaitable, model)
    assert machine._processing.try_acquire() is True
    machine._awaitable = core._future_done()
    try:
        assert await machine.dispatch(core.Event(name="go")) is None
        assert completed_awaitable.state() == "/FanoutHelperCoverage/idle"
    finally:
        machine._processing.release()
    await core.Stop(completed_awaitable)

    time_event_instance = FanoutInstance()
    machine = await core.Started(ctx, time_event_instance, model)
    await machine.dispatch(core.Event(name="timer", kind=core.Kinds.TimeEvent))
    assert time_event_instance.state() == "/FanoutHelperCoverage/idle"
    await core.Stop(time_event_instance)


def test_direct_validation_branches():
    model = _bare_model()
    state = core.StateElement(qualified_name="/Bare/idle")
    model.set(state.qualified_name, state)
    target = core.StateElement(qualified_name="/Bare/other")
    model.set(target.qualified_name, target)

    final_state = core.FinalStateElement(qualified_name="/Bare/final")
    model.set(final_state.qualified_name, final_state)

    with pytest.raises(core.ValidationError, match="state must be called"):
        core.StateElement("orphan").apply(model, [])

    with pytest.raises(
        core.ValidationError, match="initial must be called within a StateElement"
    ):
        core.InitialElement(core.Target("idle")).apply(model, [])

    with pytest.raises(core.ValidationError, match="already has an initial state"):
        core.Define(
            "DuplicateInitial",
            core.StateElement(
                "idle",
                core.InitialElement(core.Target("child")),
                core.InitialElement(core.Target("child")),
                core.StateElement("child"),
            ),
            core.InitialElement(core.Target("idle")),
        )

    with pytest.raises(core.ValidationError, match="cannot have a guard"):
        core.Define(
            "GuardedInitial",
            core.InitialElement(core.Target("idle")),
            core.StateElement(
                "idle",
                core.InitialElement(
                    core.GuardElement(_guard_true), core.Target("child")
                ),
                core.StateElement("child"),
            ),
        )

    with pytest.raises(core.ValidationError, match="initial state is required"):
        core.Define(
            "MissingInitialTarget",
            core.StateElement("idle", core.InitialElement("missing")),
        )

    with pytest.raises(core.ValidationError, match="must target a nested state"):
        core.Define(
            "WrongInitialTarget",
            core.InitialElement(core.Target("outer")),
            core.StateElement(
                "outer",
                core.InitialElement(core.Target("../other")),
                core.StateElement("child"),
            ),
            core.StateElement("other"),
        )

    with pytest.raises(core.ValidationError, match="within a nested StateElement"):
        core.ShallowHistory("memory").apply(model, [model])

    with pytest.raises(
        core.ValidationError, match='model name "Bad/Model" cannot contain "/"'
    ):
        core.Define("Bad/Model")

    slash_name_cases = [
        (
            core.StateElement("bad/state"),
            [model],
            'state name "bad/state" cannot contain "/"',
        ),
        (
            core.Final("bad/final"),
            [model],
            'final name "bad/final" cannot contain "/"',
        ),
        (
            core.ShallowHistory(
                "bad/history", core.TransitionElement(core.Target("idle"))
            ),
            [model, state],
            'ShallowHistory name "bad/history" cannot contain "/"',
        ),
        (
            core.DeepHistory(
                "bad/history", core.TransitionElement(core.Target("idle"))
            ),
            [model, state],
            'DeepHistory name "bad/history" cannot contain "/"',
        ),
        (
            core.Attribute("bad/attribute"),
            [model],
            'attribute name "bad/attribute" cannot contain "/"',
        ),
        (
            core.Operation("bad/operation"),
            [model],
            'operation name "bad/operation" cannot contain "/"',
        ),
    ]
    for partial, stack, message in slash_name_cases:
        with pytest.raises(core.ValidationError, match=re.escape(message)):
            partial.apply(model, stack)

    with pytest.raises(
        core.ValidationError, match="ShallowHistory requires a default transition"
    ):
        core.Define(
            "MissingShallowHistoryDefault",
            core.InitialElement(core.Target("parent/idle")),
            core.StateElement(
                "parent",
                core.InitialElement(core.Target("idle")),
                core.StateElement("idle"),
                core.ShallowHistory("memory"),
            ),
        )

    with pytest.raises(
        core.ValidationError, match="DeepHistory requires a default transition"
    ):
        core.Define(
            "MissingDeepHistoryDefault",
            core.InitialElement(core.Target("parent/idle")),
            core.StateElement(
                "parent",
                core.InitialElement(core.Target("idle")),
                core.StateElement("idle"),
                core.DeepHistory("memory"),
            ),
        )

    with pytest.raises(core.ValidationError, match="Top level transitions"):
        core.ResolvePaths(
            transition=core.TransitionElement(
                qualified_name="/Bare/toplevel",
                source="/",
                target="/Bare/idle",
            )
        ).apply(model, [])

    with pytest.raises(
        core.ValidationError, match='VertexElement "/Bare/missing" not found'
    ):
        core.ValidateVertex(qualified_name="/Bare/missing").apply(model, [])

    with pytest.raises(core.ValidationError, match="transition must be called"):
        core.TransitionElement(core.On("go")).apply(model, [])

    with pytest.raises(core.ValidationError, match='Source "/Bare/missing" not found'):
        core.TransitionElement(core.Source("/Bare/missing"), core.On("go")).apply(
            model, [state]
        )

    with pytest.raises(core.ValidationError, match="has no events"):
        core.PartialTransition().apply(model, [state])

    with pytest.raises(
        core.ValidationError, match="must be called within a hsm.TransitionElement"
    ):
        core.Source("idle").apply(model, [])

    with pytest.raises(core.ValidationError, match="already has a source"):
        core.Source("other").apply(
            model,
            [core.TransitionElement(qualified_name="/Bare/t", source="/Bare/idle")],
        )

    with pytest.raises(core.ValidationError, match='missing source ""'):
        core.Source(_NullPartial()).apply(
            model, [core.TransitionElement(qualified_name="/Bare/t", source=".")]
        )

    with pytest.raises(
        core.ValidationError, match="must be called within TransitionElement"
    ):
        core.Target("idle").apply(model, [])

    with pytest.raises(core.ValidationError, match="already has a target"):
        core.Target("other").apply(
            model,
            [
                core.TransitionElement(
                    qualified_name="/Bare/t", source="/Bare/idle", target="/Bare/other"
                )
            ],
        )

    with pytest.raises(core.ValidationError, match='missing target ""'):
        core.Target(_NullPartial()).apply(
            model, [core.TransitionElement(qualified_name="/Bare/t")]
        )

    with pytest.raises(
        core.ValidationError, match="entry must be called within a StateElement"
    ):
        core.Entry(_action).apply(model, [])

    with pytest.raises(core.ValidationError, match="has no missing"):
        core.PartialBehaviors(qualified_name="missing", type=core.StateElement).apply(
            model, [state]
        )

    with pytest.raises(
        core.ValidationError, match="guard must be called within a TransitionElement"
    ):
        core.GuardElement(_guard_true).apply(model, [])

    with pytest.raises(
        core.ValidationError, match="trigger must be called within a TransitionElement"
    ):
        core.On("go").apply(model, [])

    with pytest.raises(
        core.ValidationError, match="defer must be called within a state"
    ):
        core.Defer(core.Event(name="later")).apply(model, [])

    with pytest.raises(
        core.ValidationError, match="choice must be called within a state or transition"
    ):
        core.ChoiceElement("branch").apply(model, [])

    with pytest.raises(
        core.ValidationError, match="choice must be called within a state"
    ):
        core.ChoiceElement("branch").apply(
            model, [core.TransitionElement(qualified_name="/Bare/t", source=".")]
        )

    with pytest.raises(
        core.ValidationError,
        match='choice "/NoTransitions/idle/branch" has no transitions',
    ):
        core.Define(
            "NoTransitions",
            core.InitialElement(core.Target("idle")),
            core.StateElement("idle", core.ChoiceElement("branch")),
        )

    with pytest.raises(core.ValidationError, match="cannot have a guard"):
        core.Define(
            "ChoiceGuard",
            core.InitialElement(core.Target("idle")),
            core.StateElement(
                "idle",
                core.ChoiceElement(
                    "branch",
                    core.TransitionElement(
                        core.GuardElement(_guard_true), core.Target("../done")
                    ),
                ),
            ),
            core.StateElement("done"),
        )

    with pytest.raises(
        core.ValidationError, match='Final state "/Bare/missing" not found'
    ):
        core.ValidateFinalState(qualified_name="/Bare/missing").apply(model, [])

    final_state.transitions.append("/Bare/final/t")
    with pytest.raises(core.ValidationError, match="cannot have transitions"):
        core.ValidateFinalState(qualified_name="/Bare/final").apply(model, [])
    final_state.transitions.clear()
    final_state.entry.append("entry")
    with pytest.raises(core.ValidationError, match="cannot have an entry action"):
        core.ValidateFinalState(qualified_name="/Bare/final").apply(model, [])
    final_state.entry.clear()
    final_state.exit.append("exit")
    with pytest.raises(core.ValidationError, match="cannot have an exit action"):
        core.ValidateFinalState(qualified_name="/Bare/final").apply(model, [])
    final_state.exit.clear()
    final_state.activity.append("activity")
    with pytest.raises(core.ValidationError, match="cannot have an activity"):
        core.ValidateFinalState(qualified_name="/Bare/final").apply(model, [])
    final_state.activity.clear()

    with pytest.raises(
        core.ValidationError, match="Final must be called within a namespace"
    ):
        core.Final("done").apply(model, [])

    with pytest.raises(core.ValidationError, match="attribute name cannot be empty"):
        core.Attribute("").apply(model, [])

    core.Attribute("count").apply(model, [])
    with pytest.raises(core.ValidationError, match="duplicate attribute count"):
        core.Attribute("count").apply(model, [])

    with pytest.raises(core.ValidationError, match="operation name cannot be empty"):
        core.Operation("").apply(model, [model])

    core.Operation("work").apply(model, [model])
    with pytest.raises(core.ValidationError, match="duplicate operation work"):
        core.Operation("work").apply(model, [model])

    with pytest.raises(
        core.ValidationError, match="operation must be called within Define"
    ):
        core.Operation("lost").apply(model, [])

    with pytest.raises(
        core.ValidationError,
        match="OnSet\\(\\) must be called within a TransitionElement",
    ):
        core.OnSet("count").apply(model, [])

    with pytest.raises(
        core.ValidationError, match="requires a non-empty attribute name"
    ):
        core.OnSet("").apply(model, [core.TransitionElement(qualified_name="/Bare/t")])

    with pytest.raises(
        core.ValidationError,
        match="OnCall\\(\\) must be called within a TransitionElement",
    ):
        core.OnCall("work").apply(model, [])

    with pytest.raises(
        core.ValidationError, match="requires a non-empty operation name"
    ):
        core.OnCall("").apply(model, [core.TransitionElement(qualified_name="/Bare/t")])

    with pytest.raises(core.ValidationError, match='Source "/Bare/missing" not found'):
        core.PartialTransition(
            qualified_name="timer",
            owned_elements=[
                core.PartialOn("go"),
                core.After(_action),  # type: ignore[arg-type]
                core.PartialTarget("x"),
                core.PartialSource("/Bare/missing"),
            ],
        ).apply(model, [core.StateElement("s", qualified_name="/Bare/s")])
        while model.owned_elements:
            partial = model.owned_elements.pop()
            if isinstance(partial, core.RedefinableElement):
                partial.redefine(model, [])

    with pytest.raises(
        core.ValidationError, match="after must be called within a TransitionElement"
    ):
        core.After(_action).apply(model, [])  # type: ignore[arg-type]

    with pytest.raises(
        core.ValidationError, match="when must be called within a TransitionElement"
    ):
        core.When(lambda *_: None).apply(model, [])

    initial = core.InitialElement(qualified_name="/Bare/.initial")
    model.set(initial.qualified_name, initial)
    with pytest.raises(core.ValidationError, match="source is a StateElement"):
        core.When(lambda *_: None).apply(
            model,
            [
                core.TransitionElement(
                    qualified_name="/Bare/t", source=initial.qualified_name
                )
            ],
        )


def test_model_finalization_validation_branches():
    with pytest.raises(core.ValidationError, match="entry actions are not allowed"):
        core.Define(
            "TopEntry",
            core.InitialElement(core.Target("idle")),
            core.Entry(_action),
            core.StateElement("idle"),
        )

    with pytest.raises(core.ValidationError, match="exit actions are not allowed"):
        core.Define(
            "TopExit",
            core.InitialElement(core.Target("idle")),
            core.Exit(_action),
            core.StateElement("idle"),
        )

    pending_on_call = core.Define(
        "PendingOnCall",
        core.Operation("work"),
        core.InitialElement(core.Target("idle")),
        core.StateElement(
            "idle",
            core.TransitionElement(core.OnCall("work"), core.Target("../done")),
        ),
        core.StateElement("done"),
    )
    assert pending_on_call.events["@call:work"].kind == core.Kinds.CallEvent
    assert pending_on_call.events["@call:work"].source == "/PendingOnCall/work"


@pytest.mark.asyncio
async def test_runtime_wrapper_group_and_call_edge_branches():
    async def activity_done(
        ctx: core.Context, instance: CoverageInstance, event: core.Event
    ) -> None:
        return None

    model = core.Define(
        "RuntimeCoverage",
        core.Attribute("count", 1),
        core.Attribute("bag", {"items": []}),
        core.Operation("double"),
        core.Operation("missing_method"),
        core.InitialElement(core.Target("idle")),
        core.StateElement(
            "idle",
            core.Activity(activity_done),
            core.TransitionElement(core.On("go"), core.Target("../done")),
            core.TransitionElement(core.OnSet("count"), core.Target("../set_state")),
            core.TransitionElement(core.OnCall("double"), core.Target("../called")),
        ),
        core.StateElement(
            "called", core.TransitionElement(core.On("reset"), core.Target("../idle"))
        ),
        core.StateElement(
            "set_state",
            core.TransitionElement(core.On("reset"), core.Target("../idle")),
        ),
        core.StateElement("done"),
    )

    ctx = core.Context().WithValue(core.Keys.Instances, {})
    first = CoverageInstance()
    second = CoverageInstance()
    first_hsm = await core.Started(ctx, first, model)
    second_hsm = await core.Start(ctx, second, model)

    assert core.FromContext(first_hsm.context()) == (first_hsm, True)
    assert first_hsm in core.InstancesFromContext(ctx)[0]
    assert first_hsm.id.startswith("hsm-")
    assert first_hsm.qualified_name == "/RuntimeCoverage"
    assert core.QualifiedName(first) == "/RuntimeCoverage"
    assert core.AfterExecuted(ctx, first, "/RuntimeCoverage/idle").done() is False

    missing, ok = core.Get(ctx, first, "missing")
    assert (missing, ok) == (None, False)

    bag, ok = core.Get(ctx, first, "bag")
    assert ok is True
    bag["items"].append("mutated")
    fresh_bag, ok = core.Get(ctx, first, "bag")
    assert ok is True
    assert fresh_bag == {"items": []}

    await core.Set(ctx, first, "count", 1)
    assert first.state() == "/RuntimeCoverage/idle"

    await core.Set(ctx, first, "count", 2)
    assert first.state() == "/RuntimeCoverage/set_state"
    await core.Dispatch(ctx, first, core.Event(name="reset"))
    assert first.state() == "/RuntimeCoverage/idle"

    assert await core.Call(ctx, first, "double", 4) == 8
    assert first.values == [4]
    assert first.state() == "/RuntimeCoverage/called"
    await core.Dispatch(ctx, first, core.Event(name="reset"))

    with pytest.raises(core.ValidationError, match="operation name cannot be empty"):
        await first_hsm.call("")

    with pytest.raises(core.ValidationError, match='missing operation "unknown"'):
        await first_hsm.call("unknown")

    with pytest.raises(
        core.ValidationError, match='missing operation "missing_method"'
    ):
        await first_hsm.call("missing_method")

    ghost = core.TransitionElement(
        qualified_name="/RuntimeCoverage/ghost", target="/RuntimeCoverage/done"
    )
    first_hsm.model.transition_map[first.state()].setdefault("ghost", []).append(ghost)
    snapshot = core.TakeSnapshot(ctx, first)
    assert snapshot.StateElement == "/RuntimeCoverage/idle"
    assert snapshot.QueueLen == 0
    assert all(event.Name != "ghost" for event in snapshot.Events)

    group = core.NewGroup(first, core.NewGroup(second), None)
    assert group.state() == [
        "/RuntimeCoverage/idle",
        "/RuntimeCoverage/idle",
    ]
    assert group.context() is first.context()
    group_snapshot = core.TakeSnapshot(None, group)
    assert group_snapshot.ID != ""
    assert group_snapshot.QualifiedName == "/RuntimeCoverage,/RuntimeCoverage"
    assert (
        group_snapshot.StateElement == "/RuntimeCoverage/idle | /RuntimeCoverage/idle"
    )
    assert group_snapshot.QueueLen == 0

    identified_group = core.MakeGroup("coverage-group", first, second)
    assert core.TakeSnapshot(None, identified_group).ID == "coverage-group"

    await core.Dispatch(None, group, core.Event(name="go"))
    assert first.state() == "/RuntimeCoverage/done"
    assert second.state() == "/RuntimeCoverage/done"

    await core.Restart(group)
    assert first.state() == "/RuntimeCoverage/idle"
    assert second.state() == "/RuntimeCoverage/idle"

    await core.Set(None, first, "count", 3)
    await core.Set(None, second, "count", 3)
    assert first.state() == "/RuntimeCoverage/set_state"
    assert second.state() == "/RuntimeCoverage/set_state"

    await core.Dispatch(None, group, core.Event(name="reset"))
    assert await core.Call(None, first, "double", 5) == 10
    assert first.values[-1] == 5
    assert second.values == []

    await core.Stop(group)
    assert first.state() == "/RuntimeCoverage"
    assert second.state() == "/RuntimeCoverage"

    empty_group = core.NewGroup(None)
    assert empty_group.state() == []
    assert empty_group.context() is None
    empty_snapshot = empty_group.take_snapshot()
    assert empty_snapshot.ID != ""
    assert core.QualifiedName(empty_group) == ""
    with pytest.raises(core.ValidationError, match="started HSM"):
        await core.Dispatch(None, empty_group, core.Event(name="noop"))
    await core.Restart(empty_group)
    await core.Stop(empty_group)

    await core.DispatchAll(None, core.Event(name="noop"))
    await core.DispatchTo(None, core.Event(name="noop"), "hsm-*")

    with pytest.raises(core.ValidationError, match="started HSM"):
        core.TakeSnapshot(None, CoverageInstance())


@pytest.mark.asyncio
async def test_dispatch_event_schema_is_copied_from_caller():
    def mutate_schema(
        ctx: core.Context, instance: CoverageInstance, event: core.Event
    ) -> None:
        event.schema["mutated"] = True

    model = core.Define(
        "SchemaOwnership",
        core.InitialElement(core.Target("idle")),
        core.StateElement(
            "idle",
            core.TransitionElement(core.On("touch"), core.Effect(mutate_schema)),
        ),
    )
    ctx = core.Context()
    instance = CoverageInstance()
    await core.Started(ctx, instance, model)

    schema = {"mutated": False}
    event = core.Event(name="touch", schema=schema)
    await core.Dispatch(ctx, instance, event)

    assert schema == {"mutated": False}
