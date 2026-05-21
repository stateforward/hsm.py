import asyncio
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


class _NullPartial(core.PartialElement):
    def apply(self, model: core.Model, stack: list[core.NamedElement]) -> None:
        return None


def test_helper_and_factory_branches(capsys: pytest.CaptureFixture[str]):
    model = _bare_model()
    state = core.StateNode(qualified_name="/Bare/idle")
    model.set(state.qualified_name, state)

    assert core.Match("value") is False
    assert core.match("value", "val*") is True
    assert core.Element().owner() == ""
    assert core.NamedElement(qualified_name="/").owner() == ""
    assert core.NamedElement(qualified_name="/Bare/idle").name() == "idle"
    assert core.find([], core.StateNode) is None
    assert core.PartialElement().apply(model, []) is None
    assert model.get(state.qualified_name, core.TransitionNode) is None
    assert core.LCA("", "/Bare/idle") == "/Bare/idle"
    assert core.least_common_ancestor("/Bare/a", "/Bare/b") == "/Bare"
    assert core.IsAncestor("/", "/Bare/idle") is True
    assert core.IsAncestor("/Bare/idle", "/Bare/idle") is False
    assert core.is_ancestor("/Bare", "/Bare/idle") is True
    assert core._segments_between("/Bare", "/Bare") == []
    assert core._segments_between("/Bare", "/Bare/parent/child") == [
        "/Bare/parent",
        "/Bare/parent/child",
    ]

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

    assert isinstance(core.Initial("named"), core.PartialInitial)
    assert isinstance(core.Transition("named"), core.PartialTransition)
    assert isinstance(core.Source(core.State("idle")), core.PartialSource)
    assert isinstance(core.Target(core.State("idle")), core.PartialTarget)
    assert isinstance(core.When(lambda *_: None), core.PartialWhen)
    assert isinstance(core.Defer(core.Event(name="later")), core.PartialDefer)
    assert isinstance(
        core.ShallowHistory(core.Transition(core.Target("idle"))),
        core.PartialHistory,
    )
    assert isinstance(
        core.DeepHistory(core.Transition(core.Target("idle"))),
        core.PartialHistory,
    )
    assert isinstance(core.Final(core.State("done")), core.PartialFinal)

    result = subprocess.run(
        [sys.executable, "-m", "hsm.hsm"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "/root/s1" in result.stdout


@pytest.mark.asyncio
async def test_context_waitable_queue_and_instance_branches():
    await core._normalize_waitable(None)

    trigger = asyncio.Event()
    trigger.set()
    await core._normalize_waitable(trigger)
    await core._normalize_waitable(asyncio.sleep(0))

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

    ctx = core.Context()
    seen: list[str] = []

    def failing_listener() -> None:
        seen.append("boom")
        raise RuntimeError("fail")

    def ok_listener() -> None:
        seen.append("ok")

    ctx.add_listener("other", lambda: seen.append("ignored"))
    ctx.add_listener("done", failing_listener)
    ctx.add_listener("done", ok_listener)
    waiting = asyncio.create_task(ctx.wait_done())
    await asyncio.sleep(0)
    ctx.cancel()
    await waiting
    ctx.cancel()
    assert ctx.done is True
    assert seen == ["boom", "ok"]

    immediate: list[str] = []

    def immediate_listener() -> None:
        immediate.append("now")

    ctx.add_listener("done", immediate_listener)
    ctx.add_listener("done", failing_listener)
    assert immediate == ["now"]

    other = core.Context()
    other_listener = lambda: None
    other.add_listener("done", other_listener)
    other.remove_listener("done", other_listener)
    other.remove_listener("other", other_listener)
    other.cancel()
    await other.wait_done()

    class Registered:
        pass

    registered = Registered()
    other.register(registered)  # type: ignore[arg-type]
    assert registered in other.machines()
    other.unregister(registered)  # type: ignore[arg-type]
    other.unregister(registered)  # type: ignore[arg-type]

    queue = core.Queue()
    queue.push(core.Event(name="regular"))
    queue.push(core.Event(name="complete", kind=core.Kinds.CompletionEvent))
    assert queue.len() == 2
    assert (await queue.pop()).name == "complete"
    assert (await queue.pop()).name == "regular"
    assert await queue.pop() is None

    machine_free = CoverageInstance()
    await machine_free.dispatch(core.Event(name="noop"))
    assert machine_free.state() == ""
    assert machine_free.context() is None
    await machine_free.stop()
    await machine_free.restart()

    await core.noop_operation(core.Context(), machine_free, core.Event(name="noop"))
    assert await core.noop_expression(core.Context(), machine_free, core.Event(name="noop")) is True
    assert await core.noop_duration(core.Context(), machine_free, core.Event(name="noop")) == timedelta(
        seconds=0
    )


def test_direct_validation_branches():
    model = _bare_model()
    state = core.StateNode(qualified_name="/Bare/idle")
    model.set(state.qualified_name, state)
    target = core.StateNode(qualified_name="/Bare/other")
    model.set(target.qualified_name, target)

    final_state = core.FinalStateNode(qualified_name="/Bare/final")
    model.set(final_state.qualified_name, final_state)

    with pytest.raises(core.ValidationError, match="state must be called"):
        core.State("orphan").apply(model, [])

    with pytest.raises(core.ValidationError, match="initial must be called within a State"):
        core.Initial(core.Target("idle")).apply(model, [])

    with pytest.raises(core.ValidationError, match="already has an initial state"):
        core.Define(
            "DuplicateInitial",
            core.State(
                "idle",
                core.Initial(core.Target("child")),
                core.Initial(core.Target("child")),
                core.State("child"),
            ),
            core.Initial(core.Target("idle")),
        )

    with pytest.raises(core.ValidationError, match="cannot have a guard"):
        core.Define(
            "GuardedInitial",
            core.Initial(core.Target("idle")),
            core.State(
                "idle",
                core.Initial(core.Guard(_guard_true), core.Target("child")),
                core.State("child"),
            ),
        )

    with pytest.raises(core.ValidationError, match="initial state is required"):
        core.Define("MissingInitialTarget", core.State("idle", core.Initial("missing")))

    with pytest.raises(core.ValidationError, match="must target a nested state"):
        core.Define(
            "WrongInitialTarget",
            core.Initial(core.Target("outer")),
            core.State(
                "outer",
                core.Initial(core.Target("../other")),
                core.State("child"),
            ),
            core.State("other"),
        )

    with pytest.raises(core.ValidationError, match="within a nested State"):
        core.ShallowHistory("memory").apply(model, [model])

    with pytest.raises(core.ValidationError, match="Top level transitions"):
        core.ResolvePaths(
            transition=core.TransitionNode(
                qualified_name="/Bare/toplevel",
                source="/",
                target="/Bare/idle",
            )
        ).apply(model, [])

    with pytest.raises(core.ValidationError, match='Vertex "/Bare/missing" not found'):
        core.ValidateVertex(qualified_name="/Bare/missing").apply(model, [])

    with pytest.raises(core.ValidationError, match="transition must be called"):
        core.Transition(core.On("go")).apply(model, [])

    with pytest.raises(core.ValidationError, match='Source "/Bare/missing" not found'):
        core.Transition(core.Source("/Bare/missing"), core.On("go")).apply(model, [state])

    with pytest.raises(core.ValidationError, match="has no events"):
        core.PartialTransition().apply(model, [state])

    with pytest.raises(core.ValidationError, match="must be called within a hsm.Transition"):
        core.Source("idle").apply(model, [])

    with pytest.raises(core.ValidationError, match="already has a source"):
        core.Source("other").apply(
            model,
            [core.TransitionNode(qualified_name="/Bare/t", source="/Bare/idle")],
        )

    with pytest.raises(core.ValidationError, match='missing source ""'):
        core.Source(_NullPartial()).apply(model, [core.TransitionNode(qualified_name="/Bare/t", source=".")])

    with pytest.raises(core.ValidationError, match="must be called within Transition"):
        core.Target("idle").apply(model, [])

    with pytest.raises(core.ValidationError, match="already has a target"):
        core.Target("other").apply(
            model,
            [core.TransitionNode(qualified_name="/Bare/t", source="/Bare/idle", target="/Bare/other")],
        )

    with pytest.raises(core.ValidationError, match='missing target ""'):
        core.Target(_NullPartial()).apply(model, [core.TransitionNode(qualified_name="/Bare/t")])

    with pytest.raises(core.ValidationError, match="entry must be called within a State"):
        core.Entry(_action).apply(model, [])

    with pytest.raises(core.ValidationError, match="has no missing"):
        core.PartialBehaviors(qualified_name="missing", type=core.StateNode).apply(model, [state])

    with pytest.raises(core.ValidationError, match="guard must be called within a Transition"):
        core.Guard(_guard_true).apply(model, [])

    with pytest.raises(core.ValidationError, match="trigger must be called within a Transition"):
        core.On("go").apply(model, [])

    with pytest.raises(core.ValidationError, match="defer must be called within a state"):
        core.Defer(core.Event(name="later")).apply(model, [])

    with pytest.raises(core.ValidationError, match="choice must be called within a state or transition"):
        core.Choice("branch").apply(model, [])

    with pytest.raises(core.ValidationError, match="choice must be called within a state"):
        core.Choice("branch").apply(model, [core.TransitionNode(qualified_name="/Bare/t", source=".")])

    with pytest.raises(core.ValidationError, match='choice "/NoTransitions/idle/branch" has no transitions'):
        core.Define(
            "NoTransitions",
            core.Initial(core.Target("idle")),
            core.State("idle", core.Choice("branch")),
        )

    with pytest.raises(core.ValidationError, match="cannot have a guard"):
        core.Define(
            "ChoiceGuard",
            core.Initial(core.Target("idle")),
            core.State(
                "idle",
                core.Choice(
                    "branch",
                    core.Transition(core.Guard(_guard_true), core.Target("../done")),
                ),
            ),
            core.State("done"),
        )

    with pytest.raises(core.ValidationError, match='Final state "/Bare/missing" not found'):
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

    with pytest.raises(core.ValidationError, match="Final must be called within a namespace"):
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

    with pytest.raises(core.ValidationError, match="operation must be called within Define"):
        core.Operation("lost").apply(model, [])

    with pytest.raises(core.ValidationError, match="OnSet\\(\\) must be called within a Transition"):
        core.OnSet("count").apply(model, [])

    with pytest.raises(core.ValidationError, match="requires a non-empty attribute name"):
        core.OnSet("").apply(model, [core.TransitionNode(qualified_name="/Bare/t")])

    with pytest.raises(core.ValidationError, match="OnCall\\(\\) must be called within a Transition"):
        core.OnCall("work").apply(model, [])

    with pytest.raises(core.ValidationError, match="requires a non-empty operation name"):
        core.OnCall("").apply(model, [core.TransitionNode(qualified_name="/Bare/t")])

    with pytest.raises(core.ValidationError, match='Source "/Bare/missing" not found'):
        core.TimedBehavior(
            event=core.Event(name="timer"),
            duration=_action,  # type: ignore[arg-type]
            transition=core.TransitionNode(
                qualified_name="/Bare/timer",
                source="/Bare/missing",
            ),
        ).apply(model, [])

    with pytest.raises(core.ValidationError, match="after must be called within a Transition"):
        core.After(_action).apply(model, [])  # type: ignore[arg-type]

    with pytest.raises(core.ValidationError, match="when must be called within a Transition"):
        core.When(lambda *_: None).apply(model, [])

    initial = core.InitialNode(qualified_name="/Bare/.initial")
    model.set(initial.qualified_name, initial)
    with pytest.raises(core.ValidationError, match="source is a State"):
        core.When(lambda *_: None).apply(
            model,
            [core.TransitionNode(qualified_name="/Bare/t", source=initial.qualified_name)],
        )


def test_model_finalization_validation_branches():
    with pytest.raises(core.ValidationError, match="entry actions are not allowed"):
        core.Define(
            "TopEntry",
            core.Initial(core.Target("idle")),
            core.Entry(_action),
            core.State("idle"),
        )

    with pytest.raises(core.ValidationError, match="exit actions are not allowed"):
        core.Define(
            "TopExit",
            core.Initial(core.Target("idle")),
            core.Exit(_action),
            core.State("idle"),
        )

    with pytest.raises(core.ValidationError, match='missing operation "work" for OnCall'):
        core.Define(
            "PendingOnCall",
            core.Initial(core.Target("idle")),
            core.State(
                "idle",
                core.Transition(core.OnCall("work"), core.Target("../done")),
            ),
            core.State("done"),
        )


@pytest.mark.asyncio
async def test_runtime_wrapper_group_and_call_edge_branches():
    async def activity_done(
        ctx: core.Context, instance: CoverageInstance, event: core.Event
    ) -> None:
        return None

    model = core.Define(
        "RuntimeCoverage",
        core.Attribute("count", 1),
        core.Operation("double"),
        core.Operation("missing_method"),
        core.Initial(core.Target("idle")),
        core.State(
            "idle",
            core.Activity(activity_done),
            core.Transition(core.On("go"), core.Target("../done")),
            core.Transition(core.OnSet("count"), core.Target("../set_state")),
            core.Transition(core.OnCall("double"), core.Target("../called")),
        ),
        core.State("called", core.Transition(core.On("reset"), core.Target("../idle"))),
        core.State("set_state", core.Transition(core.On("reset"), core.Target("../idle"))),
        core.State("done"),
    )

    ctx = core.Context()
    first = CoverageInstance()
    second = CoverageInstance()
    first_hsm = await core.Started(ctx, first, model)
    second_hsm = await core.Start(ctx, second, model)

    assert first_hsm.context() is ctx
    delattr(first_hsm, "id")
    delattr(first_hsm, "qualified_name")
    assert first_hsm.id().startswith("hsm-")
    assert first_hsm.qualified_name() == "/RuntimeCoverage"
    assert core.QualifiedName(first) == "/RuntimeCoverage"
    assert core.AfterExecuted(ctx, first, "/RuntimeCoverage/idle").done() is False

    missing, ok = core.Get(ctx, first, "missing")
    assert (missing, ok) == (None, False)

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

    with pytest.raises(core.ValidationError, match='missing operation "missing_method"'):
        await first_hsm.call("missing_method")

    ghost = core.TransitionNode(qualified_name="/RuntimeCoverage/ghost", target="/RuntimeCoverage/done")
    first_hsm.model.transition_map[first.state()].setdefault("ghost", []).append(ghost)
    snapshot = core.TakeSnapshot(ctx, first)
    assert snapshot.State == "/RuntimeCoverage/idle"
    assert snapshot.QueueLen == 0
    assert all(event.Name != "ghost" for event in snapshot.Events)

    group = core.NewGroup(first, core.NewGroup(second), None)
    assert group.state() == "/RuntimeCoverage/idle"
    assert group.context() is ctx
    assert core.TakeSnapshot(None, group).QualifiedName == ""

    await core.Dispatch(None, group, core.Event(name="go"))
    assert first.state() == "/RuntimeCoverage/done"
    assert second.state() == "/RuntimeCoverage/done"

    await core.Restart(group)
    assert first.state() == "/RuntimeCoverage/idle"
    assert second.state() == "/RuntimeCoverage/idle"

    await core.Set(None, group, "count", 3)
    assert first.state() == "/RuntimeCoverage/set_state"
    assert second.state() == "/RuntimeCoverage/set_state"

    await core.Dispatch(None, group, core.Event(name="reset"))
    assert await core.Call(None, group, "double", 5) == 10
    assert first.values[-1] == 5
    assert second.values == []

    await core.Stop(group)
    assert first.state() == "/RuntimeCoverage"
    assert second.state() == "/RuntimeCoverage"

    empty_group = core.NewGroup(None)
    assert empty_group.state() == ""
    assert empty_group.context() is None
    assert empty_group.get("count") == (None, False)
    empty_snapshot = empty_group.take_snapshot()
    assert empty_snapshot.ID != ""
    assert core.QualifiedName(empty_group) == ""
    await core.Dispatch(None, empty_group, core.Event(name="noop"))
    await core.Restart(empty_group)
    await core.Stop(empty_group)
    with pytest.raises(core.ValidationError, match="missing hsm"):
        await core.Call(None, empty_group, "double", 1)

    await core.DispatchAll(None, core.Event(name="noop"))
    await core.DispatchTo(None, core.Event(name="noop"), "hsm-*")

    with pytest.raises(core.ValidationError, match="missing hsm"):
        core.TakeSnapshot(None, CoverageInstance())
