import asyncio
import time
from datetime import timedelta

import pytest

import hsm


class SubmachineInstance(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.log: list[str] = []


def _record(value: str):
    def callback(ctx, inst: SubmachineInstance, event):
        inst.log.append(value)

    return callback


def _motor_model() -> hsm.Model:
    return hsm.Define(
        "Motor",
        hsm.EntryPoint("cold", hsm.Target("off")),
        hsm.EntryPoint("resume", hsm.Target("running"), hsm.Effect(_record("motor.resume"))),
        hsm.ExitPoint("faulted", hsm.Effect(_record("motor.faulted"))),
        hsm.Initial(hsm.Target("off")),
        hsm.State("off", hsm.Transition(hsm.On("start"), hsm.Target("../running"))),
        hsm.State(
            "running",
            hsm.Entry(_record("motor.running")),
            hsm.Transition(hsm.On("fault"), hsm.Target("../faulted")),
            hsm.Transition(hsm.On("finish"), hsm.Target("../done")),
        ),
        hsm.Final("done"),
    )


@pytest.mark.asyncio
async def test_submachine_entry_point_bottom_up_event_and_exit_point():
    controller = hsm.Define(
        "Controller",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("enable"),
                hsm.Target("../drive"),
                hsm.EntryPoint("resume"),
                hsm.Effect(_record("controller.enable")),
            ),
        ),
        hsm.SubmachineState(
            "drive",
            _motor_model(),
            hsm.Transition(
                hsm.ExitPoint("faulted"),
                hsm.Target("../fault"),
                hsm.Effect(_record("controller.faulted")),
            ),
            hsm.Transition(
                hsm.On(hsm.FinalEvent),
                hsm.Target("../idle"),
                hsm.Effect(_record("controller.done")),
            ),
        ),
        hsm.State("fault", hsm.Entry(_record("controller.fault"))),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, controller)

    await hsm.Dispatch(ctx, instance, hsm.Event("enable"))
    assert instance.state() == "/Controller/drive/running"
    assert instance.log == [
        "controller.enable",
        "motor.resume",
        "motor.running",
    ]

    await hsm.Dispatch(ctx, instance, hsm.Event("fault"))
    assert instance.state() == "/Controller/fault"
    assert instance.log == [
        "controller.enable",
        "motor.resume",
        "motor.running",
        "motor.faulted",
        "controller.faulted",
        "controller.fault",
    ]


@pytest.mark.asyncio
async def test_submachine_unhandled_exit_point_restores_child_leaf_before_error():
    child = hsm.Define(
        "UnhandledExitPointChild",
        hsm.ExitPoint("done"),
        hsm.Initial(hsm.Target("inside")),
        hsm.State("inside", hsm.Transition(hsm.On("finish"), hsm.Target("../done"))),
    )
    parent = hsm.Define(
        "UnhandledExitPointParent",
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState("drive", child),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, parent)

    with pytest.raises(RuntimeError, match='unhandled exit point "done"'):
        await hsm.Dispatch(ctx, instance, hsm.Event("finish"))

    assert instance.state() == "/UnhandledExitPointParent/drive/inside"


@pytest.mark.asyncio
async def test_submachine_exit_point_effect_error_short_circuits_at_boundary():
    async def fail(ctx, inst: SubmachineInstance, event):
        inst.log.append("exit:effect")
        raise RuntimeError("exit boom")

    child = hsm.Define(
        "ExitPointEffectErrorChild",
        hsm.ExitPoint("done", hsm.Effect(fail)),
        hsm.Initial(hsm.Target("inside")),
        hsm.State("inside", hsm.Transition(hsm.On("finish"), hsm.Target("../done"))),
    )
    parent = hsm.Define(
        "ExitPointEffectErrorParent",
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState(
            "drive",
            child,
            hsm.Transition(hsm.ExitPoint("done"), hsm.Target("../complete")),
        ),
        hsm.State("complete", hsm.Entry(_record("entry:complete"))),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, parent)

    with pytest.raises(RuntimeError, match="exit boom"):
        await hsm.Dispatch(ctx, instance, hsm.Event("finish"))

    assert instance.state() == "/ExitPointEffectErrorParent/drive"
    assert instance.log == ["exit:effect"]


@pytest.mark.asyncio
async def test_submachine_entry_point_effect_error_short_circuits_at_boundary():
    async def fail(ctx, inst: SubmachineInstance, event):
        inst.log.append("entry-point:effect")
        raise RuntimeError("entry point boom")

    child = hsm.Define(
        "EntryPointEffectErrorChild",
        hsm.EntryPoint("warm", hsm.Target("running"), hsm.Effect(fail)),
        hsm.Initial(hsm.Target("cold")),
        hsm.State("cold"),
        hsm.State("running", hsm.Entry(_record("entry:running"))),
    )
    parent = hsm.Define(
        "EntryPointEffectErrorParent",
        hsm.Initial(hsm.Target("outside")),
        hsm.State(
            "outside",
            hsm.Transition(
                hsm.On("go"),
                hsm.Target("../drive"),
                hsm.EntryPoint("warm"),
            ),
        ),
        hsm.SubmachineState("drive", child),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, parent)

    with pytest.raises(RuntimeError, match="entry point boom"):
        await hsm.Dispatch(ctx, instance, hsm.Event("go"))

    assert instance.state() == "/EntryPointEffectErrorParent/drive"
    assert instance.log == ["entry-point:effect"]


def test_entry_point_guard_is_rejected_at_build_time():
    with pytest.raises(hsm.ValidationError, match="cannot have a guard"):
        hsm.Define(
            "EntryPointGuardRejected",
            hsm.EntryPoint(
                "warm",
                hsm.Target("running"),
                hsm.Guard(lambda ctx, inst, event: True),
            ),
            hsm.Initial(hsm.Target("cold")),
            hsm.State("cold"),
            hsm.State("running"),
        )


def test_submachine_internal_entry_point_target_is_rejected_at_build_time():
    with pytest.raises(hsm.ValidationError, match="entry point target cannot be internal"):
        hsm.Define(
            "InternalEntryPointChild",
            hsm.EntryPoint("start", hsm.Target("b")),
            hsm.Initial(hsm.Target("a")),
            hsm.State("a", hsm.Transition(hsm.On("go"), hsm.Target("../start"))),
            hsm.State("b"),
        )


@pytest.mark.asyncio
async def test_nested_submachine_exit_point_falls_through_to_ancestor_handler():
    async def deny(ctx, inst: SubmachineInstance, event):
        inst.log.append("guard:middle")
        return False

    inner = hsm.Define(
        "NestedExitFallthroughInner",
        hsm.ExitPoint("done"),
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Transition(hsm.On("finish"), hsm.Target("../done"))),
    )
    outer = hsm.Define(
        "NestedExitFallthroughOuter",
        hsm.Initial(hsm.Target("inner")),
        hsm.SubmachineState(
            "inner",
            inner,
            hsm.Transition(
                hsm.ExitPoint("done"),
                hsm.Target("../wrong"),
                hsm.Guard(deny),
            ),
        ),
        hsm.State("wrong"),
    )
    parent = hsm.Define(
        "NestedExitFallthroughParent",
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState(
            "drive",
            outer,
            hsm.Transition(
                hsm.ExitPoint("done"),
                hsm.Target("../complete"),
                hsm.Effect(_record("effect:parent")),
            ),
        ),
        hsm.State("complete", hsm.Entry(_record("entry:complete"))),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, parent)
    await hsm.Dispatch(ctx, instance, hsm.Event("finish"))

    assert instance.state() == "/NestedExitFallthroughParent/complete"
    assert instance.log == ["guard:middle", "effect:parent", "entry:complete"]


@pytest.mark.asyncio
async def test_source_qualified_exit_point_handler_precedes_local_fallthrough_handler():
    async def guard_false(ctx, inst: SubmachineInstance, event):
        inst.log.append("guard:root")
        return False

    inner = hsm.Define(
        "SourceQualifiedExitPriorityInner",
        hsm.ExitPoint("done"),
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Transition(hsm.On("finish"), hsm.Target("../done"))),
    )
    child = hsm.Define(
        "SourceQualifiedExitPriorityChild",
        hsm.Initial(hsm.Target("inner")),
        hsm.SubmachineState(
            "inner",
            inner,
            hsm.Transition(
                hsm.ExitPoint("done"),
                hsm.Target("../local"),
                hsm.Effect(_record("effect:local")),
            ),
        ),
        hsm.State("local"),
        hsm.State("wrong"),
        hsm.Transition(
            hsm.Source("inner"),
            hsm.ExitPoint("done"),
            hsm.Target("wrong"),
            hsm.Guard(guard_false),
        ),
    )
    parent = hsm.Define(
        "SourceQualifiedExitPriorityParent",
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState("drive", child),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, parent)
    await hsm.Dispatch(ctx, instance, hsm.Event("finish"))

    assert instance.state() == "/SourceQualifiedExitPriorityParent/drive/local"
    assert instance.log == ["guard:root", "effect:local"]


@pytest.mark.asyncio
async def test_child_deferred_event_replays_after_exit_point_handler_target_entry():
    child = hsm.Define(
        "ExitPointDeferredReplayChild",
        hsm.ExitPoint("done", hsm.Effect(_record("exit:done"))),
        hsm.Initial(hsm.Target("active")),
        hsm.State(
            "active",
            hsm.Defer("noise"),
            hsm.Transition(
                hsm.On("finish"),
                hsm.Target("../done"),
                hsm.Effect(_record("effect:finish")),
            ),
        ),
    )
    parent = hsm.Define(
        "ExitPointDeferredReplayParent",
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState(
            "drive",
            child,
            hsm.Transition(
                hsm.ExitPoint("done"),
                hsm.Target("../complete"),
                hsm.Effect(_record("effect:exit-handler")),
            ),
        ),
        hsm.State(
            "complete",
            hsm.Entry(_record("entry:complete")),
            hsm.Transition(
                hsm.On("noise"),
                hsm.Target("../after_noise"),
                hsm.Effect(_record("effect:noise")),
            ),
        ),
        hsm.State("after_noise", hsm.Entry(_record("entry:after-noise"))),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, parent)

    await hsm.Dispatch(ctx, instance, hsm.Event("noise"))
    await hsm.Dispatch(ctx, instance, hsm.Event("finish"))

    assert instance.state() == "/ExitPointDeferredReplayParent/after_noise"
    assert instance.log == [
        "effect:finish",
        "exit:done",
        "effect:exit-handler",
        "entry:complete",
        "effect:noise",
        "entry:after-noise",
    ]


@pytest.mark.asyncio
async def test_submachine_final_completion_bubbles_to_containing_state():
    controller = hsm.Define(
        "ControllerDone",
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState(
            "drive",
            _motor_model(),
            hsm.Transition(
                hsm.On(hsm.FinalEvent),
                hsm.Target("../idle"),
                hsm.Effect(_record("controller.done")),
            ),
        ),
        hsm.State("idle", hsm.Entry(_record("controller.idle"))),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, controller)
    assert instance.state() == "/ControllerDone/drive/off"

    await hsm.Dispatch(ctx, instance, hsm.Event("start"))
    assert instance.state() == "/ControllerDone/drive/running"

    await hsm.Dispatch(ctx, instance, hsm.Event("finish"))
    assert instance.state() == "/ControllerDone/idle"
    assert instance.log[-2:] == ["controller.done", "controller.idle"]


def test_submachine_aliases_are_exported():
    assert hsm.submachine_state is hsm.SubmachineState
    assert hsm.entry_point is hsm.EntryPoint
    assert hsm.exit_point is hsm.ExitPoint
    assert hsm.submachine_state_kind is hsm.SubmachineStateKind
    assert hsm.exit_point_kind is hsm.ExitPointKind


@pytest.mark.asyncio
async def test_submachine_child_on_call_precedes_containing_transition():
    async def approve(ctx, inst: SubmachineInstance) -> str:
        inst.log.append("operation:approve")
        return "approved"

    child = hsm.Define(
        "OnCallChildPrecedence",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(
                hsm.OnCall("approve"),
                hsm.Target("../approved"),
                hsm.Effect(_record("effect:child")),
            ),
        ),
        hsm.State("approved", hsm.Entry(_record("entry:approved"))),
    )
    parent = hsm.Define(
        "OnCallParentPrecedence",
        hsm.Operation("approve", approve),
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState(
            "drive",
            child,
            hsm.Transition(
                hsm.OnCall("approve"),
                hsm.Target("../parent_handled"),
                hsm.Effect(_record("effect:parent")),
            ),
        ),
        hsm.State("parent_handled"),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, parent)
    result = await hsm.Call(ctx, instance, "approve")

    assert result == "approved"
    assert instance.state() == "/OnCallParentPrecedence/drive/approved"
    assert instance.log == ["effect:child", "entry:approved", "operation:approve"]


@pytest.mark.asyncio
async def test_submachine_timer_uses_remapped_event_and_parent_clock():
    sleeps: list[timedelta] = []

    async def sleep(duration: timedelta) -> None:
        sleeps.append(duration)
        await asyncio.sleep(0)

    child = hsm.Define(
        "TimerChild",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(
                hsm.After(lambda ctx, inst, event: timedelta(milliseconds=5)),
                hsm.Target("../timeout"),
                hsm.Effect(_record("effect:timeout")),
            ),
        ),
        hsm.State("timeout", hsm.Entry(_record("entry:timeout"))),
    )
    parent = hsm.Define(
        "TimerParent",
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState("drive", child),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, parent, hsm.Config(Clock=hsm.Clock(sleep=sleep)))
    await asyncio.sleep(0.01)

    assert sleeps == [timedelta(milliseconds=5)]
    assert instance.state() == "/TimerParent/drive/timeout"
    assert instance.log == ["effect:timeout", "entry:timeout"]


@pytest.mark.asyncio
async def test_submachine_stale_timer_event_is_dropped_after_parent_exit():
    release_sleep = asyncio.Event()
    slept = asyncio.Event()

    async def sleep(duration: timedelta) -> None:
        slept.set()
        await release_sleep.wait()

    child = hsm.Define(
        "CancellableTimerChild",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(
                hsm.After(lambda ctx, inst, event: timedelta(milliseconds=5)),
                hsm.Target("../timeout"),
                hsm.Effect(_record("effect:timeout")),
            ),
        ),
        hsm.State("timeout"),
    )
    parent = hsm.Define(
        "CancellableTimerParent",
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState(
            "drive",
            child,
            hsm.Transition(
                hsm.On("leave"),
                hsm.Target("../outside"),
                hsm.Effect(_record("effect:leave")),
            ),
        ),
        hsm.State("outside"),
    )

    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, parent, hsm.Config(Clock=hsm.Clock(sleep=sleep)))
    await asyncio.wait_for(slept.wait(), timeout=1)
    release_sleep.set()
    await hsm.Dispatch(ctx, instance, hsm.Event("leave"))
    await asyncio.sleep(0)

    assert instance.state() == "/CancellableTimerParent/outside"
    assert instance.log == ["effect:leave"]


def _plain_nested_toggle_model() -> hsm.Model:
    return hsm.Define(
        "PlainNestedPerf",
        hsm.Initial(hsm.Target("drive/a")),
        hsm.State(
            "drive",
            hsm.Initial(hsm.Target("a")),
            hsm.State("a", hsm.Transition(hsm.On("flip"), hsm.Target("../b"))),
            hsm.State("b", hsm.Transition(hsm.On("flip"), hsm.Target("../a"))),
        ),
    )


def _submachine_toggle_model() -> hsm.Model:
    child = hsm.Define(
        "ToggleChild",
        hsm.Initial(hsm.Target("a")),
        hsm.State("a", hsm.Transition(hsm.On("flip"), hsm.Target("../b"))),
        hsm.State("b", hsm.Transition(hsm.On("flip"), hsm.Target("../a"))),
    )
    return hsm.Define(
        "SubmachinePerf",
        hsm.Initial(hsm.Target("drive")),
        hsm.SubmachineState("drive", child),
    )


async def _time_dispatches(model: hsm.Model, iterations: int) -> float:
    instance = SubmachineInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)
    event = hsm.Event("flip")
    started = time.perf_counter()
    for _ in range(iterations):
        await hsm.Dispatch(ctx, instance, event)
    elapsed = time.perf_counter() - started
    await hsm.Stop(instance)
    return elapsed


def test_submachine_dispatch_performance_matches_nested_state_shape():
    iterations = 400

    async def run():
        plain = await _time_dispatches(_plain_nested_toggle_model(), iterations)
        submachine = await _time_dispatches(_submachine_toggle_model(), iterations)
        return plain, submachine

    plain, submachine = asyncio.run(run())
    assert submachine <= max(plain * 3.0, plain + 0.05)
