import asyncio
from dataclasses import dataclass
from datetime import timedelta

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import hsm


class FuzzInstance(hsm.Instance):
    pass


@dataclass(frozen=True)
class FlatSpec:
    transitions: tuple[tuple[int | None, ...], ...]
    trace: tuple[int, ...]

    @property
    def state_count(self) -> int:
        return len(self.transitions)

    @property
    def event_count(self) -> int:
        return len(self.transitions[0])


@st.composite
def flat_specs(draw):
    state_count = draw(st.integers(min_value=1, max_value=8))
    event_count = draw(st.integers(min_value=1, max_value=5))
    target = st.one_of(st.none(), st.integers(min_value=0, max_value=state_count - 1))
    transitions = tuple(
        tuple(draw(st.lists(target, min_size=event_count, max_size=event_count)))
        for _ in range(state_count)
    )
    trace = tuple(
        draw(st.lists(st.integers(min_value=0, max_value=event_count - 1), min_size=0, max_size=80))
    )
    return FlatSpec(transitions=transitions, trace=trace)


def _flat_model(spec: FlatSpec) -> hsm.Model:
    states = []
    for state_index, row in enumerate(spec.transitions):
        transitions = []
        for event_index, target_index in enumerate(row):
            if target_index is None:
                continue
            transitions.append(
                hsm.Transition(
                    hsm.On(f"e{event_index}"),
                    hsm.Target(f"../s{target_index}"),
                )
            )
        states.append(hsm.State(f"s{state_index}", *transitions))
    return hsm.Define("FuzzFlat", hsm.Initial(hsm.Target("s0")), *states)


async def _drive_flat_spec(spec: FlatSpec) -> None:
    instance = FuzzInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, _flat_model(spec))

    expected_state = 0
    assert instance.state() == "/FuzzFlat/s0"

    for event_index in spec.trace:
        before = hsm.TakeSnapshot(ctx, instance)
        assert before.State == f"/FuzzFlat/s{expected_state}"
        assert before.QueueLen == 0

        await hsm.Dispatch(ctx, instance, hsm.Event(f"e{event_index}"))

        maybe_target = spec.transitions[expected_state][event_index]
        if maybe_target is not None:
            expected_state = maybe_target
        after = hsm.TakeSnapshot(ctx, instance)
        assert after.State == f"/FuzzFlat/s{expected_state}"
        assert after.QueueLen == 0
        assert instance.state() == after.State

    await hsm.Stop(instance)
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0


@given(flat_specs())
@settings(
    max_examples=120,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_flat_state_machine_fuzz(spec: FlatSpec):
    asyncio.run(_drive_flat_spec(spec))


@st.composite
def guarded_specs(draw):
    guard_count = draw(st.integers(min_value=1, max_value=8))
    guards = tuple(draw(st.lists(st.booleans(), min_size=guard_count, max_size=guard_count)))
    targets = tuple(
        draw(st.lists(st.integers(min_value=1, max_value=4), min_size=guard_count, max_size=guard_count))
    )
    return guards, targets


def _guarded_model(guards: tuple[bool, ...], targets: tuple[int, ...]) -> hsm.Model:
    def make_guard(value: bool):
        async def guard(ctx, inst, event):
            return value

        return guard

    transitions = [
        hsm.Transition(
            hsm.On("go"),
            hsm.Guard(make_guard(guard_value)),
            hsm.Target(f"../s{target_index}"),
        )
        for guard_value, target_index in zip(guards, targets, strict=True)
    ]
    return hsm.Define(
        "FuzzGuards",
        hsm.Initial(hsm.Target("s0")),
        hsm.State("s0", *transitions),
        *(hsm.State(f"s{index}") for index in range(1, 5)),
    )


async def _drive_guarded_spec(guards: tuple[bool, ...], targets: tuple[int, ...]) -> None:
    instance = FuzzInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, _guarded_model(guards, targets))

    await hsm.Dispatch(ctx, instance, hsm.Event("go"))

    expected = "s0"
    for guard_value, target_index in zip(guards, targets, strict=True):
        if guard_value:
            expected = f"s{target_index}"
            break
    assert instance.state() == f"/FuzzGuards/{expected}"
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    await hsm.Stop(instance)


@given(guarded_specs())
@settings(max_examples=80, deadline=None, derandomize=True)
def test_guard_order_fuzz(spec):
    guards, targets = spec
    asyncio.run(_drive_guarded_spec(guards, targets))


async def _dispatch_toggle_stress(iterations: int) -> None:
    instance = FuzzInstance()
    ctx = hsm.Context()
    model = hsm.Define(
        "StressToggle",
        hsm.Initial(hsm.Target("off")),
        hsm.State("off", hsm.Transition(hsm.On("toggle"), hsm.Target("../on"))),
        hsm.State("on", hsm.Transition(hsm.On("toggle"), hsm.Target("../off"))),
    )
    await hsm.Start(ctx, instance, model)

    await asyncio.gather(
        *(hsm.Dispatch(ctx, instance, hsm.Event("toggle")) for _ in range(iterations))
    )

    expected = "off" if iterations % 2 == 0 else "on"
    assert instance.state() == f"/StressToggle/{expected}"
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    await hsm.Stop(instance)


def test_concurrent_dispatch_stress():
    asyncio.run(_dispatch_toggle_stress(1000))


async def _broadcast_stress(machine_count: int, rounds: int) -> None:
    ctx = hsm.Context()
    model = hsm.Define(
        "BroadcastStress",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../busy"))),
        hsm.State("busy", hsm.Transition(hsm.On("reset"), hsm.Target("../idle"))),
    )
    instances = [FuzzInstance() for _ in range(machine_count)]
    for index, instance in enumerate(instances):
        await hsm.Started(ctx, instance, model, hsm.Config(ID=f"machine-{index}"))

    for _ in range(rounds):
        await hsm.DispatchAll(ctx, hsm.Event("go"))
        assert all(instance.state() == "/BroadcastStress/busy" for instance in instances)
        await hsm.DispatchTo(ctx, hsm.Event("reset"), "machine-*")
        assert all(instance.state() == "/BroadcastStress/idle" for instance in instances)
        assert all(hsm.TakeSnapshot(ctx, instance).QueueLen == 0 for instance in instances)

    await asyncio.gather(*(hsm.Stop(instance) for instance in instances))


def test_dispatch_all_and_dispatch_to_stress():
    asyncio.run(_broadcast_stress(machine_count=50, rounds=25))


@given(st.lists(st.integers(min_value=-5, max_value=5), min_size=0, max_size=80))
@settings(max_examples=80, deadline=None, derandomize=True)
def test_attribute_set_event_fuzz(values: list[int]):
    asyncio.run(_drive_attribute_set_trace(tuple(values)))


async def _drive_attribute_set_trace(values: tuple[int, ...]) -> None:
    class AttributeInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.changes: list[tuple[int | None, int]] = []

    async def record_change(ctx, inst: AttributeInstance, event: hsm.Event) -> None:
        change = event.Data
        inst.changes.append((change.old_value, change.value))

    model = hsm.Define(
        "SetFuzz",
        hsm.Attribute("count", 0),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.OnSet("count"),
                hsm.Effect(record_change),
            ),
        ),
    )
    instance = AttributeInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    expected_changes: list[tuple[int | None, int]] = []
    current = 0
    for value in values:
        await hsm.Set(ctx, instance, "count", value)
        if value != current:
            expected_changes.append((current, value))
            current = value
        observed, ok = hsm.Get(ctx, instance, "count")
        assert ok is True
        assert observed == value
        assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0

    assert instance.changes == expected_changes
    await hsm.Stop(instance)


async def _set_call_concurrency_stress(rounds: int) -> None:
    class RuntimeInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.changed = 0
            self.called = 0
            self.calls: list[int] = []

    async def add(ctx, inst: RuntimeInstance, value: int) -> int:
        await asyncio.sleep(0)
        inst.calls.append(value)
        return value + 1

    async def record_set(ctx, inst: RuntimeInstance, event: hsm.Event) -> None:
        inst.changed += 1

    async def record_call(ctx, inst: RuntimeInstance, event: hsm.Event) -> None:
        inst.called += 1

    model = hsm.Define(
        "SetCallStress",
        hsm.Attribute("value", 0),
        hsm.Operation("add", add),
        hsm.Initial(hsm.Target("ready")),
        hsm.State(
            "ready",
            hsm.Transition(hsm.OnSet("value"), hsm.Effect(record_set)),
            hsm.Transition(hsm.OnCall("add"), hsm.Effect(record_call)),
        ),
    )
    instance = RuntimeInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    async def set_value(value: int) -> None:
        await hsm.Set(ctx, instance, "value", value)

    async def call_add(value: int) -> int:
        return await hsm.Call(ctx, instance, "add", value)

    results = await asyncio.gather(
        *(set_value(index) for index in range(rounds)),
        *(call_add(index) for index in range(rounds)),
    )

    call_results = results[rounds:]
    assert sorted(call_results) == list(range(1, rounds + 1))
    assert sorted(instance.calls) == list(range(rounds))
    assert instance.changed == max(rounds - 1, 0)
    assert instance.called == rounds
    assert instance.state() == "/SetCallStress/ready"
    snapshot = hsm.TakeSnapshot(ctx, instance)
    assert snapshot.QueueLen == 0
    assert snapshot.Attributes["/SetCallStress/value"] == rounds - 1
    await hsm.Stop(instance)


def test_concurrent_set_and_call_stress():
    asyncio.run(_set_call_concurrency_stress(rounds=200))


async def _activity_cancellation_stress(rounds: int) -> None:
    class ActivityInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.started = 0
            self.cancelled = 0

    async def activity(ctx: hsm.Context, inst: ActivityInstance, event: hsm.Event) -> None:
        inst.started += 1
        try:
            while not ctx.done:
                await asyncio.sleep(0)
        finally:
            inst.cancelled += 1

    model = hsm.Define(
        "ActivityStress",
        hsm.Initial(hsm.Target("active")),
        hsm.State(
            "active",
            hsm.Activity(activity),
            hsm.Transition(hsm.On("finish"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )

    for _ in range(rounds):
        instance = ActivityInstance()
        ctx = hsm.Context()
        sm = await hsm.Started(ctx, instance, model)
        for _ in range(10):
            if instance.started:
                break
            await asyncio.sleep(0)

        await hsm.Dispatch(ctx, instance, hsm.Event("finish"))

        assert instance.state() == "/ActivityStress/done"
        assert instance.started == 1
        assert instance.cancelled == 1
        assert sm._active == {}
        assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
        await hsm.Stop(instance)
        assert sm._active == {}


def test_activity_cancellation_stress():
    asyncio.run(_activity_cancellation_stress(rounds=100))


async def _deferred_event_replay_stress(rounds: int) -> None:
    instance = FuzzInstance()
    ctx = hsm.Context()
    model = hsm.Define(
        "DeferredStress",
        hsm.Initial(hsm.Target("holding")),
        hsm.State(
            "holding",
            hsm.Defer(hsm.Event("work")),
            hsm.Transition(hsm.On("release"), hsm.Target("../processing")),
        ),
        hsm.State(
            "processing",
            hsm.Transition(hsm.On("work"), hsm.Target("../done")),
        ),
        hsm.State("done", hsm.Transition(hsm.On("reset"), hsm.Target("../holding"))),
    )
    await hsm.Start(ctx, instance, model)

    for _ in range(rounds):
        await hsm.Dispatch(ctx, instance, hsm.Event("work"))
        assert instance.state() == "/DeferredStress/holding"
        assert hsm.TakeSnapshot(ctx, instance).QueueLen == 1

        await hsm.Dispatch(ctx, instance, hsm.Event("release"))

        assert instance.state() == "/DeferredStress/done"
        assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0

        await hsm.Dispatch(ctx, instance, hsm.Event("reset"))
        assert instance.state() == "/DeferredStress/holding"

    await hsm.Stop(instance)


def test_deferred_events_replay_without_extra_dispatch():
    asyncio.run(_deferred_event_replay_stress(rounds=100))


@st.composite
def deferred_traces(draw):
    return tuple(draw(st.lists(st.sampled_from(("work", "release", "reset")), min_size=0, max_size=80)))


async def _drive_deferred_trace(trace: tuple[str, ...]) -> None:
    instance = FuzzInstance()
    ctx = hsm.Context()
    model = hsm.Define(
        "DeferredFuzz",
        hsm.Initial(hsm.Target("holding")),
        hsm.State(
            "holding",
            hsm.Defer(hsm.Event("work")),
            hsm.Transition(hsm.On("release"), hsm.Target("../processing")),
        ),
        hsm.State(
            "processing",
            hsm.Transition(hsm.On("work"), hsm.Target("../done")),
            hsm.Transition(hsm.On("reset"), hsm.Target("../holding")),
        ),
        hsm.State("done", hsm.Transition(hsm.On("reset"), hsm.Target("../holding"))),
    )
    await hsm.Start(ctx, instance, model)

    expected = "holding"
    deferred_work = 0
    for event_name in trace:
        await hsm.Dispatch(ctx, instance, hsm.Event(event_name))
        if expected == "holding":
            if event_name == "work":
                deferred_work += 1
            elif event_name == "release":
                expected = "done" if deferred_work else "processing"
                deferred_work = 0
        elif expected == "processing":
            if event_name == "work":
                expected = "done"
            elif event_name == "reset":
                expected = "holding"
        elif expected == "done" and event_name == "reset":
            expected = "holding"

        snapshot = hsm.TakeSnapshot(ctx, instance)
        assert snapshot.State == f"/DeferredFuzz/{expected}"
        assert snapshot.QueueLen == deferred_work

    await hsm.Stop(instance)


@given(deferred_traces())
@settings(max_examples=80, deadline=None, derandomize=True)
def test_deferred_event_fuzz(trace: tuple[str, ...]):
    asyncio.run(_drive_deferred_trace(trace))


@given(
    st.lists(
        st.sampled_from(("missing_initial", "bad_initial_target", "missing_transition_target")),
        min_size=1,
        max_size=20,
    )
)
@settings(max_examples=60, deadline=None, derandomize=True)
def test_invalid_model_definitions_fail_fast(cases: list[str]):
    for index, case in enumerate(cases):
        if case == "missing_initial":
            builder = lambda: hsm.Define(f"Invalid{index}", hsm.State("idle"))
            message = "initial state is required"
        elif case == "bad_initial_target":
            builder = lambda: hsm.Define(
                f"Invalid{index}",
                hsm.Initial(hsm.Target("missing")),
                hsm.State("idle"),
            )
            message = "Vertex"
        else:
            builder = lambda: hsm.Define(
                f"Invalid{index}",
                hsm.Initial(hsm.Target("idle")),
                hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../missing"))),
            )
            message = "Vertex"

        try:
            builder()
        except hsm.ValidationError as error:
            assert message in str(error)
        else:
            raise AssertionError(f"{case} unexpectedly built a model")


@given(
    st.sampled_from(("after", "every", "at")),
    st.one_of(st.none(), st.integers(), st.text(max_size=20), st.booleans()),
)
@settings(max_examples=60, deadline=None, derandomize=True)
def test_invalid_timer_callback_results_dispatch_error(mode: str, invalid_value: object):
    asyncio.run(_drive_invalid_timer_callback(mode, invalid_value))


async def _drive_invalid_timer_callback(mode: str, invalid_value: object) -> None:
    class TimerErrorInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.error: Exception | None = None

    async def invalid_timer(ctx, inst, event):
        return invalid_value

    async def error_effect(ctx, inst: TimerErrorInstance, event: hsm.Event) -> None:
        inst.error = event.Data

    if mode == "after":
        timer = hsm.After(invalid_timer)
        expected = "After()/Every() duration must return timedelta"
    elif mode == "every":
        timer = hsm.Every(invalid_timer)
        expected = "After()/Every() duration must return timedelta"
    else:
        timer = hsm.At(invalid_timer)
        expected = "At() timepoint must return datetime"

    model = hsm.Define(
        "InvalidTimer",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(timer, hsm.Target("../done")),
            hsm.Transition(
                hsm.On(hsm.ErrorEvent),
                hsm.Target("../error"),
                hsm.Effect(error_effect),
            ),
        ),
        hsm.State("done"),
        hsm.State("error"),
    )
    instance = TimerErrorInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    await asyncio.wait_for(hsm.AfterEntry(ctx, instance, "/InvalidTimer/error"), timeout=1)

    assert isinstance(instance.error, TypeError)
    assert expected in str(instance.error)
    assert instance.state() == "/InvalidTimer/error"
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0

    await hsm.Stop(instance)


async def _stop_during_inflight_dispatch_does_not_block_loop() -> None:
    class BlockingInstance(hsm.Instance):
        pass

    entered_effect = asyncio.Event()
    release_effect = asyncio.Event()

    async def slow_effect(ctx, inst, event):
        entered_effect.set()
        await release_effect.wait()

    model = hsm.Define(
        "InflightStop",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("go"),
                hsm.Target("../done"),
                hsm.Effect(slow_effect),
            ),
        ),
        hsm.State("done"),
    )
    instance = BlockingInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    dispatch_task = asyncio.create_task(hsm.Dispatch(ctx, instance, hsm.Event("go")))
    await asyncio.wait_for(entered_effect.wait(), timeout=1)

    stop_task = asyncio.create_task(hsm.Stop(instance))
    await asyncio.sleep(0)
    assert not stop_task.done()

    release_effect.set()
    await asyncio.wait_for(asyncio.gather(dispatch_task, stop_task), timeout=1)
    assert instance.state() == "/InflightStop"


def test_stop_during_inflight_dispatch_does_not_block_event_loop():
    asyncio.run(_stop_during_inflight_dispatch_does_not_block_loop())


async def _cancelled_stop_waiter_releases_processing_mutex() -> None:
    class BlockingInstance(hsm.Instance):
        pass

    entered_effect = asyncio.Event()
    release_effect = asyncio.Event()

    async def slow_effect(ctx, inst, event):
        entered_effect.set()
        await release_effect.wait()

    model = hsm.Define(
        "CancelledStop",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../done"), hsm.Effect(slow_effect)),
        ),
        hsm.State("done"),
    )
    instance = BlockingInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    dispatch_task = asyncio.create_task(hsm.Dispatch(ctx, instance, hsm.Event("go")))
    await asyncio.wait_for(entered_effect.wait(), timeout=1)

    cancelled_stop = asyncio.create_task(hsm.Stop(instance))
    await asyncio.sleep(0)
    cancelled_stop.cancel()
    try:
        await cancelled_stop
    except asyncio.CancelledError:
        pass

    release_effect.set()
    await asyncio.wait_for(dispatch_task, timeout=1)
    assert instance.state() == "/CancelledStop/done"

    await asyncio.wait_for(hsm.Stop(instance), timeout=1)
    assert instance.state() == "/CancelledStop"


def test_cancelled_stop_waiter_does_not_poison_processing_mutex():
    asyncio.run(_cancelled_stop_waiter_releases_processing_mutex())


async def _cancelled_dispatch_awaiter_does_not_cancel_processing_task() -> None:
    class BlockingInstance(hsm.Instance):
        pass

    entered_effect = asyncio.Event()
    release_effect = asyncio.Event()

    async def slow_effect(ctx, inst, event):
        entered_effect.set()
        await release_effect.wait()

    model = hsm.Define(
        "CancelledDispatchAwaiter",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../done"), hsm.Effect(slow_effect))),
        hsm.State("done"),
    )
    instance = BlockingInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    dispatch_task = asyncio.create_task(hsm.Dispatch(ctx, instance, hsm.Event("go")))
    await asyncio.wait_for(entered_effect.wait(), timeout=1)

    dispatch_task.cancel()
    try:
        await dispatch_task
    except asyncio.CancelledError:
        pass

    release_effect.set()
    await asyncio.wait_for(hsm.AfterEntry(ctx, instance, "/CancelledDispatchAwaiter/done"), timeout=1)

    assert instance.state() == "/CancelledDispatchAwaiter/done"
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    await asyncio.wait_for(hsm.Stop(instance), timeout=1)


def test_cancelled_dispatch_awaiter_does_not_cancel_processing_task():
    asyncio.run(_cancelled_dispatch_awaiter_does_not_cancel_processing_task())


async def _awaited_nested_dispatch_from_effect_does_not_self_await() -> None:
    class NestedDispatchInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.trace: list[str] = []

    async def j_effect(ctx, inst, event):
        inst.trace.append("j.effect")
        await hsm.Dispatch(ctx, inst, hsm.Event("k"))
        inst.trace.append("j.after-dispatch")

    async def k_effect(ctx, inst, event):
        inst.trace.append("k.effect")

    model = hsm.Define(
        "NestedDispatch",
        hsm.Initial(hsm.Target("left")),
        hsm.State(
            "left",
            hsm.Transition(
                hsm.On("j"),
                hsm.Target("../middle"),
                hsm.Effect(j_effect),
            ),
        ),
        hsm.State(
            "middle",
            hsm.Transition(
                hsm.On("k"),
                hsm.Target("../right"),
                hsm.Effect(k_effect),
            ),
        ),
        hsm.State("right"),
    )

    instance = NestedDispatchInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    await asyncio.wait_for(hsm.Dispatch(ctx, instance, hsm.Event("j")), timeout=1)

    assert instance.state() == "/NestedDispatch/right"
    assert instance.trace == ["j.effect", "j.after-dispatch", "k.effect"]
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0


def test_awaited_nested_dispatch_from_effect_does_not_self_await():
    asyncio.run(_awaited_nested_dispatch_from_effect_does_not_self_await())


async def _awaited_stop_from_effect_does_not_self_deadlock() -> None:
    class SelfStoppingInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.trace: list[str] = []

    async def stop_effect(ctx, inst, event):
        inst.trace.append("effect.before-stop")
        await hsm.Stop(inst)
        inst.trace.append("effect.after-stop")

    async def done_entry(ctx, inst, event):
        inst.trace.append("done.entry")

    async def done_exit(ctx, inst, event):
        inst.trace.append("done.exit")

    model = hsm.Define(
        "SelfStopping",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("stop"),
                hsm.Target("../done"),
                hsm.Effect(stop_effect),
            ),
        ),
        hsm.State("done", hsm.Entry(done_entry), hsm.Exit(done_exit)),
    )

    instance = SelfStoppingInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    await asyncio.wait_for(hsm.Dispatch(ctx, instance, hsm.Event("stop")), timeout=1)

    assert instance.state() == "/SelfStopping"
    assert ctx.machines() == []
    assert instance.trace == ["effect.before-stop", "effect.after-stop", "done.entry", "done.exit"]
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0


def test_awaited_stop_from_effect_does_not_self_deadlock():
    asyncio.run(_awaited_stop_from_effect_does_not_self_deadlock())


async def _awaited_restart_from_effect_does_not_self_deadlock() -> None:
    class SelfRestartingInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.trace: list[str] = []

    async def idle_entry(ctx, inst, event):
        inst.trace.append(f"idle.entry:{event.Data!r}")

    async def restart_effect(ctx, inst, event):
        inst.trace.append("effect.before-restart")
        await hsm.Restart(inst, "again")
        inst.trace.append("effect.after-restart")

    async def done_entry(ctx, inst, event):
        inst.trace.append("done.entry")

    model = hsm.Define(
        "SelfRestarting",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Entry(idle_entry),
            hsm.Transition(
                hsm.On("restart"),
                hsm.Target("../done"),
                hsm.Effect(restart_effect),
            ),
        ),
        hsm.State("done", hsm.Entry(done_entry)),
    )

    instance = SelfRestartingInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    await asyncio.wait_for(hsm.Dispatch(ctx, instance, hsm.Event("restart")), timeout=1)

    assert instance.state() == "/SelfRestarting/idle"
    assert len(ctx.machines()) == 1
    assert instance.trace == [
        "idle.entry:None",
        "effect.before-restart",
        "effect.after-restart",
        "done.entry",
        "idle.entry:'again'",
    ]
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0


def test_awaited_restart_from_effect_does_not_self_deadlock():
    asyncio.run(_awaited_restart_from_effect_does_not_self_deadlock())


async def _cancelled_stop_after_acquire_releases_processing_mutex() -> None:
    class BlockingInstance(hsm.Instance):
        pass

    entered_exit = asyncio.Event()
    release_exits: asyncio.Queue[asyncio.Event] = asyncio.Queue()

    async def slow_exit(ctx, inst, event):
        entered_exit.set()
        release_exit = asyncio.Event()
        release_exits.put_nowait(release_exit)
        await release_exit.wait()

    model = hsm.Define(
        "CancelledAcquiredStop",
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Exit(slow_exit)),
    )
    instance = BlockingInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    cancelled_stop = asyncio.create_task(hsm.Stop(instance))
    await asyncio.wait_for(entered_exit.wait(), timeout=1)
    cancelled_stop.cancel()
    try:
        await cancelled_stop
    except asyncio.CancelledError:
        pass

    first_release = await asyncio.wait_for(release_exits.get(), timeout=1)
    first_release.set()

    entered_exit.clear()
    second_stop = asyncio.create_task(hsm.Stop(instance))
    await asyncio.wait_for(entered_exit.wait(), timeout=1)
    second_release = await asyncio.wait_for(release_exits.get(), timeout=1)
    second_release.set()
    await asyncio.wait_for(second_stop, timeout=1)

    assert instance.state() == "/CancelledAcquiredStop"
    assert ctx.machines() == []


def test_cancelled_stop_after_acquire_releases_processing_mutex():
    asyncio.run(_cancelled_stop_after_acquire_releases_processing_mutex())


async def _context_registration_lifecycle_stress(rounds: int) -> None:
    class LifecycleInstance(hsm.Instance):
        pass

    ctx = hsm.Context()
    model = hsm.Define(
        "RegistrationLifecycle",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../done"))),
        hsm.State("done"),
    )

    for index in range(rounds):
        instance = LifecycleInstance()
        await hsm.Start(ctx, instance, model)
        assert len(ctx.machines()) == 1
        await hsm.DispatchAll(ctx, hsm.Event("go"))
        assert instance.state() == "/RegistrationLifecycle/done"
        await hsm.Stop(instance)
        assert instance.state() == "/RegistrationLifecycle"
        assert ctx.machines() == []

        if index % 10 == 0:
            await hsm.DispatchAll(ctx, hsm.Event("go"))
            assert ctx.machines() == []


def test_context_registration_does_not_leak_on_repeated_start_stop():
    asyncio.run(_context_registration_lifecycle_stress(rounds=100))


async def _restart_preserves_context_registration() -> None:
    class RestartInstance(hsm.Instance):
        pass

    ctx = hsm.Context()
    model = hsm.Define(
        "RestartRegistration",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../done"))),
        hsm.State("done"),
    )
    instance = RestartInstance()
    await hsm.Start(ctx, instance, model)
    assert len(ctx.machines()) == 1

    await hsm.Restart(instance)
    assert len(ctx.machines()) == 1
    assert instance.state() == "/RestartRegistration/idle"

    await hsm.DispatchAll(ctx, hsm.Event("go"))
    assert instance.state() == "/RestartRegistration/done"
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0

    await hsm.Stop(instance)
    assert ctx.machines() == []


def test_restart_preserves_context_registration():
    asyncio.run(_restart_preserves_context_registration())


async def _timer_restart_cancellation_stress(rounds: int) -> None:
    class TimerInstance(hsm.Instance):
        pass

    sleeps: list[asyncio.Future[None]] = []
    cancelled = 0

    async def manual_sleep(duration):
        nonlocal cancelled
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        sleeps.append(future)
        try:
            await future
        except asyncio.CancelledError:
            cancelled += 1
            raise

    async def delay(ctx, inst, event):
        return timedelta(seconds=1)

    model = hsm.Define(
        "TimerRestart",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(hsm.After(delay), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )
    instance = TimerInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model, hsm.Config(Clock=hsm.Clock(sleep=manual_sleep)))

    for _ in range(rounds):
        for _ in range(10):
            if sleeps:
                break
            await asyncio.sleep(0)
        old_sleep = sleeps.pop(0)

        await hsm.Restart(instance)

        assert instance.state() == "/TimerRestart/waiting"
        assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
        assert cancelled >= 1
        if not old_sleep.done():
            old_sleep.set_result(None)
        await asyncio.sleep(0)
        assert instance.state() == "/TimerRestart/waiting"

    for _ in range(10):
        if sleeps:
            break
        await asyncio.sleep(0)
    final_sleep = sleeps.pop(0)
    final_sleep.set_result(None)
    for _ in range(10):
        if instance.state() == "/TimerRestart/done":
            break
        await asyncio.sleep(0)
    assert instance.state() == "/TimerRestart/done"
    await hsm.Stop(instance)


def test_timer_restart_cancellation_stress():
    asyncio.run(_timer_restart_cancellation_stress(rounds=50))


async def _history_reentry_stress(rounds: int) -> None:
    instance = FuzzInstance()
    ctx = hsm.Context()
    model = hsm.Define(
        "HistoryStress",
        hsm.Initial(hsm.Target("parent")),
        hsm.State(
            "parent",
            hsm.Initial(hsm.Target("a")),
            hsm.ShallowHistory("shallow", hsm.Transition(hsm.Target("a"))),
            hsm.DeepHistory("deep", hsm.Transition(hsm.Target("a"))),
            hsm.State(
                "a",
                hsm.Initial(hsm.Target("a1")),
                hsm.State("a1", hsm.Transition(hsm.On("next"), hsm.Target("../a2"))),
                hsm.State("a2", hsm.Transition(hsm.On("leave"), hsm.Target("../../../outside"))),
            ),
            hsm.State("b"),
        ),
        hsm.State(
            "outside",
            hsm.Transition(hsm.On("shallow"), hsm.Target("../parent/shallow")),
            hsm.Transition(hsm.On("deep"), hsm.Target("../parent/deep")),
            hsm.Transition(hsm.On("reset"), hsm.Target("../parent")),
        ),
    )
    await hsm.Start(ctx, instance, model)

    for _ in range(rounds):
        assert instance.state() == "/HistoryStress/parent/a/a1"
        await hsm.Dispatch(ctx, instance, hsm.Event("next"))
        assert instance.state() == "/HistoryStress/parent/a/a2"
        await hsm.Dispatch(ctx, instance, hsm.Event("leave"))
        assert instance.state() == "/HistoryStress/outside"

        await hsm.Dispatch(ctx, instance, hsm.Event("shallow"))
        assert instance.state() == "/HistoryStress/parent/a/a1"
        await hsm.Dispatch(ctx, instance, hsm.Event("next"))
        await hsm.Dispatch(ctx, instance, hsm.Event("leave"))

        await hsm.Dispatch(ctx, instance, hsm.Event("deep"))
        assert instance.state() == "/HistoryStress/parent/a/a2"
        await hsm.Dispatch(ctx, instance, hsm.Event("leave"))

        await hsm.Dispatch(ctx, instance, hsm.Event("reset"))
        assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0

    await hsm.Stop(instance)


def test_shallow_and_deep_history_reentry_stress():
    asyncio.run(_history_reentry_stress(rounds=50))
