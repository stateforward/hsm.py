import asyncio
from dataclasses import dataclass
from datetime import timedelta

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import hsm
from hsm import hsm as core


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


@given(st.lists(st.sampled_from(("to_a", "to_b", "leave", "parent_leave", "return")), min_size=0, max_size=80))
@settings(max_examples=80, deadline=None, derandomize=True)
def test_hierarchical_transition_trace_fuzz(trace: list[str]):
    asyncio.run(_drive_hierarchical_trace(tuple(trace)))


async def _drive_hierarchical_trace(trace: tuple[str, ...]) -> None:
    class HierarchicalInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.log: list[str] = []

    def record(action: str):
        async def callback(ctx, inst: HierarchicalInstance, event):
            inst.log.append(action)

        return callback

    model = hsm.Define(
        "HierarchicalFuzz",
        hsm.Initial(hsm.Target("parent/a")),
        hsm.State(
            "parent",
            hsm.Entry(record("parent.entry")),
            hsm.Exit(record("parent.exit")),
            hsm.Initial(hsm.Target("a")),
            hsm.Transition(hsm.On("parent_leave"), hsm.Target("../outside")),
            hsm.State(
                "a",
                hsm.Entry(record("a.entry")),
                hsm.Exit(record("a.exit")),
                hsm.Transition(hsm.On("to_b"), hsm.Target("../b")),
                hsm.Transition(hsm.On("leave"), hsm.Target("../../outside")),
            ),
            hsm.State(
                "b",
                hsm.Entry(record("b.entry")),
                hsm.Exit(record("b.exit")),
                hsm.Transition(hsm.On("to_a"), hsm.Target("../a")),
                hsm.Transition(hsm.On("leave"), hsm.Target("../../outside")),
            ),
        ),
        hsm.State(
            "outside",
            hsm.Entry(record("outside.entry")),
            hsm.Exit(record("outside.exit")),
            hsm.Transition(hsm.On("return"), hsm.Target("../parent/a")),
        ),
    )

    instance = HierarchicalInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    expected_state = "a"
    expected_log = ["parent.entry", "a.entry"]
    assert instance.state() == "/HierarchicalFuzz/parent/a"
    assert instance.log == expected_log

    for event_name in trace:
        await hsm.Dispatch(ctx, instance, hsm.Event(event_name))
        if expected_state == "a":
            if event_name == "to_b":
                expected_state = "b"
                expected_log.extend(["a.exit", "b.entry"])
            elif event_name in ("leave", "parent_leave"):
                expected_state = "outside"
                expected_log.extend(["a.exit", "parent.exit", "outside.entry"])
        elif expected_state == "b":
            if event_name == "to_a":
                expected_state = "a"
                expected_log.extend(["b.exit", "a.entry"])
            elif event_name in ("leave", "parent_leave"):
                expected_state = "outside"
                expected_log.extend(["b.exit", "parent.exit", "outside.entry"])
        elif expected_state == "outside" and event_name == "return":
            expected_state = "a"
            expected_log.extend(["outside.exit", "parent.entry", "a.entry"])

        expected_path = (
            f"/HierarchicalFuzz/parent/{expected_state}"
            if expected_state in ("a", "b")
            else "/HierarchicalFuzz/outside"
        )
        snapshot = hsm.TakeSnapshot(ctx, instance)
        assert snapshot.State == expected_path
        assert snapshot.QueueLen == 0
        assert instance.log == expected_log

    await hsm.Stop(instance)
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0


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


async def _broadcast_self_stop_stress(machine_count: int, rounds: int) -> None:
    class BroadcastSelfStopInstance(FuzzInstance):
        def __init__(self) -> None:
            super().__init__()
            self.stops = 0
            self.entries = 0

    async def stop_effect(ctx, inst: BroadcastSelfStopInstance, event) -> None:
        inst.stops += 1
        await hsm.Stop(inst)

    async def idle_entry(ctx, inst: BroadcastSelfStopInstance, event) -> None:
        inst.entries += 1

    model = hsm.Define(
        "BroadcastSelfStop",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Entry(idle_entry),
            hsm.Transition(hsm.On("stop"), hsm.Effect(stop_effect)),
        ),
    )

    ctx = hsm.Context()
    instances = [BroadcastSelfStopInstance() for _ in range(machine_count)]

    for round_index in range(rounds):
        for index, instance in enumerate(instances):
            assert instance.state() in ("", "/BroadcastSelfStop")
            await hsm.Started(ctx, instance, model, hsm.Config(ID=f"self-stop-{round_index}-{index}"))

        assert len(ctx.machines()) == machine_count
        await hsm.DispatchAll(ctx, hsm.Event("stop"))

        assert ctx.machines() == []
        for instance in instances:
            assert instance.state() == "/BroadcastSelfStop"
            assert instance.stops == round_index + 1
            assert instance.entries == round_index + 1
            snapshot = hsm.TakeSnapshot(ctx, instance)
            assert snapshot.State == "/BroadcastSelfStop"
            assert snapshot.QueueLen == 0


def test_broadcast_self_stop_stress():
    asyncio.run(_broadcast_self_stop_stress(machine_count=40, rounds=25))


async def _snapshot_attribute_aliasing_stress(rounds: int) -> None:
    class SnapshotIsolationInstance(FuzzInstance):
        pass

    model = hsm.Define(
        "SnapshotIsolation",
        hsm.Attribute("payload", {"items": []}),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.OnSet("payload"))),
    )

    ctx = hsm.Context()
    instance = SnapshotIsolationInstance()
    await hsm.Start(ctx, instance, model)

    original_payload = {"items": ["original"]}
    await hsm.Set(ctx, instance, "payload", original_payload)
    original_payload["items"].append("mutated-after-set")

    runtime_value, ok = hsm.Get(ctx, instance, "payload")
    assert ok is True
    assert runtime_value["items"] == ["original", "mutated-after-set"]

    for index in range(rounds):
        value = {"items": [index, {"nested": [index]}]}
        await hsm.Set(ctx, instance, "payload", value)

        snapshot = hsm.TakeSnapshot(ctx, instance)
        snapshot_payload = snapshot.Attributes["/SnapshotIsolation/payload"]
        snapshot_payload["items"][1]["nested"].append("snapshot-mutated")
        snapshot.Attributes["/SnapshotIsolation/payload"] = {"items": ["replaced"]}

        runtime_value, ok = hsm.Get(ctx, instance, "payload")
        assert ok is True
        assert runtime_value == {"items": [index, {"nested": [index]}]}

        value["items"][1]["nested"].append("caller-mutated")
        runtime_value, ok = hsm.Get(ctx, instance, "payload")
        assert ok is True
        assert runtime_value == {"items": [index, {"nested": [index, "caller-mutated"]}]}

        fresh_snapshot = hsm.TakeSnapshot(ctx, instance)
        assert fresh_snapshot.Attributes["/SnapshotIsolation/payload"] == runtime_value
        assert fresh_snapshot.QueueLen == 0

    await hsm.Stop(instance)


def test_snapshot_attribute_aliasing_stress():
    asyncio.run(_snapshot_attribute_aliasing_stress(rounds=100))


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


async def _stop_discards_deferred_events() -> None:
    instance = FuzzInstance()
    ctx = hsm.Context()
    model = hsm.Define(
        "DeferredStop",
        hsm.Initial(hsm.Target("holding")),
        hsm.State(
            "holding",
            hsm.Defer(hsm.Event("work")),
        ),
    )
    sm = await hsm.Start(ctx, instance, model)

    await hsm.Dispatch(ctx, instance, hsm.Event("work"))
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 1

    await hsm.Stop(instance)

    assert instance.state() == "/DeferredStop"
    assert ctx.machines() == []
    assert hsm.TakeSnapshot(ctx, sm).QueueLen == 0


def test_stop_discards_deferred_events():
    asyncio.run(_stop_discards_deferred_events())


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


async def _failing_error_handler_does_not_recursively_enqueue_error_events() -> None:
    class RecursiveErrorInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.error_effects = 0

    async def failing_effect(ctx, inst, event):
        raise RuntimeError("primary failure")

    async def failing_error_effect(ctx, inst: RecursiveErrorInstance, event):
        inst.error_effects += 1
        raise RuntimeError("secondary failure")

    model = hsm.Define(
        "RecursiveError",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("go"),
                hsm.Target("../failed"),
                hsm.Effect(failing_effect),
            ),
        ),
        hsm.State(
            "failed",
            hsm.Transition(
                hsm.On(hsm.ErrorEvent),
                hsm.Target("../error"),
                hsm.Effect(failing_error_effect),
            ),
        ),
        hsm.State("error"),
    )

    instance = RecursiveErrorInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    await asyncio.wait_for(hsm.Dispatch(ctx, instance, hsm.Event("go")), timeout=1)

    assert instance.state() == "/RecursiveError/error"
    assert instance.error_effects == 1
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    await hsm.Stop(instance)


def test_failing_error_handler_does_not_recursively_enqueue_error_events():
    asyncio.run(_failing_error_handler_does_not_recursively_enqueue_error_events())


async def _manual_cancelled_error_callbacks_do_not_abort_processing() -> None:
    class CancelledCallbackInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.errors: list[BaseException] = []
            self.activity_started = False

    async def cancelled_effect(ctx, inst, event):
        raise asyncio.CancelledError("manual effect cancellation")

    async def cancelled_activity(ctx, inst: CancelledCallbackInstance, event):
        inst.activity_started = True
        raise asyncio.CancelledError("manual activity cancellation")

    async def cancelled_guard(ctx, inst, event):
        raise asyncio.CancelledError("manual guard cancellation")

    async def error_effect(ctx, inst: CancelledCallbackInstance, event):
        inst.errors.append(event.Data)

    model = hsm.Define(
        "ManualCancelledCallbacks",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("effect"),
                hsm.Target("../after_effect"),
                hsm.Effect(cancelled_effect),
            ),
            hsm.Transition(
                hsm.On("guard"),
                hsm.Guard(cancelled_guard),
                hsm.Target("../guarded"),
            ),
        ),
        hsm.State(
            "after_effect",
            hsm.Transition(
                hsm.On(hsm.ErrorEvent),
                hsm.Target("../error"),
                hsm.Effect(error_effect),
            ),
        ),
        hsm.State(
            "activity",
            hsm.Activity(cancelled_activity),
            hsm.Transition(
                hsm.On(hsm.ErrorEvent),
                hsm.Target("../error"),
                hsm.Effect(error_effect),
            ),
        ),
        hsm.State("guarded"),
        hsm.State("error"),
    )

    ctx = hsm.Context()

    effect_instance = CancelledCallbackInstance()
    await hsm.Start(ctx, effect_instance, model)
    await hsm.Dispatch(ctx, effect_instance, hsm.Event("effect"))
    assert effect_instance.state() == "/ManualCancelledCallbacks/error"
    assert len(effect_instance.errors) == 1
    assert isinstance(effect_instance.errors[0], asyncio.CancelledError)
    assert hsm.TakeSnapshot(ctx, effect_instance).QueueLen == 0
    await hsm.Stop(effect_instance)

    guard_instance = CancelledCallbackInstance()
    await hsm.Start(ctx, guard_instance, model)
    await hsm.Dispatch(ctx, guard_instance, hsm.Event("guard"))
    assert guard_instance.state() == "/ManualCancelledCallbacks/idle"
    assert guard_instance.errors == []
    assert hsm.TakeSnapshot(ctx, guard_instance).QueueLen == 0
    await hsm.Stop(guard_instance)

    activity_instance = CancelledCallbackInstance()
    activity_model = hsm.Define(
        "ManualCancelledActivity",
        hsm.Initial(hsm.Target("activity")),
        hsm.State(
            "activity",
            hsm.Activity(cancelled_activity),
            hsm.Transition(
                hsm.On(hsm.ErrorEvent),
                hsm.Target("../error"),
                hsm.Effect(error_effect),
            ),
        ),
        hsm.State("error"),
    )
    await hsm.Start(ctx, activity_instance, activity_model)
    await asyncio.wait_for(hsm.AfterEntry(ctx, activity_instance, "/ManualCancelledActivity/error"), timeout=1)
    assert activity_instance.activity_started is True
    assert len(activity_instance.errors) == 1
    assert isinstance(activity_instance.errors[0], asyncio.CancelledError)
    assert hsm.TakeSnapshot(ctx, activity_instance).QueueLen == 0
    await hsm.Stop(activity_instance)


def test_manual_cancelled_error_callbacks_do_not_abort_processing():
    asyncio.run(_manual_cancelled_error_callbacks_do_not_abort_processing())


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


async def _stop_from_activity_does_not_await_current_activity_task() -> None:
    class ActivityStopInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.trace: list[str] = []

    activity_started = asyncio.Event()
    activity_done = asyncio.Event()

    async def self_stopping_activity(ctx, inst, event):
        inst.trace.append("activity.before-stop")
        activity_started.set()
        await hsm.Stop(inst)
        assert ctx.done is True
        inst.trace.append("activity.after-stop")
        activity_done.set()

    model = hsm.Define(
        "ActivitySelfStop",
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Activity(self_stopping_activity)),
    )

    instance = ActivityStopInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)
    await asyncio.wait_for(activity_started.wait(), timeout=1)
    await asyncio.wait_for(activity_done.wait(), timeout=1)

    assert instance.state() == "/ActivitySelfStop"
    assert ctx.machines() == []
    assert instance.trace == ["activity.before-stop", "activity.after-stop"]


def test_stop_from_activity_does_not_await_current_activity_task():
    asyncio.run(_stop_from_activity_does_not_await_current_activity_task())


async def _restart_from_activity_does_not_await_current_activity_task() -> None:
    class ActivityRestartInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.trace: list[str] = []
            self.restarted = False

    activity_done = asyncio.Event()

    async def self_restarting_activity(ctx, inst: ActivityRestartInstance, event):
        if inst.restarted:
            return
        inst.restarted = True
        inst.trace.append("activity.before-restart")
        await hsm.Restart(inst, "again")
        assert ctx.done is True
        inst.trace.append("activity.after-restart")
        activity_done.set()

    async def active_entry(ctx, inst, event):
        inst.trace.append(f"entry:{event.Data!r}")

    model = hsm.Define(
        "ActivitySelfRestart",
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Entry(active_entry), hsm.Activity(self_restarting_activity)),
    )

    instance = ActivityRestartInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)
    await asyncio.wait_for(activity_done.wait(), timeout=1)

    assert instance.state() == "/ActivitySelfRestart/active"
    assert len(ctx.machines()) == 1
    assert instance.trace == [
        "entry:None",
        "activity.before-restart",
        "entry:'again'",
        "activity.after-restart",
    ]
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    await hsm.Stop(instance)


def test_restart_from_activity_does_not_await_current_activity_task():
    asyncio.run(_restart_from_activity_does_not_await_current_activity_task())


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


async def _cancelled_context_broadcasts_do_not_dispatch() -> None:
    class CancelledContextInstance(hsm.Instance):
        pass

    ctx = hsm.Context()
    model = hsm.Define(
        "CancelledContextBroadcast",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../done"))),
        hsm.State("done"),
    )
    first = await hsm.Started(ctx, CancelledContextInstance(), model, hsm.Config(ID="first"))
    second = await hsm.Started(ctx, CancelledContextInstance(), model, hsm.Config(ID="second"))

    assert {hsm.ID(machine) for machine in ctx.machines()} == {"first", "second"}
    ctx.cancel()

    await hsm.DispatchAll(ctx, hsm.Event("go"))
    await hsm.DispatchTo(ctx, hsm.Event("go"), "first")

    assert first.state() == "/CancelledContextBroadcast/idle"
    assert second.state() == "/CancelledContextBroadcast/idle"
    assert hsm.TakeSnapshot(ctx, first).QueueLen == 0
    assert hsm.TakeSnapshot(ctx, second).QueueLen == 0

    await hsm.Stop(first)
    await hsm.Stop(second)


def test_cancelled_context_broadcasts_do_not_dispatch():
    asyncio.run(_cancelled_context_broadcasts_do_not_dispatch())


async def _repeated_start_fails_without_leaking_context_registration() -> None:
    class RepeatedStartInstance(hsm.Instance):
        pass

    ctx = hsm.Context()
    model = hsm.Define(
        "RepeatedStart",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )
    instance = RepeatedStartInstance()
    sm = await hsm.Start(ctx, instance, model)
    assert ctx.machines() == [sm]

    try:
        await hsm.Start(ctx, instance, model)
    except hsm.ValidationError as error:
        assert "already has a running HSM" in str(error)
    else:
        raise AssertionError("repeated Start() on an instance should fail")

    try:
        await hsm.Start(ctx, sm)
    except hsm.ValidationError as error:
        assert "already started HSM" in str(error)
    else:
        raise AssertionError("repeated Start() on an HSM should fail")

    assert ctx.machines() == [sm]
    assert instance.state() == "/RepeatedStart/idle"
    await hsm.Stop(instance)
    assert ctx.machines() == []


def test_repeated_start_fails_without_leaking_context_registration():
    asyncio.run(_repeated_start_fails_without_leaking_context_registration())


async def _stopped_hsm_start_resets_runtime_state() -> None:
    class ReusableInstance(hsm.Instance):
        pass

    entries: list[tuple[object, bool]] = []

    async def record_entry(ctx: hsm.Context, inst: ReusableInstance, event: hsm.Event) -> None:
        entries.append((event.Data, ctx.done))

    ctx = hsm.Context()
    model = hsm.Define(
        "ReusableStart",
        hsm.Attribute("count", 0),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Entry(record_entry)),
    )
    instance = ReusableInstance()
    sm = hsm.New(instance, model)

    await hsm.Start(ctx, sm, "first")
    await hsm.Set(ctx, instance, "count", 7)
    assert hsm.Get(ctx, sm, "count") == (7, True)
    await hsm.Stop(sm)

    assert instance.state() == "/ReusableStart"
    assert ctx.machines() == []

    await hsm.Start(ctx, sm, "second")

    assert instance.state() == "/ReusableStart/idle"
    assert ctx.machines() == [sm]
    assert hsm.Get(ctx, sm, "count") == (0, True)
    assert hsm.TakeSnapshot(ctx, sm).QueueLen == 0
    assert entries == [("first", False), ("second", False)]

    await hsm.Stop(sm)
    assert ctx.machines() == []


def test_stopped_hsm_start_resets_runtime_state():
    asyncio.run(_stopped_hsm_start_resets_runtime_state())


async def _starting_new_hsm_in_new_context_clears_old_context_registration() -> None:
    class MovedContextInstance(hsm.Instance):
        pass

    model = hsm.Define(
        "MovedContext",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )
    instance = MovedContextInstance()
    sm = hsm.New(instance, model)
    old_context = sm.context()
    new_context = hsm.Context()

    assert old_context.machines() == [sm]
    assert new_context.machines() == []

    await hsm.Start(new_context, sm)

    assert old_context.machines() == []
    assert new_context.machines() == [sm]

    await hsm.Stop(sm)

    assert old_context.machines() == []
    assert new_context.machines() == []


def test_starting_new_hsm_in_new_context_clears_old_context_registration():
    asyncio.run(_starting_new_hsm_in_new_context_clears_old_context_registration())


async def _cancelled_observer_waiters_do_not_accumulate(rounds: int) -> None:
    class ObserverLeakInstance(hsm.Instance):
        pass

    model = hsm.Define(
        "ObserverLeak",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../done"))),
        hsm.State("done"),
    )
    instance = ObserverLeakInstance()
    ctx = hsm.Context()
    sm = await hsm.Start(ctx, instance, model)

    async def cancel_waiter(future: asyncio.Future[None]) -> None:
        future.cancel()
        try:
            await future
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0)

    for _ in range(rounds):
        await asyncio.gather(
            cancel_waiter(hsm.AfterEntry(ctx, instance, "/ObserverLeak/missing")),
            cancel_waiter(hsm.AfterExit(ctx, instance, "/ObserverLeak/missing")),
            cancel_waiter(hsm.AfterExecuted(ctx, instance, "/ObserverLeak/missing")),
            cancel_waiter(hsm.AfterDispatch(ctx, instance, hsm.Event("missing"))),
            cancel_waiter(hsm.AfterProcess(ctx, instance, hsm.Event("missing"))),
        )

    assert sm._after.entry == []
    assert sm._after.exit == []
    assert sm._after.executed == []
    assert sm._after.dispatch == []
    assert sm._after.process == []

    await hsm.Dispatch(ctx, instance, hsm.Event("go"))
    assert instance.state() == "/ObserverLeak/done"
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    await hsm.Stop(instance)


def test_cancelled_observer_waiters_do_not_accumulate():
    asyncio.run(_cancelled_observer_waiters_do_not_accumulate(rounds=100))


async def _stop_cancels_pending_observer_waiters() -> None:
    class ObserverStopInstance(hsm.Instance):
        pass

    model = hsm.Define(
        "ObserverStop",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )
    instance = ObserverStopInstance()
    ctx = hsm.Context()
    sm = await hsm.Start(ctx, instance, model)
    futures = [
        hsm.AfterEntry(ctx, instance, "/ObserverStop/missing"),
        hsm.AfterExit(ctx, instance, "/ObserverStop/missing"),
        hsm.AfterExecuted(ctx, instance, "/ObserverStop/missing"),
        hsm.AfterDispatch(ctx, instance, hsm.Event("missing")),
        hsm.AfterProcess(ctx, instance, hsm.Event("missing")),
    ]

    await hsm.Stop(instance)
    await asyncio.sleep(0)

    assert all(future.cancelled() for future in futures)
    assert sm._after.entry == []
    assert sm._after.exit == []
    assert sm._after.executed == []
    assert sm._after.dispatch == []
    assert sm._after.process == []


def test_stop_cancels_pending_observer_waiters():
    asyncio.run(_stop_cancels_pending_observer_waiters())


async def _stopped_machine_rejects_observer_waiters() -> None:
    class ObserverStoppedInstance(hsm.Instance):
        pass

    model = hsm.Define(
        "ObserverStopped",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )
    instance = ObserverStoppedInstance()
    ctx = hsm.Context()
    sm = await hsm.Start(ctx, instance, model)
    await hsm.Stop(instance)

    for make_waiter in (
        lambda: hsm.AfterEntry(ctx, instance, "/ObserverStopped/idle"),
        lambda: hsm.AfterExit(ctx, instance, "/ObserverStopped/idle"),
        lambda: hsm.AfterExecuted(ctx, instance, "/ObserverStopped/idle"),
        lambda: hsm.AfterDispatch(ctx, instance, hsm.Event("go")),
        lambda: hsm.AfterProcess(ctx, instance, hsm.Event("go")),
    ):
        try:
            make_waiter()
        except hsm.ValidationError as error:
            assert "started HSM" in str(error)
        else:
            raise AssertionError("observer waiter on stopped HSM should fail")

    assert sm._after.entry == []
    assert sm._after.exit == []
    assert sm._after.executed == []
    assert sm._after.dispatch == []
    assert sm._after.process == []


def test_stopped_machine_rejects_observer_waiters():
    asyncio.run(_stopped_machine_rejects_observer_waiters())


async def _cancelled_context_waiter_does_not_poison_future() -> None:
    ctx = hsm.Context()

    cancelled_waiter = asyncio.create_task(ctx.wait_done())
    await asyncio.sleep(0)
    cancelled_waiter.cancel()
    try:
        await cancelled_waiter
    except asyncio.CancelledError:
        pass

    assert ctx.done is False
    assert ctx._done_future is not None
    assert ctx._done_future.cancelled() is False

    live_waiter = asyncio.create_task(ctx.wait_done())
    await asyncio.sleep(0)
    assert live_waiter.done() is False

    ctx.cancel()
    await asyncio.wait_for(live_waiter, timeout=1)
    await ctx.wait_done()


def test_cancelled_context_waiter_does_not_poison_future():
    asyncio.run(_cancelled_context_waiter_does_not_poison_future())


async def _mixed_context_waiter_cancellation_stress() -> None:
    ctx = hsm.Context()
    waiters = [asyncio.create_task(ctx.wait_done()) for _ in range(100)]
    await asyncio.sleep(0)

    cancelled = waiters[::3]
    live = [waiter for waiter in waiters if waiter not in cancelled]
    for waiter in cancelled:
        waiter.cancel()

    results = await asyncio.gather(*cancelled, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    assert ctx.done is False
    assert ctx._done_future is not None
    assert ctx._done_future.cancelled() is False
    assert all(waiter.done() is False for waiter in live)

    ctx.cancel()
    await asyncio.wait_for(asyncio.gather(*live), timeout=1)
    await ctx.wait_done()


def test_mixed_context_waiter_cancellation_stress():
    asyncio.run(_mixed_context_waiter_cancellation_stress())


async def _cancelled_mutex_handoff_releases_lock() -> None:
    mutex = core.Mutex()

    for _ in range(100):
        await mutex.acquire()
        waiter = asyncio.create_task(mutex.acquire())
        await asyncio.sleep(0)

        mutex.release()
        waiter.cancel()
        try:
            await waiter
        except asyncio.CancelledError:
            pass

        await asyncio.wait_for(mutex.acquire(), timeout=1)
        assert mutex.locked() is True
        assert len(mutex._waiters) == 0
        mutex.release()
        assert mutex.locked() is False


def test_cancelled_mutex_handoff_releases_lock():
    asyncio.run(_cancelled_mutex_handoff_releases_lock())


async def _group_event_mutations_preflight_members() -> None:
    class GroupPreflightInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.entries: list[object] = []

    async def entry(ctx, inst: GroupPreflightInstance, event):
        inst.entries.append(event.Data)

    model = hsm.Define(
        "GroupPreflight",
        hsm.Attribute("flag", False),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Entry(entry),
            hsm.Transition(hsm.On("go"), hsm.Target("../done")),
            hsm.Transition(hsm.OnSet("flag"), hsm.Target("../changed")),
        ),
        hsm.State("done"),
        hsm.State("changed"),
    )

    ctx = hsm.Context()
    running = GroupPreflightInstance()
    stopped = GroupPreflightInstance()
    never_started = GroupPreflightInstance()
    await hsm.Start(ctx, running, model)
    await hsm.Start(ctx, stopped, model)
    await hsm.Stop(stopped)

    cases = (
        (hsm.MakeGroup(running, stopped), "started HSM"),
        (hsm.MakeGroup(running, never_started), "missing hsm"),
    )
    for group, expected_error in cases:
        for operation in (
            lambda group=group: hsm.Dispatch(ctx, group, hsm.Event("go")),
            lambda group=group: hsm.Set(ctx, group, "flag", True),
            lambda group=group: hsm.Restart(group, "restarted"),
            lambda group=group: hsm.Stop(group),
        ):
            try:
                await operation()
            except hsm.ValidationError as error:
                assert expected_error in str(error)
            else:
                raise AssertionError("group event mutation should fail before partial fan-out")

            assert running.state() == "/GroupPreflight/idle"
            assert stopped.state() == "/GroupPreflight"
            assert never_started.state() == ""
            assert hsm.Get(ctx, running, "flag") == (False, True)
            assert hsm.TakeSnapshot(ctx, running).QueueLen == 0
            assert running.entries == [None]

    await hsm.Stop(running)


def test_group_event_mutations_preflight_members():
    asyncio.run(_group_event_mutations_preflight_members())


@given(
    st.lists(
        st.sampled_from(
            (
                "start_a",
                "start_b",
                "restart",
                "stop",
                "go",
                "reset",
                "set_0",
                "set_1",
                "call",
            )
        ),
        min_size=1,
        max_size=80,
    )
)
@settings(max_examples=80, deadline=None, derandomize=True)
def test_adversarial_lifecycle_event_script_fuzz(actions: list[str]):
    asyncio.run(_drive_adversarial_lifecycle_event_script(tuple(actions)))


async def _drive_adversarial_lifecycle_event_script(actions: tuple[str, ...]) -> None:
    class ScriptInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.calls = 0

    async def ping(ctx: hsm.Context, inst: ScriptInstance, value: int) -> int:
        inst.calls += 1
        return value + 1

    model = hsm.Define(
        "LifecycleScript",
        hsm.Attribute("flag", 0),
        hsm.Operation("ping", ping),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../done")),
            hsm.Transition(hsm.OnSet("flag")),
            hsm.Transition(hsm.OnCall("ping")),
        ),
        hsm.State(
            "done",
            hsm.Transition(hsm.On("reset"), hsm.Target("../idle")),
            hsm.Transition(hsm.OnSet("flag")),
            hsm.Transition(hsm.OnCall("ping")),
        ),
    )
    instance = ScriptInstance()
    sm = hsm.New(instance, model)
    initial_context = sm.context()
    context_a = hsm.Context()
    context_b = hsm.Context()
    contexts = (initial_context, context_a, context_b)

    expected_context: hsm.Context | None = initial_context
    expected_state = "/LifecycleScript"
    expected_flag = 0
    expected_calls = 0

    def assert_context_membership() -> None:
        memberships = [ctx for ctx in contexts if sm in ctx.machines()]
        if expected_context is None:
            assert memberships == []
        else:
            assert memberships == [expected_context]

    assert_context_membership()

    for index, action in enumerate(actions):
        if action in ("start_a", "start_b"):
            target_context = context_a if action == "start_a" else context_b
            if sm._started:
                try:
                    await hsm.Start(target_context, sm, index)
                except hsm.ValidationError as error:
                    assert "already started HSM" in str(error)
                else:
                    raise AssertionError("Start() on a running HSM should fail")
            else:
                await hsm.Start(target_context, sm, index)
                expected_context = target_context
                expected_state = "/LifecycleScript/idle"
                expected_flag = 0
        elif action == "restart":
            await hsm.Restart(sm, index)
            expected_context = sm.context()
            expected_state = "/LifecycleScript/idle"
            expected_flag = 0
        elif action == "stop":
            await hsm.Stop(sm)
            expected_context = None
            expected_state = "/LifecycleScript"
            expected_flag = 0
        elif action == "go":
            if sm._started:
                await hsm.Dispatch(sm.context(), sm, hsm.Event("go"))
                if expected_state == "/LifecycleScript/idle":
                    expected_state = "/LifecycleScript/done"
            else:
                try:
                    await hsm.Dispatch(sm.context(), sm, hsm.Event("go"))
                except hsm.ValidationError as error:
                    assert "started HSM" in str(error)
                else:
                    raise AssertionError("Dispatch() on a stopped HSM should fail")
        elif action == "reset":
            if sm._started:
                await hsm.Dispatch(sm.context(), sm, hsm.Event("reset"))
                if expected_state == "/LifecycleScript/done":
                    expected_state = "/LifecycleScript/idle"
            else:
                try:
                    await hsm.Dispatch(sm.context(), sm, hsm.Event("reset"))
                except hsm.ValidationError as error:
                    assert "started HSM" in str(error)
                else:
                    raise AssertionError("Dispatch() on a stopped HSM should fail")
        elif action in ("set_0", "set_1"):
            value = 0 if action == "set_0" else 1
            if sm._started:
                await hsm.Set(sm.context(), sm, "flag", value)
                expected_flag = value
            else:
                try:
                    await hsm.Set(sm.context(), sm, "flag", value)
                except hsm.ValidationError as error:
                    assert "started HSM" in str(error)
                else:
                    raise AssertionError("Set() on a stopped HSM should fail")
        elif action == "call":
            if sm._started:
                assert await hsm.Call(sm.context(), sm, "ping", index) == index + 1
                expected_calls += 1
            else:
                try:
                    await hsm.Call(sm.context(), sm, "ping", index)
                except hsm.ValidationError as error:
                    assert "started HSM" in str(error)
                else:
                    raise AssertionError("Call() on a stopped HSM should fail")

        assert_context_membership()
        snapshot = hsm.TakeSnapshot(sm.context(), sm)
        assert snapshot.State == expected_state
        assert snapshot.QueueLen == 0
        assert hsm.Get(sm.context(), sm, "flag") == (expected_flag, True)
        assert instance.calls == expected_calls

    await hsm.Stop(sm)
    expected_context = None
    assert_context_membership()
    assert hsm.TakeSnapshot(sm.context(), sm).QueueLen == 0


async def _stopped_machine_rejects_event_mutating_operations() -> None:
    class StoppedInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.called = False

        async def work(self):
            self.called = True

    ctx = hsm.Context()
    model = hsm.Define(
        "StoppedRejects",
        hsm.Attribute("flag", False),
        hsm.Operation("work"),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../done")),
            hsm.Transition(hsm.OnSet("flag"), hsm.Target("../done")),
            hsm.Transition(hsm.OnCall("work"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )
    instance = StoppedInstance()
    sm = await hsm.Start(ctx, instance, model)
    await hsm.Stop(instance)

    assert instance.state() == "/StoppedRejects"
    assert ctx.machines() == []

    for operation in (
        hsm.Dispatch(ctx, instance, hsm.Event("go")),
        hsm.Set(ctx, instance, "flag", True),
        hsm.Call(ctx, instance, "work"),
    ):
        try:
            await operation
        except hsm.ValidationError as error:
            assert "started HSM" in str(error)
        else:
            raise AssertionError("operation on stopped HSM should fail")

    try:
        instance.dispatch(hsm.Event("go"))
    except hsm.ValidationError as error:
        assert "started HSM" in str(error)
    else:
        raise AssertionError("instance.dispatch() on stopped HSM should fail")

    assert instance.called is False
    assert instance.state() == "/StoppedRejects"
    assert hsm.TakeSnapshot(ctx, sm).QueueLen == 0


def test_stopped_machine_rejects_event_mutating_operations():
    asyncio.run(_stopped_machine_rejects_event_mutating_operations())


async def _never_started_instance_rejects_event_mutating_conveniences() -> None:
    instance = hsm.Instance()

    try:
        instance.dispatch(hsm.Event("go"))
    except hsm.ValidationError as error:
        assert "missing hsm" in str(error)
    else:
        raise AssertionError("instance.dispatch() on never-started instance should fail")

    try:
        await instance.restart()
    except hsm.ValidationError as error:
        assert "missing hsm" in str(error)
    else:
        raise AssertionError("instance.restart() on never-started instance should fail")

    await instance.stop()
    assert instance.state() == ""
    assert instance.context() is None


def test_never_started_instance_rejects_event_mutating_conveniences():
    asyncio.run(_never_started_instance_rejects_event_mutating_conveniences())


async def _stop_suppresses_exit_action_errors_without_leaving_stale_queue() -> None:
    class StopErrorInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.error_handled = False

    async def failing_exit(ctx, inst, event):
        raise RuntimeError("stop cleanup failed")

    async def error_effect(ctx, inst: StopErrorInstance, event):
        inst.error_handled = True

    model = hsm.Define(
        "StopExitError",
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Exit(failing_exit)),
        hsm.State(
            "error",
            hsm.Transition(hsm.On(hsm.ErrorEvent), hsm.Effect(error_effect)),
        ),
    )
    instance = StopErrorInstance()
    ctx = hsm.Context()
    sm = await hsm.Start(ctx, instance, model)

    await hsm.Stop(instance)

    assert instance.state() == "/StopExitError"
    assert ctx.machines() == []
    assert instance.error_handled is False
    assert hsm.TakeSnapshot(ctx, sm).QueueLen == 0


def test_stop_suppresses_exit_action_errors_without_leaving_stale_queue():
    asyncio.run(_stop_suppresses_exit_action_errors_without_leaving_stale_queue())


async def _stop_suppresses_exit_cancelled_error_without_stale_registration() -> None:
    class StopCancelledInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.exit_seen = False

    async def cancelling_exit(ctx, inst: StopCancelledInstance, event):
        inst.exit_seen = True
        raise asyncio.CancelledError("manual exit cancellation")

    model = hsm.Define(
        "StopExitCancelled",
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Exit(cancelling_exit)),
    )
    instance = StopCancelledInstance()
    ctx = hsm.Context()
    sm = await hsm.Start(ctx, instance, model)

    await hsm.Stop(instance)

    assert instance.exit_seen is True
    assert instance.state() == "/StopExitCancelled"
    assert ctx.machines() == []
    assert hsm.TakeSnapshot(ctx, sm).QueueLen == 0

    try:
        await hsm.Dispatch(ctx, instance, hsm.Event("go"))
    except hsm.ValidationError as error:
        assert "started HSM" in str(error)
    else:
        raise AssertionError("Stop() with manual CancelledError must leave a stopped HSM")


def test_stop_suppresses_exit_cancelled_error_without_stale_registration():
    asyncio.run(_stop_suppresses_exit_cancelled_error_without_stale_registration())


async def _activity_cancellation_cleanup_errors_do_not_dispatch_error_events() -> None:
    class CleanupErrorInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.error_handled = False

    activity_started = asyncio.Event()

    async def cleanup_error_activity(ctx, inst, event):
        activity_started.set()
        try:
            while True:
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            assert ctx.done is True
            raise RuntimeError("activity cleanup failed")

    async def error_effect(ctx, inst: CleanupErrorInstance, event):
        inst.error_handled = True

    model = hsm.Define(
        "ActivityCleanupError",
        hsm.Initial(hsm.Target("active")),
        hsm.State(
            "active",
            hsm.Activity(cleanup_error_activity),
            hsm.Transition(hsm.On("finish"), hsm.Target("../done")),
            hsm.Transition(hsm.On(hsm.ErrorEvent), hsm.Target("../error"), hsm.Effect(error_effect)),
        ),
        hsm.State("done"),
        hsm.State("error"),
    )
    instance = CleanupErrorInstance()
    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)
    await asyncio.wait_for(activity_started.wait(), timeout=1)

    await asyncio.wait_for(hsm.Dispatch(ctx, instance, hsm.Event("finish")), timeout=1)

    assert instance.state() == "/ActivityCleanupError/done"
    assert instance.error_handled is False
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    await hsm.Stop(instance)


def test_activity_cancellation_cleanup_errors_do_not_dispatch_error_events():
    asyncio.run(_activity_cancellation_cleanup_errors_do_not_dispatch_error_events())


async def _completed_activity_removes_active_record_without_exit() -> None:
    class CompletedActivityInstance(hsm.Instance):
        pass

    activity_started = asyncio.Event()
    release_activity = asyncio.Event()

    async def one_shot_activity(ctx, inst: CompletedActivityInstance, event):
        activity_started.set()
        await release_activity.wait()

    model = hsm.Define(
        "CompletedActivityCleanup",
        hsm.Initial(hsm.Target("active")),
        hsm.State(
            "active",
            hsm.Activity(one_shot_activity),
            hsm.Transition(hsm.On("finish"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )
    instance = CompletedActivityInstance()
    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model)
    await asyncio.wait_for(activity_started.wait(), timeout=1)
    active_tasks = [active.task for active in sm._active.values()]
    assert len(active_tasks) == 1

    executed_state = hsm.AfterExecuted(ctx, sm, "/CompletedActivityCleanup/active")
    executed_behavior = hsm.AfterExecuted(
        ctx,
        sm,
        "/CompletedActivityCleanup/active/activity/one_shot_activity/0",
    )
    release_activity.set()
    await asyncio.wait_for(active_tasks[0], timeout=1)
    await asyncio.wait_for(executed_state, timeout=1)
    await asyncio.wait_for(executed_behavior, timeout=1)

    assert instance.state() == "/CompletedActivityCleanup/active"
    assert sm._active == {}
    assert hsm.TakeSnapshot(ctx, sm).QueueLen == 0

    await hsm.Dispatch(ctx, sm, hsm.Event("finish"))
    assert instance.state() == "/CompletedActivityCleanup/done"
    assert sm._active == {}

    await hsm.Stop(sm)


def test_completed_activity_removes_active_record_without_exit():
    asyncio.run(_completed_activity_removes_active_record_without_exit())


async def _after_executed_closes_when_activity_is_cancelled_on_stop() -> None:
    class StoppedActivityInstance(hsm.Instance):
        pass

    activity_started = asyncio.Event()
    activity_cancelled = asyncio.Event()

    async def cancellable_activity(ctx, inst: StoppedActivityInstance, event):
        activity_started.set()
        try:
            await ctx.wait_done()
        finally:
            activity_cancelled.set()

    model = hsm.Define(
        "StoppedActivityExecuted",
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Activity(cancellable_activity)),
    )
    instance = StoppedActivityInstance()
    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model)
    await asyncio.wait_for(activity_started.wait(), timeout=1)

    executed_state = hsm.AfterExecuted(ctx, sm, "/StoppedActivityExecuted/active")
    await hsm.Stop(sm)

    await asyncio.wait_for(activity_cancelled.wait(), timeout=1)
    await asyncio.wait_for(executed_state, timeout=1)
    assert sm._active == {}


def test_after_executed_closes_when_activity_is_cancelled_on_stop():
    asyncio.run(_after_executed_closes_when_activity_is_cancelled_on_stop())


async def _cancelled_start_cleans_up_registration_and_activities() -> None:
    class CancelledStartInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.activity_cancelled = False

    entered_entry = asyncio.Event()
    release_entry = asyncio.Event()
    activity_started = asyncio.Event()

    async def blocking_entry(ctx, inst, event):
        entered_entry.set()
        await release_entry.wait()

    async def startup_activity(ctx, inst: CancelledStartInstance, event):
        activity_started.set()
        try:
            await ctx.wait_done()
        finally:
            inst.activity_cancelled = True

    model = hsm.Define(
        "CancelledStart",
        hsm.Initial(hsm.Target("active/child")),
        hsm.State(
            "active",
            hsm.Activity(startup_activity),
            hsm.State("child", hsm.Entry(blocking_entry)),
        ),
    )

    instance = CancelledStartInstance()
    ctx = hsm.Context()
    start_task = asyncio.create_task(hsm.Start(ctx, instance, model))
    await asyncio.wait_for(activity_started.wait(), timeout=1)
    await asyncio.wait_for(entered_entry.wait(), timeout=1)

    start_task.cancel()
    try:
        await start_task
    except asyncio.CancelledError:
        pass

    assert instance.activity_cancelled is True
    assert instance.state() == "/CancelledStart"
    assert ctx.machines() == []

    try:
        await hsm.Dispatch(ctx, instance, hsm.Event("go"))
    except hsm.ValidationError as error:
        assert "started HSM" in str(error)
    else:
        raise AssertionError("cancelled Start() should leave a stopped HSM")


def test_cancelled_start_cleans_up_registration_and_activities():
    asyncio.run(_cancelled_start_cleans_up_registration_and_activities())


async def _cancelled_start_discards_queued_startup_events() -> None:
    class QueuedStartupInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.entries: list[object] = []

    entered_entry = asyncio.Event()
    release_entry = asyncio.Event()

    async def blocking_entry(ctx, inst: QueuedStartupInstance, event):
        inst.entries.append(event.Data)
        if event.Data == "first":
            await hsm.Set(ctx, inst, "count", 5)
            await hsm.Dispatch(ctx, inst, hsm.Event("queued"))
        entered_entry.set()
        await release_entry.wait()

    model = hsm.Define(
        "CancelledStartQueuedEvents",
        hsm.Attribute("count", 0),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Entry(blocking_entry),
            hsm.Transition(hsm.On("queued"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )

    instance = QueuedStartupInstance()
    ctx = hsm.Context()
    sm = hsm.New(instance, model)

    start_task = asyncio.create_task(hsm.Start(ctx, sm, "first"))
    await asyncio.wait_for(entered_entry.wait(), timeout=1)

    start_task.cancel()
    try:
        await start_task
    except asyncio.CancelledError:
        pass

    assert instance.state() == "/CancelledStartQueuedEvents"
    assert ctx.machines() == []
    assert hsm.TakeSnapshot(ctx, sm).QueueLen == 0
    assert hsm.Get(ctx, sm, "count") == (0, True)

    release_entry.set()
    entered_entry.clear()
    await hsm.Start(ctx, sm, "second")

    assert instance.state() == "/CancelledStartQueuedEvents/idle"
    assert hsm.TakeSnapshot(ctx, sm).QueueLen == 0
    assert instance.entries == ["first", "second"]

    await hsm.Stop(sm)


def test_cancelled_start_discards_queued_startup_events():
    asyncio.run(_cancelled_start_discards_queued_startup_events())


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
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    await hsm.Stop(instance)
    await asyncio.sleep(0)
    sm = getattr(instance, "_Instance__hsm")
    assert sm._active == {}
    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    assert all(future.done() for future in sleeps)


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


async def _stop_resets_dynamic_runtime_state() -> None:
    instance = FuzzInstance()
    ctx = hsm.Context()
    model = hsm.Define(
        "StopReset",
        hsm.Attribute("count", 0),
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
        ),
        hsm.State(
            "outside",
            hsm.Transition(hsm.On("shallow"), hsm.Target("../parent/shallow")),
            hsm.Transition(hsm.On("deep"), hsm.Target("../parent/deep")),
        ),
    )
    sm = await hsm.Started(ctx, instance, model)

    await hsm.Set(ctx, sm, "count", 7)
    await hsm.Dispatch(ctx, sm, hsm.Event("next"))
    await hsm.Dispatch(ctx, sm, hsm.Event("leave"))
    await hsm.Dispatch(ctx, sm, hsm.Event("deep"))

    assert instance.state() == "/StopReset/parent/a/a2"
    assert hsm.Get(ctx, sm, "count") == (7, True)
    assert sm._history_shallow
    assert sm._history_deep

    await hsm.Stop(sm)

    snapshot = hsm.TakeSnapshot(ctx, sm)
    assert snapshot.State == "/StopReset"
    assert snapshot.QueueLen == 0
    assert snapshot.Attributes == {"/StopReset/count": 0}
    assert hsm.Get(ctx, sm, "count") == (0, True)
    assert sm._active == {}
    assert sm._history_shallow == {}
    assert sm._history_deep == {}
    assert sm._runtime_context.done is False
    assert sm._stop_requested is False
    assert sm._restart_requested is None


def test_stop_resets_dynamic_runtime_state():
    asyncio.run(_stop_resets_dynamic_runtime_state())


@st.composite
def named_operation_guard_specs(draw):
    guard_count = draw(st.integers(min_value=1, max_value=8))
    guards = tuple(draw(st.lists(st.booleans(), min_size=guard_count, max_size=guard_count)))
    return guards


async def _drive_named_operation_guard_spec(guards: tuple[bool, ...]) -> None:
    class NamedOperationGuardInstance(FuzzInstance):
        def __init__(self) -> None:
            super().__init__()
            self.trace: list[str] = []

    instance = NamedOperationGuardInstance()
    operations = []
    transitions = []
    states = [hsm.State("s0")]

    for index, guard_value in enumerate(guards):
        guard_name = f"guard_{index}"
        effect_name = f"effect_{index}"
        target_name = f"s{index + 1}"

        async def guard(ctx, inst: NamedOperationGuardInstance, event, *, value=guard_value, name=guard_name):
            assert isinstance(ctx, hsm.Context)
            assert event.Name == "go"
            inst.trace.append(name)
            return value

        async def effect(ctx, inst: NamedOperationGuardInstance, event, *, name=effect_name):
            assert isinstance(ctx, hsm.Context)
            assert event.Name == "go"
            inst.trace.append(name)

        operations.append(hsm.operation(guard_name, guard))
        operations.append(hsm.operation(effect_name, effect))
        transitions.append(
            hsm.transition(
                hsm.on("go"),
                hsm.guard(guard_name),
                hsm.effect(effect_name),
                hsm.target(f"../{target_name}"),
            )
        )
        states.append(hsm.state(target_name))

    model = hsm.define(
        "NamedOperationGuardFuzz",
        *operations,
        hsm.initial(hsm.target("s0")),
        hsm.state("s0", *transitions),
        *states[1:],
    )

    ctx = hsm.context()
    await hsm.start(ctx, instance, model)
    await hsm.dispatch(ctx, instance, hsm.event("go"))

    try:
        accepted_index = guards.index(True)
    except ValueError:
        accepted_index = -1

    if accepted_index == -1:
        assert instance.state() == "/NamedOperationGuardFuzz/s0"
        assert instance.trace == [f"guard_{index}" for index in range(len(guards))]
    else:
        assert instance.state() == f"/NamedOperationGuardFuzz/s{accepted_index + 1}"
        assert instance.trace == [
            *(f"guard_{index}" for index in range(accepted_index + 1)),
            f"effect_{accepted_index}",
        ]

    assert hsm.take_snapshot(ctx, instance).queue_len == 0
    await hsm.stop(instance)


@given(named_operation_guard_specs())
@settings(max_examples=80, deadline=None, derandomize=True)
def test_named_operation_guard_and_effect_fuzz(guards: tuple[bool, ...]):
    asyncio.run(_drive_named_operation_guard_spec(guards))


async def _named_operation_activity_and_error_stress(rounds: int) -> None:
    class NamedOperationRuntimeInstance(FuzzInstance):
        def __init__(self) -> None:
            super().__init__()
            self.started = 0
            self.cancelled = 0
            self.errors: list[str] = []
            self.entries = 0
            self.releases: asyncio.Queue[asyncio.Event] = asyncio.Queue()

        async def run_activity(self, event) -> None:
            self.started += 1
            release = asyncio.Event()
            await self.releases.put(release)
            try:
                await release.wait()
            except asyncio.CancelledError:
                self.cancelled += 1
                raise

    async def fail_effect(ctx, inst: NamedOperationRuntimeInstance, event) -> None:
        raise RuntimeError(f"named failure {event.Name}")

    async def record_error(ctx, inst: NamedOperationRuntimeInstance, event) -> None:
        inst.errors.append(str(event.Data))

    async def enter_error(ctx, inst: NamedOperationRuntimeInstance, event) -> None:
        inst.entries += 1

    model = hsm.define(
        "NamedOperationRuntimeStress",
        hsm.operation("run_activity"),
        hsm.operation("fail_effect", fail_effect),
        hsm.operation("record_error", record_error),
        hsm.operation("enter_error", enter_error),
        hsm.initial(hsm.target("active")),
        hsm.state(
            "active",
            hsm.activity("run_activity"),
            hsm.transition(hsm.on("fail"), hsm.effect("fail_effect")),
            hsm.transition(
                hsm.on(hsm.ErrorEvent),
                hsm.target("../error"),
                hsm.effect("record_error"),
            ),
        ),
        hsm.state("error", hsm.entry("enter_error"), hsm.transition(hsm.on("reset"), hsm.target("../active"))),
    )

    ctx = hsm.context()
    instance = NamedOperationRuntimeInstance()
    await hsm.start(ctx, instance, model)

    for index in range(rounds):
        release = await asyncio.wait_for(instance.releases.get(), timeout=1)
        assert hsm.take_snapshot(ctx, instance).queue_len == 0
        entered_error = hsm.AfterEntry(ctx, instance, "/NamedOperationRuntimeStress/error")
        await hsm.dispatch(ctx, instance, hsm.event("fail"))
        await asyncio.wait_for(entered_error, timeout=1)
        assert instance.state() == "/NamedOperationRuntimeStress/error"
        assert instance.errors[-1] == "named failure fail"
        assert instance.entries == index + 1
        assert instance.cancelled == index + 1
        assert release.is_set() is False
        assert hsm.take_snapshot(ctx, instance).queue_len == 0
        await hsm.dispatch(ctx, instance, hsm.event("reset"))
        assert instance.state() == "/NamedOperationRuntimeStress/active"

    await hsm.stop(instance)
    assert instance.started == rounds + 1
    assert instance.cancelled == rounds + 1
    assert hsm.take_snapshot(ctx, instance).queue_len == 0


def test_named_operation_activity_and_error_stress():
    asyncio.run(_named_operation_activity_and_error_stress(rounds=50))


async def _when_waitable_cancellation_stress(rounds: int) -> None:
    class WhenWaitableInstance(FuzzInstance):
        def __init__(self) -> None:
            super().__init__()
            self.armed: asyncio.Queue[asyncio.Future[None]] = asyncio.Queue()
            self.cancelled = 0
            self.fired = 0

    async def wait_for_external_signal(ctx, inst: WhenWaitableInstance, event):
        future = asyncio.get_running_loop().create_future()
        await inst.armed.put(future)

        async def waiter() -> None:
            try:
                await future
            except asyncio.CancelledError:
                inst.cancelled += 1
                raise

        return waiter()

    async def record_fire(ctx, inst: WhenWaitableInstance, event) -> None:
        inst.fired += 1

    model = hsm.define(
        "WhenWaitableStress",
        hsm.initial(hsm.target("waiting")),
        hsm.state(
            "waiting",
            hsm.transition(hsm.when(wait_for_external_signal), hsm.target("../fired"), hsm.effect(record_fire)),
            hsm.transition(hsm.on("cancel"), hsm.target("../cancelled")),
        ),
        hsm.state("fired", hsm.transition(hsm.on("reset"), hsm.target("../waiting"))),
        hsm.state("cancelled", hsm.transition(hsm.on("reset"), hsm.target("../waiting"))),
    )

    ctx = hsm.context()
    instance = WhenWaitableInstance()
    await hsm.start(ctx, instance, model)

    for index in range(rounds):
        future = await asyncio.wait_for(instance.armed.get(), timeout=1)
        assert hsm.take_snapshot(ctx, instance).queue_len == 0
        if index % 2 == 0:
            entered_cancelled = hsm.AfterEntry(ctx, instance, "/WhenWaitableStress/cancelled")
            await hsm.dispatch(ctx, instance, hsm.event("cancel"))
            await asyncio.wait_for(entered_cancelled, timeout=1)
            assert instance.state() == "/WhenWaitableStress/cancelled"
            assert instance.cancelled >= (index // 2) + 1
            if not future.done():
                future.set_result(None)
            await asyncio.sleep(0)
            assert instance.state() == "/WhenWaitableStress/cancelled"
            assert instance.fired == index // 2
        else:
            entered_fired = hsm.AfterEntry(ctx, instance, "/WhenWaitableStress/fired")
            future.set_result(None)
            await asyncio.wait_for(entered_fired, timeout=1)
            assert instance.state() == "/WhenWaitableStress/fired"
            assert instance.fired == (index // 2) + 1
        assert hsm.take_snapshot(ctx, instance).queue_len == 0
        await hsm.dispatch(ctx, instance, hsm.event("reset"))
        assert instance.state() == "/WhenWaitableStress/waiting"

    stop_future = await asyncio.wait_for(instance.armed.get(), timeout=1)
    await hsm.stop(instance)
    if not stop_future.done():
        stop_future.set_result(None)
    await asyncio.sleep(0)
    assert instance.state() == "/WhenWaitableStress"
    assert hsm.take_snapshot(ctx, instance).queue_len == 0


def test_when_waitable_cancellation_stress():
    asyncio.run(_when_waitable_cancellation_stress(rounds=50))


async def _when_owned_future_is_not_cancelled_on_state_exit() -> None:
    class OwnedFutureInstance(FuzzInstance):
        def __init__(self) -> None:
            super().__init__()
            self.armed: asyncio.Queue[asyncio.Future[None]] = asyncio.Queue()
            self.fired = 0

    async def owned_future(ctx, inst: OwnedFutureInstance, event):
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        await inst.armed.put(future)
        return future

    async def record_fire(ctx, inst: OwnedFutureInstance, event) -> None:
        inst.fired += 1

    model = hsm.define(
        "WhenOwnedFuture",
        hsm.initial(hsm.target("waiting")),
        hsm.state(
            "waiting",
            hsm.transition(hsm.when(owned_future), hsm.target("../fired"), hsm.effect(record_fire)),
            hsm.transition(hsm.on("cancel"), hsm.target("../cancelled")),
        ),
        hsm.state("fired"),
        hsm.state("cancelled"),
    )

    ctx = hsm.context()
    instance = OwnedFutureInstance()
    await hsm.start(ctx, instance, model)

    future = await asyncio.wait_for(instance.armed.get(), timeout=1)
    entered_cancelled = hsm.AfterEntry(ctx, instance, "/WhenOwnedFuture/cancelled")
    await hsm.dispatch(ctx, instance, hsm.event("cancel"))
    await asyncio.wait_for(entered_cancelled, timeout=1)

    assert future.cancelled() is False
    assert future.done() is False
    future.set_result(None)
    await asyncio.sleep(0)

    assert instance.state() == "/WhenOwnedFuture/cancelled"
    assert instance.fired == 0
    assert hsm.take_snapshot(ctx, instance).queue_len == 0

    await hsm.stop(instance)


def test_when_owned_future_is_not_cancelled_on_state_exit():
    asyncio.run(_when_owned_future_is_not_cancelled_on_state_exit())
