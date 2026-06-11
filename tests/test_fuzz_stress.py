import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import hsm


def _runtime_context() -> hsm.Context:
    return hsm.Context().WithValue(hsm.Keys.Instances, {})


async def _wait_for(predicate: Callable[[], bool], message: str) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError(message)


class FuzzInstance(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.log: list[Any] = []


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
        draw(
            st.lists(
                st.integers(min_value=0, max_value=event_count - 1),
                min_size=0,
                max_size=80,
            )
        )
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
    ctx = _runtime_context()
    await hsm.Started(ctx, instance, _flat_model(spec))

    expected_state = 0
    assert instance.state() == "/FuzzFlat/s0"

    for event_index in spec.trace:
        before = instance.take_snapshot()
        assert before.State == f"/FuzzFlat/s{expected_state}"
        assert before.QueueLen == 0

        await hsm.Dispatch(ctx, instance, hsm.Event(name=f"e{event_index}"))

        maybe_target = spec.transitions[expected_state][event_index]
        if maybe_target is not None:
            expected_state = maybe_target
        after = instance.take_snapshot()
        assert after.State == f"/FuzzFlat/s{expected_state}"
        assert after.QueueLen == 0

    await instance.stop(instance.context())


@given(flat_specs())
@settings(
    max_examples=80,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_flat_state_machine_fuzz(spec: FlatSpec):
    asyncio.run(_drive_flat_spec(spec))


@st.composite
def guarded_specs(draw):
    guard_count = draw(st.integers(min_value=1, max_value=8))
    guards = tuple(
        draw(st.lists(st.booleans(), min_size=guard_count, max_size=guard_count))
    )
    targets = tuple(
        draw(
            st.lists(
                st.integers(min_value=1, max_value=4),
                min_size=guard_count,
                max_size=guard_count,
            )
        )
    )
    return guards, targets


def _guarded_model(guards: tuple[bool, ...], targets: tuple[int, ...]) -> hsm.Model:
    transitions = []
    for index, (value, target) in enumerate(zip(guards, targets, strict=True)):

        def guard(ctx, inst: FuzzInstance, event, *, value=value, index=index):
            inst.log.append(f"guard:{index}")
            return value

        guard.__name__ = f"guard_{index}"
        transitions.append(
            hsm.Transition(
                hsm.On("go"),
                hsm.Guard(guard),
                hsm.Target(f"../s{target}"),
            )
        )
    states = [hsm.State("s0", *transitions)]
    states.extend(hsm.State(f"s{index}") for index in range(1, 5))
    return hsm.Define("FuzzGuards", hsm.Initial(hsm.Target("s0")), *states)


async def _drive_guarded_spec(guards: tuple[bool, ...], targets: tuple[int, ...]) -> None:
    instance = FuzzInstance()
    ctx = _runtime_context()
    await hsm.Started(ctx, instance, _guarded_model(guards, targets))
    await hsm.Dispatch(ctx, instance, hsm.Event(name="go"))

    first_true = next((index for index, value in enumerate(guards) if value), None)
    expected_state = 0 if first_true is None else targets[first_true]
    expected_guards = len(guards) if first_true is None else first_true + 1

    assert instance.state() == f"/FuzzGuards/s{expected_state}"
    assert instance.log == [f"guard:{index}" for index in range(expected_guards)]


@given(guarded_specs())
@settings(max_examples=80, deadline=None, derandomize=True)
def test_guard_order_fuzz(spec):
    guards, targets = spec
    asyncio.run(_drive_guarded_spec(guards, targets))


@given(
    st.lists(
        st.sampled_from(("next", "leave", "reset", "noop")),
        min_size=0,
        max_size=60,
    )
)
@settings(max_examples=60, deadline=None, derandomize=True)
def test_hierarchical_transition_trace_fuzz(trace: list[str]):
    asyncio.run(_drive_hierarchical_trace(tuple(trace)))


async def _drive_hierarchical_trace(trace: tuple[str, ...]) -> None:
    instance = FuzzInstance()

    def record(label: str):
        def action(ctx, inst: FuzzInstance, event):
            inst.log.append(label)

        action.__name__ = label.replace(":", "_")
        return action

    model = hsm.Define(
        "FuzzHierarchy",
        hsm.Initial(hsm.Target("parent/a")),
        hsm.State(
            "parent",
            hsm.Entry(record("parent:entry")),
            hsm.Exit(record("parent:exit")),
            hsm.State(
                "a",
                hsm.Entry(record("a:entry")),
                hsm.Exit(record("a:exit")),
                hsm.Transition(hsm.On("next"), hsm.Target("../b")),
            ),
            hsm.State(
                "b",
                hsm.Entry(record("b:entry")),
                hsm.Exit(record("b:exit")),
                hsm.Transition(hsm.On("leave"), hsm.Target("../../outside")),
            ),
        ),
        hsm.State(
            "outside",
            hsm.Transition(hsm.On("reset"), hsm.Target("../parent/a")),
        ),
    )

    ctx = _runtime_context()
    await hsm.Started(ctx, instance, model)
    expected = "/FuzzHierarchy/parent/a"

    for name in trace:
        await hsm.Dispatch(ctx, instance, hsm.Event(name=name))
        if expected.endswith("/a") and name == "next":
            expected = "/FuzzHierarchy/parent/b"
        elif expected.endswith("/b") and name == "leave":
            expected = "/FuzzHierarchy/outside"
        elif expected.endswith("/outside") and name == "reset":
            expected = "/FuzzHierarchy/parent/a"
        assert instance.state() == expected
        assert instance.take_snapshot().QueueLen == 0

    await instance.stop(instance.context())


def _toggle_model(name: str = "ToggleStress") -> hsm.Model:
    return hsm.Define(
        name,
        hsm.Initial(hsm.Target("a")),
        hsm.State("a", hsm.Transition(hsm.On("toggle"), hsm.Target("../b"))),
        hsm.State("b", hsm.Transition(hsm.On("toggle"), hsm.Target("../a"))),
    )


def test_concurrent_dispatch_stress():
    asyncio.run(_concurrent_dispatch_stress(iterations=80))


async def _concurrent_dispatch_stress(iterations: int) -> None:
    instance = FuzzInstance()
    ctx = _runtime_context()
    await hsm.Started(ctx, instance, _toggle_model())

    await asyncio.gather(
        *(hsm.Dispatch(ctx, instance, hsm.Event(name="toggle")) for _ in range(iterations))
    )

    expected = "/ToggleStress/a" if iterations % 2 == 0 else "/ToggleStress/b"
    assert instance.state() == expected
    assert instance.take_snapshot().QueueLen == 0

    await instance.stop(instance.context())


def test_dispatch_all_and_dispatch_to_stress():
    asyncio.run(_dispatch_all_and_dispatch_to_stress(machine_count=12))


async def _dispatch_all_and_dispatch_to_stress(machine_count: int) -> None:
    model = hsm.Define(
        "BroadcastStress",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../done"))),
        hsm.State("done", hsm.Transition(hsm.On("reset"), hsm.Target("../idle"))),
    )
    ctx = _runtime_context()
    instances: list[FuzzInstance] = []
    for index in range(machine_count):
        instance = FuzzInstance()
        instances.append(instance)
        await hsm.Started(ctx, instance, model, hsm.Config(ID=f"machine-{index}"))

    await hsm.DispatchTo(ctx, hsm.Event(name="go"), "machine-0", "machine-2")
    assert instances[0].state() == "/BroadcastStress/done"
    assert instances[2].state() == "/BroadcastStress/done"
    assert all(
        instance.state() == "/BroadcastStress/idle"
        for index, instance in enumerate(instances)
        if index not in (0, 2)
    )

    await hsm.DispatchAll(ctx, hsm.Event(name="go"))
    assert all(instance.state() == "/BroadcastStress/done" for instance in instances)

    group = hsm.Group(*instances)
    await hsm.Dispatch(ctx, group, hsm.Event(name="reset"))
    assert group.state() == ["/BroadcastStress/idle"] * machine_count
    assert len(group.take_snapshot()) == machine_count

    await group.stop(group.context())


def test_event_data_and_metadata_reference_contract_stress():
    asyncio.run(_event_data_and_metadata_reference_contract_stress(rounds=40))


async def _event_data_and_metadata_reference_contract_stress(rounds: int) -> None:
    class EventContractInstance(FuzzInstance):
        pass

    def touch(ctx, inst: EventContractInstance, event: hsm.Event) -> None:
        assert isinstance(event.data, dict)
        event.data["seen"].append(inst.take_snapshot().ID)
        event.metadata["last"] = inst.take_snapshot().ID
        inst.log.append((event.data, event.metadata))

    model = hsm.Define(
        "EventContractStress",
        hsm.Initial(hsm.Target("ready")),
        hsm.State("ready", hsm.Transition(hsm.On("touch"), hsm.Effect(touch))),
    )

    ctx = _runtime_context()
    instance = EventContractInstance()
    await hsm.Started(ctx, instance, model, hsm.Config(ID="event-contract"))

    for index in range(rounds):
        data = {"seen": []}
        metadata = {"trace": index}
        event = hsm.Event(name="touch", data=data, metadata=metadata)
        await hsm.Dispatch(ctx, instance, event)
        assert data == {"seen": ["event-contract"]}
        assert metadata == {"trace": index, "last": "event-contract"}
        assert instance.log[-1] == (data, metadata)

    await instance.stop(instance.context())


@given(st.lists(st.integers(min_value=-20, max_value=20), min_size=0, max_size=80))
@settings(max_examples=80, deadline=None, derandomize=True)
def test_attribute_set_event_fuzz(values: list[int]):
    asyncio.run(_drive_attribute_set_trace(tuple(values)))


async def _drive_attribute_set_trace(values: tuple[int, ...]) -> None:
    instance = FuzzInstance()

    def record_change(ctx, inst: FuzzInstance, event: hsm.Event) -> None:
        assert isinstance(event.data, hsm.AttributeChange)
        inst.log.append((event.data.old_value, event.data.value))

    model = hsm.Define(
        "AttributeSetFuzz",
        hsm.Attribute("count", 0),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.OnSet("count"), hsm.Effect(record_change))),
    )

    ctx = _runtime_context()
    await hsm.Started(ctx, instance, model)
    current = 0
    expected: list[tuple[int, int]] = []

    for value in values:
        await hsm.Set(ctx, instance, "count", value)
        if value != current:
            expected.append((current, value))
            current = value
        assert hsm.Get(ctx, instance, "count") == (current, True)

    assert instance.log == expected

    await instance.stop(instance.context())


def test_concurrent_call_stress():
    asyncio.run(_concurrent_call_stress(rounds=60))


async def _concurrent_call_stress(rounds: int) -> None:
    class RuntimeInstance(FuzzInstance):
        def add(self, value: int) -> int:
            self.log.append(f"add:{value}")
            return value + 1

    def record_call(ctx, inst: RuntimeInstance, event: hsm.Event) -> None:
        assert isinstance(event.data, hsm.CallData)
        inst.log.append(event.data.args[0])

    model = hsm.Define(
        "CallStress",
        hsm.Operation("add"),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.OnCall("add"), hsm.Effect(record_call))),
    )

    ctx = _runtime_context()
    instance = RuntimeInstance()
    await hsm.Started(ctx, instance, model)

    results = await asyncio.gather(
        *(hsm.Call(ctx, instance, "add", index) for index in range(rounds))
    )
    assert results == [index + 1 for index in range(rounds)]
    await _wait_for(
        lambda: len([item for item in instance.log if isinstance(item, int)]) == rounds,
        "oncall events did not finish",
    )
    assert sorted(item for item in instance.log if isinstance(item, int)) == list(range(rounds))

    await instance.stop(instance.context())


def test_deferred_events_replay_without_extra_dispatch():
    asyncio.run(_deferred_events_replay_without_extra_dispatch())


async def _deferred_events_replay_without_extra_dispatch() -> None:
    instance = FuzzInstance()
    model = hsm.Define(
        "DeferredStress",
        hsm.Initial(hsm.Target("holding")),
        hsm.State(
            "holding",
            hsm.Defer("work"),
            hsm.Transition(hsm.On("release"), hsm.Target("../ready")),
        ),
        hsm.State("ready", hsm.Transition(hsm.On("work"), hsm.Target("../done"))),
        hsm.State("done"),
    )

    ctx = _runtime_context()
    await hsm.Started(ctx, instance, model)
    await hsm.Dispatch(ctx, instance, hsm.Event(name="work"))
    assert instance.state() == "/DeferredStress/holding"
    assert instance.take_snapshot().QueueLen == 1

    await hsm.Dispatch(ctx, instance, hsm.Event(name="release"))
    assert instance.state() == "/DeferredStress/done"
    assert instance.take_snapshot().QueueLen == 0

    await instance.stop(instance.context())


def test_activity_cancellation_stress():
    asyncio.run(_activity_cancellation_stress())


async def _activity_cancellation_stress() -> None:
    instance = FuzzInstance()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def activity(ctx, inst: FuzzInstance, event: hsm.Event) -> None:
        started.set()
        try:
            await asyncio.wrap_future(ctx.Done())
        finally:
            cancelled.set()

    model = hsm.Define(
        "ActivityCancelStress",
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Activity(activity)),
    )

    await hsm.Started(hsm.Context(), instance, model)
    await asyncio.wait_for(started.wait(), timeout=1)
    await instance.stop(instance.context())
    await asyncio.wait_for(cancelled.wait(), timeout=1)
    assert instance.state() == "/ActivityCancelStress"


def test_timer_restart_cancellation_stress():
    asyncio.run(_timer_restart_cancellation_stress())


async def _timer_restart_cancellation_stress() -> None:
    sleeps: list[asyncio.Future[None]] = []

    async def sleep(duration: timedelta) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        sleeps.append(future)
        await future

    def delay(ctx, inst, event):
        return timedelta(milliseconds=10)

    model = hsm.Define(
        "TimerStress",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State("waiting", hsm.Transition(hsm.After(delay), hsm.Target("../done"))),
        hsm.State("done"),
    )
    instance = FuzzInstance()
    await hsm.Started(
        hsm.Context(),
        instance,
        model,
        hsm.Config(Clock=hsm.Clock(sleep=sleep)),
    )
    await _wait_for(lambda: len(sleeps) == 1, "timer was not scheduled")
    await instance.restart(instance.context())
    await _wait_for(lambda: len(sleeps) == 2, "timer was not rescheduled")
    sleeps[-1].set_result(None)
    await _wait_for(
        lambda: instance.state() == "/TimerStress/done",
        "timer transition did not fire",
    )

    await instance.stop(instance.context())
