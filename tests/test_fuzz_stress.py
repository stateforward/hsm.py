import asyncio
from dataclasses import dataclass

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
