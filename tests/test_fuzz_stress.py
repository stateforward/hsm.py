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
