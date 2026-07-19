import asyncio
import dataclasses
import re
from datetime import datetime, timedelta

import pytest

import hsm.hsm as core
from hsm import generic


class CoverageInstance(core.Instance):
    def __init__(self):
        super().__init__()
        self.log: list[str] = []

    def double(self, value: int) -> int:
        self.log.append(f"double:{value}")
        return value * 2


def test_model_indexes_and_public_helpers():
    model = core.Define(
        "CoverageIndexes",
        core.Initial(core.Target("parent")),
        core.State(
            "parent",
            core.Initial(core.Target("idle")),
            core.State(
                "idle",
                core.Transition(core.On("go"), core.Target("../done")),
            ),
            core.State("done"),
            core.ShallowHistory("memory", core.Transition(core.Target("idle"))),
        ),
    )

    assert core.LCA("/CoverageIndexes/parent/idle", "/CoverageIndexes/parent/done") == "/CoverageIndexes/parent"
    assert core.IsAncestor("/CoverageIndexes/parent", "/CoverageIndexes/parent/idle")
    assert core.kind.Is(core.StateKind, core.VertexKind)
    assert "go" in model.events
    assert "/CoverageIndexes/parent/idle" in model.transition_map
    assert (
        "/CoverageIndexes/parent",
        "/CoverageIndexes/parent/idle",
    ) in model.history_paths


def test_context_config_and_queue_public_contracts():
    parent = core.context.new_context().WithValue("request", "root")
    child, cancel = parent.WithCancel()
    assert child.Value("request") == "root"
    assert child.Deadline() == (None, False)
    assert child.deadline() == (None, False)
    assert child.Err() is None
    assert child.err() is None
    cancel()
    assert child.Done().done()
    assert child.Err() is core.context.CanceledError
    child.cancel()

    queue = core.MultiQueue()
    assert queue.push(core.Event(name="regular")) == (None,)
    assert queue.push(core.Event(name="complete", kind=core.CompletionEventKind)) == (None,)
    assert queue.len() == (2, None)
    event, ok, error = queue.pop()
    assert ok and error is None and event.name == "complete"
    event, ok, error = queue.pop()
    assert ok and error is None and event.name == "regular"
    queue.clear()

    clock = core.Clock()
    config = core.Config(ID="machine-1", Name="/Alias", Data={"boot": True}, Clock=clock, Queue=queue)
    assert config.id == "machine-1"
    assert config.name == "/Alias"
    assert config.data == {"boot": True}
    assert config.clock is clock
    assert config.queue is queue
    assert config.ID == "machine-1"
    assert config.Name == "/Alias"
    assert config.Data == {"boot": True}
    assert config.Clock is clock
    assert config.Queue is queue
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.id = "machine-2"

    class MissingPop:
        def push(self, event):
            return (None,)

        def len(self):
            return (0, None)

    with pytest.raises(TypeError, match="callable pop"):
        core.MultiQueue(MissingPop())

    class FailingFifo(core.Fifo):
        def push(self, event):
            return (None,)

        def pop(self):
            raise RuntimeError("pop failed")

        def len(self):
            raise RuntimeError("len failed")

    failing_queue = core.MultiQueue(FailingFifo())
    event, ok, error = failing_queue.pop()
    assert event == core.Event()
    assert ok is False
    assert isinstance(error, RuntimeError)
    count, error = failing_queue.len()
    assert count == 0
    assert isinstance(error, RuntimeError)


def test_value_object_properties_and_generic_helpers():
    element = core.Element(id="element-1", qualified_name="/Root/child")
    assert element.Kind() == core.ElementKind
    assert element.ID() == "element-1"
    assert element.QualifiedName() == "/Root/child"
    assert element.owner() == "/Root"
    assert element.name() == "child"
    assert core.Element(qualified_name="/").owner() == ""
    assert core.StateElement(qualified_name="/Root/state").Kind() == core.StateKind

    metadata = {"trace": "abc"}
    event = core.Event(
        name="go",
        data={"value": 1},
        kind=core.TimeEventKind,
        id="event-1",
        source="source",
        target="target",
        schema=dict,
        metadata=metadata,
    )
    assert event.Name == "go"
    assert event.Data == {"value": 1}
    assert event.ID == "event-1"
    assert event.Source == "source"
    assert event.Target == "target"
    assert event.Kind == core.TimeEventKind
    assert event.Schema is dict
    assert event.with_data(2).Data == 2
    event_with_id = event.with_data_and_id(3, "event-2")
    assert event_with_id.Data == 3
    assert event_with_id.ID == "event-2"
    assert event_with_id.metadata is metadata
    assert core.CompletionEvent("done").Kind == core.CompletionEventKind

    change = core.AttributeChange(name="count", old_value=1, value=2)
    assert change.Name == "count"
    assert change.Old == 1
    assert change.New == 2
    assert change.Value == 2

    snapshot = core.Snapshot(
        ID="machine",
        QualifiedName="/Machine",
        State="/Machine/idle",
        Attributes={"x": 1},
        QueueLen=2,
    )
    assert snapshot.id == "machine"
    assert snapshot.qualified_name == "/Machine"
    assert snapshot.state == "/Machine/idle"
    assert snapshot.attributes == {"x": 1}
    assert snapshot.queue_len == 2
    assert snapshot.transitions == ()

    queue = generic.Queue[str]()
    assert queue.pop() == (None, False, None)
    assert queue.push("one") == (None,)
    assert queue.len() == (1, None)
    assert queue.pop() == ("one", True, None)
    queue.push("two")
    queue.clear()
    assert queue.len() == (0, None)

    values = generic.Map[str, int]()
    assert values.load("missing") == (None, False)
    values.store("one", 1)
    assert values.load("one") == (1, True)
    assert values.swap("two", 2) == (None, False)
    assert values.swap("two", 3) == (2, True)
    assert dict(values.items()) == {"one": 1, "two": 3}
    values.delete("one")
    assert values.load("one") == (None, False)
    values.clear()
    assert values.items() == ()


@pytest.mark.asyncio
async def test_generic_awaitable_and_clock_timer_branches():
    awaitable = generic.Awaitable[int]()
    waiter = asyncio.create_task(awaitable.wait())
    await asyncio.sleep(0)
    awaitable.set_result(4)
    awaitable.set_result(5)
    assert await waiter == 4
    assert await awaitable == 4
    assert awaitable.done()
    assert awaitable.result() == 4
    assert awaitable.exception() is None

    failed = generic.Awaitable[int]()
    failed.set_exception(RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        await failed.wait()
    assert isinstance(failed.exception(), RuntimeError)

    cancelled = generic.Awaitable[int]()
    assert cancelled.cancel()
    assert cancelled.cancelled()

    timer = core.Timer(timedelta(milliseconds=0))
    assert isinstance(await timer, datetime)
    assert timer.Stop() is False
    assert timer.Reset(timedelta(seconds=1)) is False
    assert timer.Stop() is True
    with pytest.raises(asyncio.CancelledError):
        await timer

    sleep_calls: list[timedelta] = []

    def sync_sleep(duration: timedelta) -> None:
        sleep_calls.append(duration)

    clock = core.Clock(Sleep=sync_sleep)
    await clock.Sleep(timedelta(milliseconds=1))
    assert sleep_calls == [timedelta(milliseconds=1)]
    assert isinstance(await clock.After(timedelta(milliseconds=1)), datetime)

    now = datetime.now()
    after_clock = core.Clock(After=lambda duration: now)
    assert await after_clock.After(timedelta(milliseconds=1)) is now

    async def async_after(duration: timedelta) -> datetime:
        return now

    async_after_clock = core.Clock(After=async_after)
    assert await async_after_clock.After(timedelta(milliseconds=1)) is now

    custom_timer = core.Timer(timedelta(seconds=1))
    timer_clock = core.Clock(NewTimer=lambda duration: custom_timer)
    assert timer_clock.NewTimer(timedelta(seconds=1)) is custom_timer
    custom_timer.Stop()


@pytest.mark.asyncio
async def test_lifecycle_set_call_and_snapshot_contracts():
    instance = CoverageInstance()

    def entry(ctx: core.Context, inst: CoverageInstance, event: core.Event) -> None:
        inst.log.append(f"entry:{event.data}")

    def on_set(ctx: core.Context, inst: CoverageInstance, event: core.Event) -> None:
        assert isinstance(event.data, core.AttributeChange)
        inst.log.append(f"set:{event.data.value}")

    def on_call(ctx: core.Context, inst: CoverageInstance, event: core.Event) -> None:
        assert isinstance(event.data, core.CallData)
        inst.log.append(event.data.name)

    model = core.Define(
        "RuntimeCoverage",
        core.Attribute("flag", False),
        core.Attribute("payload", {"items": []}),
        core.Operation("double"),
        core.Initial(core.Target("idle")),
        core.State(
            "idle",
            core.Entry(entry),
            core.Transition(core.OnSet("flag"), core.Target("../changed"), core.Effect(on_set)),
        ),
        core.State(
            "changed",
            core.Transition(core.OnCall("double"), core.Target("../called"), core.Effect(on_call)),
        ),
        core.State("called"),
    )

    ctx = core.context.new_context().WithValue(core.Keys.Instances, {})
    await core.Started(ctx, instance, model, core.Config(ID="runtime", Data="boot"))
    assert instance.state() == "/RuntimeCoverage/idle"
    assert instance.log == ["entry:boot"]
    assert core.Get(ctx, instance, "flag") == (False, True)

    assert await core.Set(ctx, instance, "flag", True) is None
    assert instance.state() == "/RuntimeCoverage/changed"
    assert core.Get(ctx, instance, "flag") == (True, True)
    assert core.Get(instance.context(), None, "flag") == (True, True)

    result = await core.Call(ctx, instance, "double", 7)
    for _ in range(100):
        if instance.state() == "/RuntimeCoverage/called":
            break
        await asyncio.sleep(0)
    assert result == 14
    assert instance.log == ["entry:boot", "set:True", "double:7", "/RuntimeCoverage/double"]
    assert instance.state() == "/RuntimeCoverage/called"

    assert await core.Call(instance.context(), None, "double", 3) == 6
    await core.Set(instance.context(), None, "flag", False)
    assert core.Get(instance.context(), None, "flag") == (False, True)

    payload = {"items": ["one"]}
    await core.Set(ctx, instance, "payload", payload)
    snapshot = core.TakeSnapshot(ctx, instance)
    payload["items"].append("two")
    assert snapshot.Attributes["/RuntimeCoverage/payload"] is payload
    assert snapshot.Attributes["/RuntimeCoverage/payload"]["items"] == ["one", "two"]
    with pytest.raises(TypeError):
        snapshot.Attributes["/RuntimeCoverage/payload"] = {}

    await core.Stop(instance)


@pytest.mark.asyncio
async def test_not_started_instance_and_top_level_error_contracts():
    instance = CoverageInstance()
    ctx = core.context.new_context()

    assert instance.state() == ""
    assert isinstance(instance.context(), core.Context)
    assert isinstance(instance.clock(), core.DefaultClock)
    assert instance.get("missing") == (None, False)
    assert instance.take_snapshot() == core.Snapshot()

    dispatch = instance.dispatch(ctx, core.Event(name="go"))
    with pytest.raises(RuntimeError, match="dispatch requires"):
        await dispatch

    started = CoverageInstance()
    core.New(
        started,
        core.Define(
            "NotStartedDispatchCoverage",
            core.Initial(core.Target("idle")),
            core.State("idle"),
        ),
    )
    dispatch = core.Dispatch(ctx, started, core.Event(name="go"))
    with pytest.raises(RuntimeError, match="started HSM"):
        await dispatch

    with pytest.raises(RuntimeError, match="initialized instance"):
        await instance.start(ctx)
    with pytest.raises(RuntimeError) as raised:
        await instance.set("flag", True)
    assert re.fullmatch(
        rf"{re.escape(__file__)}:\d+: operation requires a started HSM",
        str(raised.value),
    )
    with pytest.raises(RuntimeError, match="started HSM"):
        await instance.call("work")
    with pytest.raises(RuntimeError, match="started HSM"):
        await instance.restart(ctx)
    assert await instance.stop(ctx) is None
    with pytest.raises(RuntimeError, match="take snapshot requires"):
        core.TakeSnapshot(ctx, instance)

    assert core.Get(None, None, "missing") == (None, False)
    with pytest.raises(RuntimeError, match="started HSM"):
        await core.Set(None, None, "missing", True)
    with pytest.raises(RuntimeError, match="started HSM"):
        await core.Call(None, None, "missing")

    empty = core.Group("empty")
    assert empty.state() == []
    assert isinstance(empty.context(), core.Context)
    assert core.ID(empty) == "empty"
    assert core.QualifiedName(empty) == ""
    assert core.Name(empty) == ""
    assert await empty.dispatch(empty.context(), core.Event(name="noop")) is None
    await empty.stop(empty.context())
    await empty.restart(empty.context())
    with pytest.raises(TypeError, match="expected hsm.Instance"):
        core.Group(object())


@pytest.mark.asyncio
async def test_dispatch_queue_push_failure_dispatches_error_event():
    push_error = RuntimeError("push failed")
    pushed: list[str] = []
    seen_errors: list[BaseException] = []

    def capture_error(ctx: core.Context, inst: CoverageInstance, event: core.Event):
        seen_errors.append(event.data)

    class RejectingFifo:
        def push(self, event: core.Event) -> core.QueuePushResult:
            pushed.append(event.name)
            return (push_error,)

        def pop(self) -> core.QueuePopResult:
            return core.Event(), False, None

        def len(self) -> core.QueueLenResult:
            return 0, None

        def clear(self) -> None:
            pushed.clear()

    model = core.Define(
        "DispatchPushFailure",
        core.Initial(core.Target("idle")),
        core.State(
            "idle",
            core.Transition(
                core.On("hsm/error"),
                core.Target("../failed"),
                core.Effect(capture_error),
            ),
        ),
        core.State("failed"),
    )
    instance = await core.Started(
        core.context.new_context(),
        CoverageInstance(),
        model,
        core.Config(Queue=core.MultiQueue(RejectingFifo())),
    )

    completion = instance.dispatch(instance.context(), core.Event(name="go"))
    assert pushed == ["go"]
    await completion
    assert instance.state() == "/DispatchPushFailure/failed"
    assert seen_errors == [push_error]

    await core.Stop(instance)


@pytest.mark.asyncio
async def test_dispatch_all_dispatch_to_group_and_restart():
    model = core.Define(
        "GroupCoverage",
        core.Initial(core.Target("idle")),
        core.State("idle", core.Transition(core.On("go"), core.Target("../done"))),
        core.State("done"),
    )
    ctx = core.context.new_context().WithValue(core.Keys.Instances, {})
    first = await core.Started(ctx, CoverageInstance(), model, core.Config(ID="first"))
    second = await core.Started(ctx, CoverageInstance(), model, core.Config(ID="second"))

    await core.DispatchTo(ctx, core.Event(name="go"), "first")
    assert first.state() == "/GroupCoverage/done"
    assert second.state() == "/GroupCoverage/idle"

    await core.DispatchAll(ctx, core.Event(name="go"))
    assert [first.state(), second.state()] == [
        "/GroupCoverage/done",
        "/GroupCoverage/done",
    ]

    group = core.Group(first, second)
    snapshots = core.TakeSnapshot(ctx, group)
    assert [snapshot.ID for snapshot in snapshots] == ["first", "second"]
    assert group.state() == ["/GroupCoverage/done", "/GroupCoverage/done"]

    await core.Restart(group)
    assert group.state() == ["/GroupCoverage/idle", "/GroupCoverage/idle"]
    await core.Stop(group)


@pytest.mark.asyncio
async def test_config_clock_drives_time_event_activity():
    sleeps: list[tuple[timedelta, asyncio.Future[None]]] = []

    async def sleep(duration: timedelta) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        sleeps.append((duration, future))
        await future

    def delay(ctx: core.Context, inst: CoverageInstance, event: core.Event) -> timedelta:
        return timedelta(milliseconds=25)

    model = core.Define(
        "ClockCoverage",
        core.Initial(core.Target("waiting")),
        core.State(
            "waiting",
            core.Transition(core.After(delay), core.Target("../done")),
        ),
        core.State("done"),
    )

    instance = CoverageInstance()
    await core.Started(
        core.context.new_context(),
        instance,
        model,
        core.Config(Clock=core.Clock(sleep=sleep)),
    )
    for _ in range(100):
        if sleeps:
            break
        await asyncio.sleep(0)
    assert sleeps and sleeps[0][0] == timedelta(milliseconds=25)

    sleeps[0][1].set_result(None)
    for _ in range(100):
        if instance.state() == "/ClockCoverage/done":
            break
        await asyncio.sleep(0)
    assert instance.state() == "/ClockCoverage/done"

    await core.Stop(instance)
