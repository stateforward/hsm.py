import asyncio
import dataclasses
from datetime import timedelta

import pytest

import hsm.hsm as core


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
    parent = core.Context().WithValue("request", "root")
    child, cancel = parent.WithCancel()
    assert child.Value("request") == "root"
    cancel()
    assert child.Done().done()

    queue = core.MultiQueue()
    assert queue.push(core.Event(name="regular")) == (None,)
    assert queue.push(core.Event(name="complete", kind=core.CompletionEventKind)) == (None,)
    assert queue.len() == (2, None)
    event, ok, error = queue.pop()
    assert ok and error is None and event.name == "complete"
    event, ok, error = queue.pop()
    assert ok and error is None and event.name == "regular"

    clock = core.Clock()
    config = core.Config(ID="machine-1", Name="/Alias", Data={"boot": True}, Clock=clock, Queue=queue)
    assert config.id == "machine-1"
    assert config.name == "/Alias"
    assert config.data == {"boot": True}
    assert config.clock is clock
    assert config.queue is queue
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.id = "machine-2"


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

    ctx = core.Context().WithValue(core.Keys.Instances, {})
    await core.Started(ctx, instance, model, core.Config(ID="runtime", Data="boot"))
    assert instance.state() == "/RuntimeCoverage/idle"
    assert instance.log == ["entry:boot"]
    assert core.Get(ctx, instance, "flag") == (False, True)

    assert await core.Set(ctx, instance, "flag", True) is None
    assert instance.state() == "/RuntimeCoverage/changed"
    assert core.Get(ctx, instance, "flag") == (True, True)

    result = await core.Call(ctx, instance, "double", 7)
    for _ in range(100):
        if instance.state() == "/RuntimeCoverage/called":
            break
        await asyncio.sleep(0)
    assert result == 14
    assert instance.log == ["entry:boot", "set:True", "double:7", "/RuntimeCoverage/double"]
    assert instance.state() == "/RuntimeCoverage/called"

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
async def test_dispatch_all_dispatch_to_group_and_restart():
    model = core.Define(
        "GroupCoverage",
        core.Initial(core.Target("idle")),
        core.State("idle", core.Transition(core.On("go"), core.Target("../done"))),
        core.State("done"),
    )
    ctx = core.Context().WithValue(core.Keys.Instances, {})
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
        core.Context(),
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
