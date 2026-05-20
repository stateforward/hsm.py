import asyncio

import pytest

import hsm


class ParityInstance(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.log: list[str] = []


@pytest.mark.asyncio
async def test_pascal_case_aliases_and_snapshot():
    instance = ParityInstance()

    model = hsm.Define(
        "AliasMachine",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("go"),
                hsm.Target("../done"),
            ),
        ),
        hsm.State("done"),
    )

    assert model.qualified_name == "/AliasMachine"
    assert hsm.define is hsm.Define
    assert hsm.state is hsm.State
    assert hsm.transition is hsm.Transition

    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    snapshot = hsm.TakeSnapshot(ctx, instance)
    assert snapshot.QualifiedName == "/AliasMachine"
    assert snapshot.State == "/AliasMachine/idle"
    assert snapshot.QueueLen == 0
    assert hsm.ID(instance) == snapshot.ID
    assert hsm.QualifiedName(instance) == "/AliasMachine"
    assert hsm.Name(instance) == "AliasMachine"
    assert any(event.Name == "go" and event.Target == "/AliasMachine/done" for event in snapshot.Events)

    await hsm.Stop(instance)


@pytest.mark.asyncio
async def test_attribute_onset_get_set_and_snapshot():
    instance = ParityInstance()

    model = hsm.Define(
        "AttributeMachine",
        hsm.Attribute("count", 1),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.OnSet("count"),
                hsm.Target("../changed"),
            ),
        ),
        hsm.State("changed"),
    )

    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    initial_value, ok = hsm.Get(ctx, instance, "count")
    assert ok is True
    assert initial_value == 1
    initial_snapshot = hsm.TakeSnapshot(ctx, instance)
    assert initial_snapshot.Attributes["/AttributeMachine/count"] == 1
    assert any(
        event.Name == "/AttributeMachine/count"
        and event.Kind == hsm.ChangeEventKind
        for event in initial_snapshot.Events
    )

    await hsm.Set(ctx, instance, "count", 2)

    updated_value, ok = hsm.Get(ctx, instance, "count")
    assert ok is True
    assert updated_value == 2
    assert instance.state() == "/AttributeMachine/changed"
    assert hsm.TakeSnapshot(ctx, instance).Attributes["/AttributeMachine/count"] == 2

    await hsm.Stop(instance)


@pytest.mark.asyncio
async def test_snapshot_identity_config_and_event_data_helpers():
    instance = ParityInstance()
    seen: list[object] = []

    async def idle_entry(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event) -> None:
        seen.append(event.Data)

    model = hsm.Define(
        "ConfiguredMachine",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Entry(idle_entry)),
    )

    ctx = hsm.Context()
    sm = await hsm.Started(
        ctx,
        instance,
        model,
        hsm.Config(ID="alpha", Name="/ConfiguredAlias", Data="boot"),
    )

    event = hsm.Event(name="go").WithDataAndID({"value": 1}, "event-1")

    assert hsm.ID(sm) == "alpha"
    assert hsm.ID(instance) == "alpha"
    assert hsm.QualifiedName(instance) == "/ConfiguredAlias"
    assert hsm.Name(instance) == "ConfiguredAlias"
    assert hsm.TakeSnapshot(ctx, instance).State == "/ConfiguredMachine/idle"
    assert seen == ["boot"]
    assert event.Data == {"value": 1}
    assert event.ID == "event-1"

    await hsm.Restart(instance, "again")
    assert seen == ["boot", "again"]

    await hsm.Stop(instance)


@pytest.mark.asyncio
async def test_operation_oncall_and_call():
    instance = ParityInstance()

    async def do(ctx: hsm.Context, inst: ParityInstance, value: int) -> str:
        inst.log.append(f"call:{value}")
        return f"ok:{value}"

    model = hsm.Define(
        "CallMachine",
        hsm.Operation("do", do),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.OnCall("do"),
                hsm.Target("../called"),
            ),
        ),
        hsm.State("called"),
    )

    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)

    result = await hsm.Call(ctx, instance, "do", 7)

    assert result == "ok:7"
    assert instance.log == ["call:7"]
    assert instance.state() == "/CallMachine/called"

    await hsm.Stop(instance)


@pytest.mark.asyncio
async def test_observers_restart_and_shallow_history():
    instance = ParityInstance()

    model = hsm.Define(
        "ObserverHistoryMachine",
        hsm.Initial(hsm.Target("parent")),
        hsm.State(
            "parent",
            hsm.ShallowHistory("memory", hsm.Transition(hsm.Target("b"))),
            hsm.Initial(hsm.Target("a")),
            hsm.State(
                "a",
                hsm.Transition(hsm.On("advance"), hsm.Target("../b")),
            ),
            hsm.State("b"),
            hsm.Transition(hsm.On("leave"), hsm.Target("../outside")),
        ),
        hsm.State(
            "outside",
            hsm.Transition(hsm.On("resume"), hsm.Target("../parent/memory")),
        ),
    )

    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)
    assert instance.state() == "/ObserverHistoryMachine/parent/a"

    await hsm.Dispatch(ctx, instance, hsm.Event(name="advance"))
    assert instance.state() == "/ObserverHistoryMachine/parent/b"

    entered_outside = hsm.AfterEntry(ctx, instance, "/ObserverHistoryMachine/outside")
    exited_parent_b = hsm.AfterExit(ctx, instance, "/ObserverHistoryMachine/parent/b")
    processed_leave = hsm.AfterProcess(ctx, instance, hsm.Event(name="leave"))
    dispatched_leave = hsm.AfterDispatch(ctx, instance, hsm.Event(name="leave"))

    await hsm.Dispatch(ctx, instance, hsm.Event(name="leave"))
    await asyncio.gather(entered_outside, exited_parent_b, processed_leave, dispatched_leave)

    assert instance.state() == "/ObserverHistoryMachine/outside"

    await hsm.Dispatch(ctx, instance, hsm.Event(name="resume"))
    assert instance.state() == "/ObserverHistoryMachine/parent/b"

    await hsm.Restart(instance)
    assert instance.state() == "/ObserverHistoryMachine/parent/a"

    await hsm.Stop(instance)


@pytest.mark.asyncio
async def test_group_dispatch_all_and_dispatch_to():
    first = ParityInstance()
    second = ParityInstance()

    model = hsm.Define(
        "GroupMachine",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )

    ctx = hsm.Context()
    await hsm.Start(ctx, first, model)
    await hsm.Start(ctx, second, model)

    first_id = hsm.TakeSnapshot(ctx, first).ID
    second_id = hsm.TakeSnapshot(ctx, second).ID
    assert first_id != second_id

    await hsm.DispatchTo(ctx, hsm.Event(name="go"), first_id)
    assert first.state() == "/GroupMachine/done"
    assert second.state() == "/GroupMachine/idle"

    await hsm.DispatchAll(ctx, hsm.Event(name="go"))
    assert first.state() == "/GroupMachine/done"
    assert second.state() == "/GroupMachine/done"

    group = hsm.NewGroup(first, second)
    group_snapshot = hsm.TakeSnapshot(ctx, group)
    assert group_snapshot.ID != ""
    assert group_snapshot.QualifiedName == ""

    await hsm.Stop(group)
