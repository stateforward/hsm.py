import asyncio
from datetime import datetime, timedelta

import pytest

import hsm


class ParityInstance(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.log: list[str] = []


def test_snake_case_dsl_aliases_are_available():
    aliases = {
        "activity": hsm.Activity,
        "after": hsm.After,
        "after_dispatch": hsm.AfterDispatch,
        "after_entry": hsm.AfterEntry,
        "after_executed": hsm.AfterExecuted,
        "after_exit": hsm.AfterExit,
        "after_process": hsm.AfterProcess,
        "at": hsm.At,
        "attribute": hsm.Attribute,
        "clock": hsm.Clock,
        "element": hsm.Element,
        "validation_error": hsm.ValidationError,
        "behavior": hsm.Behavior,
        "model": hsm.Model,
        "instance": hsm.Instance,
        "call": hsm.Call,
        "call_data": hsm.CallData,
        "choice": hsm.Choice,
        "config": hsm.Config,
        "context": hsm.Context,
        "deep_history": hsm.DeepHistory,
        "default_clock": hsm.DefaultClock,
        "define": hsm.Define,
        "defer": hsm.Defer,
        "dispatch": hsm.Dispatch,
        "dispatch_all": hsm.DispatchAll,
        "dispatch_to": hsm.DispatchTo,
        "effect": hsm.Effect,
        "entry": hsm.Entry,
        "event": hsm.Event,
        "completion_event": hsm.CompletionEvent,
        "event_snapshot": hsm.EventSnapshot,
        "every": hsm.Every,
        "exit": hsm.Exit,
        "final": hsm.Final,
        "final_state": hsm.FinalState,
        "get": hsm.Get,
        "guard": hsm.Guard,
        "id": hsm.ID,
        "initial": hsm.Initial,
        "is_ancestor": hsm.IsAncestor,
        "lca": hsm.LCA,
        "make_group": hsm.MakeGroup,
        "name": hsm.Name,
        "new": hsm.New,
        "new_group": hsm.NewGroup,
        "on": hsm.On,
        "on_call": hsm.OnCall,
        "on_set": hsm.OnSet,
        "operation": hsm.Operation,
        "qualified_name": hsm.QualifiedName,
        "restart": hsm.Restart,
        "set": hsm.Set,
        "shallow_history": hsm.ShallowHistory,
        "source": hsm.Source,
        "start": hsm.Start,
        "started": hsm.Started,
        "state": hsm.State,
        "stop": hsm.Stop,
        "snapshot": hsm.Snapshot,
        "take_snapshot": hsm.TakeSnapshot,
        "target": hsm.Target,
        "transition": hsm.Transition,
        "when": hsm.When,
    }

    for alias_name, canonical in aliases.items():
        assert getattr(hsm, alias_name) is canonical

    assert "started" in hsm.__all__
    assert hsm.make_kind is hsm.MakeKind
    assert hsm.is_kind is hsm.IsKind
    custom = hsm.make_kind(hsm.Kinds.Element)
    assert hsm.is_kind(custom, hsm.Kinds.Element)
    assert hsm.match("machine-1", "machine-*")
    assert hsm.match("machine-1", "other-*", "machine-*")
    assert hsm.onset is hsm.OnSet
    assert hsm.MakeGroup is hsm.NewGroup


def test_snake_case_dsl_values_are_available():
    aliases = {
        "any_event": hsm.AnyEvent,
        "attribute_change": hsm.AttributeChange,
        "initial_event": hsm.InitialEvent,
        "error_event": hsm.ErrorEvent,
        "final_event": hsm.FinalEvent,
        "infinite_duration": hsm.InfiniteDuration,
        "kinds": hsm.Kinds,
        "null_kind": hsm.NullKind,
        "element_kind": hsm.ElementKind,
        "partial_kind": hsm.PartialKind,
        "namespace_kind": hsm.NamespaceKind,
        "named_element_kind": hsm.NamedElementKind,
        "vertex_kind": hsm.VertexKind,
        "constraint_kind": hsm.ConstraintKind,
        "behavior_kind": hsm.BehaviorKind,
        "concurrent_kind": hsm.ConcurrentKind,
        "sequential_kind": hsm.SequentialKind,
        "state_machine_kind": hsm.StateMachineKind,
        "state_kind": hsm.StateKind,
        "transition_kind": hsm.TransitionKind,
        "internal_kind": hsm.InternalKind,
        "external_kind": hsm.ExternalKind,
        "local_kind": hsm.LocalKind,
        "self_kind": hsm.SelfKind,
        "event_kind": hsm.EventKind,
        "time_event_kind": hsm.TimeEventKind,
        "completion_event_kind": hsm.CompletionEventKind,
        "change_event_kind": hsm.ChangeEventKind,
        "call_event_kind": hsm.CallEventKind,
        "error_event_kind": hsm.ErrorEventKind,
        "pseudostate_kind": hsm.PseudostateKind,
        "initial_kind": hsm.InitialKind,
        "final_state_kind": hsm.FinalStateKind,
        "choice_kind": hsm.ChoiceKind,
        "shallow_history_kind": hsm.ShallowHistoryKind,
        "deep_history_kind": hsm.DeepHistoryKind,
        "attribute_kind": hsm.AttributeKind,
        "operation_kind": hsm.OperationKind,
    }

    for alias_name, canonical in aliases.items():
        assert getattr(hsm, alias_name) is canonical
        assert alias_name in hsm.__all__


@pytest.mark.asyncio
async def test_snake_case_dsl_aliases_build_and_run_model():
    instance = ParityInstance()

    async def started(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event) -> None:
        inst.log.append("started")

    model = hsm.define(
        "SnakeDslMachine",
        hsm.attribute("count", 0),
        hsm.operation("record"),
        hsm.initial(hsm.target("idle")),
        hsm.state(
            "idle",
            hsm.entry(started),
            hsm.transition(
                hsm.on("go"),
                hsm.guard(lambda ctx, inst, event: True),
                hsm.effect(lambda ctx, inst, event: inst.log.append("go")),
                hsm.target("../done"),
            ),
            hsm.transition(
                hsm.on_set("count"),
                hsm.target("../done"),
            ),
        ),
        hsm.final("done"),
    )

    ctx = hsm.context()
    await hsm.start(ctx, instance, model)

    assert instance.state() == "/SnakeDslMachine/idle"
    assert hsm.get(ctx, instance, "count") == (0, True)

    await hsm.dispatch(ctx, instance, hsm.event("go"))

    snapshot = hsm.take_snapshot(ctx, instance)
    assert snapshot.state == "/SnakeDslMachine/done"
    assert instance.log == ["started", "go"]

    await hsm.stop(instance)


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
    assert snapshot.qualified_name == "/AliasMachine"
    assert snapshot.State == "/AliasMachine/idle"
    assert snapshot.state == "/AliasMachine/idle"
    assert snapshot.QueueLen == 0
    assert snapshot.queue_len == 0
    assert hsm.ID(instance) == snapshot.ID
    assert hsm.ID(instance) == snapshot.id
    assert hsm.QualifiedName(instance) == "/AliasMachine"
    assert hsm.Name(instance) == "AliasMachine"
    assert any(event.Name == "go" and event.Target == "/AliasMachine/done" for event in snapshot.Events)
    assert any(event.name == "go" and event.target == "/AliasMachine/done" for event in snapshot.events)

    await hsm.Stop(instance)


@pytest.mark.asyncio
async def test_model_paths_use_posix_semantics_for_absolute_targets():
    instance = ParityInstance()
    model = hsm.Define(
        "PosixPathMachine",
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Transition(hsm.On("stop"), hsm.Target("/inactive"))),
        hsm.State("inactive"),
    )

    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model)
    await hsm.Dispatch(ctx, instance, hsm.Event("stop"))

    snapshot = hsm.TakeSnapshot(ctx, instance)
    assert snapshot.State == "/PosixPathMachine/inactive"
    assert "\\" not in snapshot.State

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
    snake_event = hsm.Event(name="go").with_data_and_id({"value": 1}, "event-1")
    snake_data_event = hsm.Event(name="go").with_data({"value": 2})

    assert hsm.ID(sm) == "alpha"
    assert hsm.ID(instance) == "alpha"
    assert hsm.QualifiedName(instance) == "/ConfiguredAlias"
    assert hsm.Name(instance) == "ConfiguredAlias"
    assert hsm.TakeSnapshot(ctx, instance).State == "/ConfiguredMachine/idle"
    assert seen == ["boot"]
    assert event.Data == {"value": 1}
    assert event.ID == "event-1"
    assert snake_event.Name == event.Name
    assert snake_event.Data == event.Data
    assert snake_event.ID == event.ID
    assert snake_data_event.Data == {"value": 2}

    await hsm.Restart(instance, "again")
    assert seen == ["boot", "again"]

    await hsm.Stop(instance)


@pytest.mark.asyncio
async def test_config_clock_drives_after_transition():
    instance = ParityInstance()
    sleeps: list[tuple[object, asyncio.Future[None]]] = []

    async def manual_sleep(duration):
        future = asyncio.get_running_loop().create_future()
        sleeps.append((duration, future))
        await future

    async def delay(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event):
        return timedelta(seconds=5)

    model = hsm.Define(
        "ClockMachine",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(
                hsm.After(delay),
                hsm.Target("../done"),
            ),
        ),
        hsm.State("done"),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model, hsm.Config(Clock=hsm.Clock(sleep=manual_sleep)))

    for _ in range(10):
        if sleeps:
            break
        await asyncio.sleep(0)

    assert len(sleeps) == 1
    assert sleeps[0][0].total_seconds() == 5
    assert instance.state() == "/ClockMachine/waiting"

    entered_done = hsm.AfterEntry(ctx, instance, "/ClockMachine/done")
    sleeps[0][1].set_result(None)
    await entered_done
    assert instance.state() == "/ClockMachine/done"

    await hsm.Stop(instance)


@pytest.mark.asyncio
async def test_config_clock_drives_at_transition():
    instance = ParityInstance()
    sleeps: list[tuple[object, asyncio.Future[None]]] = []

    async def manual_sleep(duration):
        future = asyncio.get_running_loop().create_future()
        sleeps.append((duration, future))
        await future

    target_time = datetime.now() + timedelta(hours=2)

    async def deadline(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event):
        return target_time

    model = hsm.Define(
        "AtClockMachine",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(
                hsm.At(deadline),
                hsm.Target("../done"),
            ),
        ),
        hsm.State("done"),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model, hsm.Config(Clock=hsm.Clock(sleep=manual_sleep)))

    for _ in range(10):
        if sleeps:
            break
        await asyncio.sleep(0)

    assert len(sleeps) == 1
    assert sleeps[0][0] > timedelta(hours=1, minutes=59)
    assert instance.state() == "/AtClockMachine/waiting"

    entered_done = hsm.AfterEntry(ctx, instance, "/AtClockMachine/done")
    sleeps[0][1].set_result(None)
    await entered_done
    assert instance.state() == "/AtClockMachine/done"

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
