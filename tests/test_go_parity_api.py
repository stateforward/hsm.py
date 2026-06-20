import asyncio
import dataclasses
import re
from datetime import datetime, timedelta

import pytest

import hsm


DSL_HSM_APIS = {
    "After",
    "At",
    "Attribute",
    "Activity",
    "Choice",
    "DeepHistory",
    "Defer",
    "Define",
    "Dispatch",
    "DispatchAll",
    "DispatchTo",
    "Effect",
    "Entry",
    "EntryPoint",
    "Every",
    "Exit",
    "ExitPoint",
    "Final",
    "Guard",
    "ID",
    "Initial",
    "MakeGroup",
    "Name",
    "New",
    "NewGroup",
    "On",
    "OnCall",
    "OnSet",
    "Operation",
    "QualifiedName",
    "Restart",
    "ShallowHistory",
    "Source",
    "Start",
    "Started",
    "State",
    "Stop",
    "SubmachineState",
    "TakeSnapshot",
    "Target",
    "Transition",
    "When",
}


def _snake_case(name: str) -> str:
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return value.lower()


class ParityInstance(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.log: list[str] = []


def test_snake_case_dsl_aliases_are_available():
    aliases = {
        "activity": hsm.Activity,
        "after": hsm.After,
        "at": hsm.At,
        "attribute": hsm.Attribute,
        "choice": hsm.Choice,
        "deep_history": hsm.DeepHistory,
        "define": hsm.Define,
        "defer": hsm.Defer,
        "dispatch": hsm.Dispatch,
        "dispatch_all": hsm.DispatchAll,
        "dispatch_to": hsm.DispatchTo,
        "effect": hsm.Effect,
        "entry": hsm.Entry,
        "entry_point": hsm.EntryPoint,
        "every": hsm.Every,
        "exit": hsm.Exit,
        "exit_point": hsm.ExitPoint,
        "final": hsm.Final,
        "guard": hsm.Guard,
        "id": hsm.ID,
        "initial": hsm.Initial,
        "make_group": hsm.MakeGroup,
        "name": hsm.Name,
        "new": hsm.New,
        "new_group": hsm.NewGroup,
        "on": hsm.On,
        "on_call": hsm.OnCall,
        "on_set": hsm.OnSet,
        "operation": hsm.Operation,
        "qualified_name": hsm.QualifiedName,
        "redefine": hsm.Redefine,
        "restart": hsm.Restart,
        "shallow_history": hsm.ShallowHistory,
        "source": hsm.Source,
        "start": hsm.Start,
        "started": hsm.Started,
        "state": hsm.State,
        "stop": hsm.Stop,
        "submachine_state": hsm.SubmachineState,
        "take_snapshot": hsm.TakeSnapshot,
        "target": hsm.Target,
        "transition": hsm.Transition,
        "when": hsm.When,
    }

    for alias_name, canonical in aliases.items():
        assert getattr(hsm, alias_name) is canonical

    assert "started" in hsm.__all__
    assert hsm.MakeGroup is hsm.NewGroup
    assert hsm.group is hsm.Group
    assert "Group" in hsm.__all__
    assert "group" in hsm.__all__


def test_public_pascal_case_exports_have_snake_case_aliases():
    missing = []
    for name in sorted(DSL_HSM_APIS):
        alias = _snake_case(name)
        if not hasattr(hsm, alias):
            missing.append((name, alias))
            continue
        assert getattr(hsm, alias) is getattr(hsm, name)
        assert alias in hsm.__all__

    assert missing == []


def test_dsl_documented_hsm_apis_have_snake_case_aliases():
    missing = []
    for name in sorted(DSL_HSM_APIS):
        if not hasattr(hsm, name):
            missing.append((name, "<canonical>"))
            continue
        alias = _snake_case(name)
        if not hasattr(hsm, alias):
            missing.append((name, alias))
            continue
        assert getattr(hsm, alias) is getattr(hsm, name)
        assert alias in hsm.__all__

    assert missing == []


def test_dsl_values_are_exported():
    values = {
        "AnyEvent": hsm.AnyEvent,
        "AttributeChange": hsm.AttributeChange,
        "InitialEvent": hsm.InitialEvent,
        "ErrorEvent": hsm.ErrorEvent,
        "FinalEvent": hsm.FinalEvent,
        "InfiniteDuration": hsm.InfiniteDuration,
        "NullKind": hsm.NullKind,
        "ElementKind": hsm.ElementKind,
        "NamespaceKind": hsm.NamespaceKind,
        "VertexKind": hsm.VertexKind,
        "ConstraintKind": hsm.ConstraintKind,
        "BehaviorKind": hsm.BehaviorKind,
        "ConcurrentKind": hsm.ConcurrentKind,
        "SequentialKind": hsm.SequentialKind,
        "StateMachineKind": hsm.StateMachineKind,
        "StateKind": hsm.StateKind,
        "TransitionKind": hsm.TransitionKind,
        "InternalKind": hsm.InternalKind,
        "ExternalKind": hsm.ExternalKind,
        "LocalKind": hsm.LocalKind,
        "SelfKind": hsm.SelfKind,
        "EventKind": hsm.EventKind,
        "TimeEventKind": hsm.TimeEventKind,
        "CompletionEventKind": hsm.CompletionEventKind,
        "CallEventKind": hsm.CallEventKind,
        "ErrorEventKind": hsm.ErrorEventKind,
        "PseudostateKind": hsm.PseudostateKind,
        "InitialKind": hsm.InitialKind,
        "FinalStateKind": hsm.FinalStateKind,
        "ChoiceKind": hsm.ChoiceKind,
        "ShallowHistoryKind": hsm.ShallowHistoryKind,
        "DeepHistoryKind": hsm.DeepHistoryKind,
        "AttributeKind": hsm.AttributeKind,
        "OperationKind": hsm.OperationKind,
    }

    for name, canonical in values.items():
        assert getattr(hsm, name) is canonical
        assert name in hsm.__all__


def test_config_supports_snake_case_fields():
    clock = hsm.Clock()
    config = hsm.Config(
        id="machine-1",
        name="/Alias",
        data={"boot": True},
        clock=clock,
    )

    assert config.ID == "machine-1"
    assert config.Name == "/Alias"
    assert config.Data == {"boot": True}
    assert config.Clock is clock
    assert config.id == "machine-1"
    assert config.name == "/Alias"
    assert config.data == {"boot": True}
    assert config.clock is clock

    with pytest.raises(dataclasses.FrozenInstanceError):
        config.id = "machine-2"


@pytest.mark.asyncio
async def test_snake_case_dsl_aliases_build_and_run_model():
    instance = ParityInstance()

    def started(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event) -> None:
        inst.log.append("started")

    def allow(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event) -> bool:
        return True

    def record_go(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event) -> None:
        inst.log.append("go")

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
                hsm.guard(allow),
                hsm.effect(record_go),
                hsm.target("../done"),
            ),
            hsm.transition(
                hsm.on_set("count"),
                hsm.target("../done"),
            ),
        ),
        hsm.final("done"),
    )

    ctx = hsm.Context()
    await hsm.started(ctx, instance, model)

    assert instance.state() == "/SnakeDslMachine/idle"
    assert hsm.Get(ctx, instance, "count") == (0, True)

    await hsm.dispatch(ctx, instance, hsm.Event(name="go"))

    snapshot = hsm.TakeSnapshot(ctx, instance)
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

    ctx = hsm.Context().WithValue(hsm.Keys.Instances, {})
    await hsm.Started(ctx, instance, model)

    snapshot = instance.take_snapshot()
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
    assert any(
        "go" in transition.events and transition.target == "/AliasMachine/done"
        for transition in snapshot.Transitions
    )
    assert any(
        "go" in transition.events and transition.target == "/AliasMachine/done"
        for transition in snapshot.transitions
    )

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_model_paths_use_posix_semantics_for_absolute_targets():
    instance = ParityInstance()
    model = hsm.Define(
        "PosixPathMachine",
        hsm.Initial(hsm.Target("active")),
        hsm.State("active", hsm.Transition(hsm.On("stop"), hsm.Target("/inactive"))),
        hsm.State("inactive"),
    )

    ctx = hsm.Context().WithValue(hsm.Keys.Instances, {})
    await hsm.Started(ctx, instance, model)
    await hsm.Dispatch(ctx, instance, hsm.Event(name="stop"))

    snapshot = instance.take_snapshot()
    assert snapshot.State == "/PosixPathMachine/inactive"
    assert "\\" not in snapshot.State

    await instance.stop(instance.context())


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

    ctx = hsm.Context().WithValue(hsm.Keys.Instances, {})
    await hsm.Started(ctx, instance, model)

    initial_value, ok = hsm.Get(ctx, instance, "count")
    assert ok is True
    assert initial_value == 1
    initial_snapshot = instance.take_snapshot()
    assert initial_snapshot.Attributes["/AttributeMachine/count"] == 1
    assert any(
        transition.events == ["/AttributeMachine/count"]
        and transition.target == "/AttributeMachine/changed"
        for transition in initial_snapshot.Transitions
    )

    await hsm.Set(ctx, instance, "count", 2)

    updated_value, ok = hsm.Get(ctx, instance, "count")
    assert ok is True
    assert updated_value == 2
    assert instance.state() == "/AttributeMachine/changed"
    assert instance.take_snapshot().Attributes["/AttributeMachine/count"] == 2

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_snapshot_mapping_is_read_only_and_values_remain_user_mutable():
    instance = ParityInstance()
    go = hsm.Event(name="go", schema={"fields": ["payload"]})

    model = hsm.Define(
        "SnapshotReadOnly",
        hsm.Attribute("payload", {"items": []}),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On(go), hsm.Target("../done"))),
        hsm.State("done"),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)

    runtime_payload = {"items": [{"nested": ["initial"]}]}
    await hsm.Set(ctx, instance, "payload", runtime_payload)
    stored_payload, ok = hsm.Get(ctx, instance, "payload")
    assert ok is True
    assert stored_payload is runtime_payload
    snapshot = instance.take_snapshot()

    runtime_payload["items"][0]["nested"].append("mutated-after-snapshot")

    snapshot_payload = snapshot.Attributes["/SnapshotReadOnly/payload"]
    assert snapshot_payload is runtime_payload
    assert snapshot_payload["items"][0]["nested"] == [
        "initial",
        "mutated-after-snapshot",
    ]
    assert isinstance(snapshot.Transitions, tuple)

    with pytest.raises(TypeError):
        snapshot.Attributes["/SnapshotReadOnly/payload"] = {"items": []}
    snapshot_payload["items"][0]["nested"].append("snapshot-mutated")
    assert runtime_payload["items"][0]["nested"][-1] == "snapshot-mutated"
    with pytest.raises(AttributeError):
        snapshot.Transitions.append("extra")

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_when_string_is_onset_attribute_trigger():
    instance = ParityInstance()

    model = hsm.Define(
        "WhenAttributeMachine",
        hsm.Attribute("flag", False),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.When("flag"),
                hsm.Target("../changed"),
            ),
        ),
        hsm.State("changed"),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)

    await hsm.Set(ctx, instance, "flag", True)

    assert instance.state() == "/WhenAttributeMachine/changed"
    assert hsm.Get(ctx, instance, "flag") == (True, True)
    assert instance.take_snapshot().QueueLen == 0

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_snapshot_identity_config_and_event_data_helpers():
    instance = ParityInstance()
    seen: list[object] = []

    def idle_entry(
        ctx: hsm.Context, inst: ParityInstance, event: hsm.Event
    ) -> None:
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
        hsm.Config(id="alpha", name="/ConfiguredAlias", data="boot"),
    )

    event = hsm.Event(name="go").WithDataAndID({"value": 1}, "event-1")
    snake_event = hsm.Event(name="go").with_data_and_id({"value": 1}, "event-1")
    snake_data_event = hsm.Event(name="go").with_data({"value": 2})

    assert hsm.ID(sm) == "alpha"
    assert hsm.ID(instance) == "alpha"
    assert hsm.QualifiedName(instance) == "/ConfiguredAlias"
    assert hsm.Name(instance) == "ConfiguredAlias"
    assert instance.take_snapshot().State == "/ConfiguredMachine/idle"
    assert seen == ["boot"]
    assert event.Data == {"value": 1}
    assert event.ID == "event-1"
    assert snake_event.Name == event.Name
    assert snake_event.Data == event.Data
    assert snake_event.ID == event.ID
    assert snake_data_event.Data == {"value": 2}

    await instance.restart(instance.context(), "again")
    assert seen == ["boot", "again"]

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_context_carries_current_machine_and_all_started_machines():
    model = hsm.Define(
        "ContextMachineRegistry",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )

    ctx = hsm.Context().WithValue(hsm.Keys.Instances, {})
    alpha = await hsm.Started(ctx, ParityInstance(), model, hsm.Config(id="alpha"))
    bravo = await hsm.Started(
        alpha.context(), ParityInstance(), model, hsm.Config(id="bravo")
    )
    tagged = ctx.WithValue("request-id", "req-7")

    assert ctx.Value(hsm.Keys.HSM) is None
    assert alpha.context().Value(hsm.Keys.HSM) is not None
    assert bravo.context().Value(hsm.Keys.HSM) is not None
    assert ctx.Value(hsm.Keys.Owner) is None
    assert ctx.Value("request-id") is None
    assert tagged.Value("request-id") == "req-7"
    instances = ctx.Value(hsm.Keys.Instances)
    assert isinstance(instances, dict)
    assert instances == {"alpha": alpha, "bravo": bravo}
    assert alpha.context().Value(hsm.Keys.Instances) is instances


@pytest.mark.asyncio
async def test_cross_machine_dispatch_stamps_source_and_target_from_context():
    def send_to_bravo(
        ctx: hsm.Context, inst: ParityInstance, event: hsm.Event
    ) -> None:
        _ = hsm.DispatchTo(ctx, hsm.Event(name="relay"), "bravo")

    def record_delivery(
        ctx: hsm.Context, inst: ParityInstance, event: hsm.Event
    ) -> None:
        inst.log.append(f"{event.Source}->{event.Target}")

    model = hsm.Define(
        "ContextDeliveryEnvelope",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Effect(send_to_bravo)),
            hsm.Transition(hsm.On("relay"), hsm.Effect(record_delivery)),
        ),
    )

    ctx = hsm.Context()
    alpha_instance = ParityInstance()
    bravo_instance = ParityInstance()
    alpha = await hsm.Started(ctx, alpha_instance, model, hsm.Config(id="alpha"))
    await hsm.Started(alpha.context(), bravo_instance, model, hsm.Config(id="bravo"))

    await hsm.Dispatch(alpha.context(), alpha, hsm.Event(name="go"))
    for _ in range(100):
        if bravo_instance.log:
            break
        await asyncio.sleep(0)

    assert alpha_instance.log == []
    assert bravo_instance.log == ["->bravo"]


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
    await hsm.Started(
        ctx, instance, model, hsm.Config(Clock=hsm.Clock(sleep=manual_sleep))
    )

    for _ in range(10):
        if sleeps:
            break
        await asyncio.sleep(0)

    assert len(sleeps) == 1
    assert sleeps[0][0].total_seconds() == 5
    assert instance.state() == "/ClockMachine/waiting"

    sleeps[0][1].set_result(None)
    for _ in range(100):
        if instance.state() == "/ClockMachine/done":
            break
        await asyncio.sleep(0)
    assert instance.state() == "/ClockMachine/done"

    await instance.stop(instance.context())


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
    await hsm.Started(
        ctx, instance, model, hsm.Config(Clock=hsm.Clock(sleep=manual_sleep))
    )

    for _ in range(10):
        if sleeps:
            break
        await asyncio.sleep(0)

    assert len(sleeps) == 1
    assert sleeps[0][0] > timedelta(hours=1, minutes=59)
    assert instance.state() == "/AtClockMachine/waiting"

    sleeps[0][1].set_result(None)
    for _ in range(100):
        if instance.state() == "/AtClockMachine/done":
            break
        await asyncio.sleep(0)
    assert instance.state() == "/AtClockMachine/done"

    await instance.stop(instance.context())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("timer_factory", "attribute_value"),
    [
        (hsm.After, timedelta(seconds=3)),
        (hsm.Every, timedelta(seconds=4)),
    ],
)
async def test_attribute_duration_drives_after_and_every_transitions(
    timer_factory, attribute_value
):
    instance = ParityInstance()
    sleeps: list[tuple[object, asyncio.Future[None]]] = []

    async def manual_sleep(duration):
        future = asyncio.get_running_loop().create_future()
        sleeps.append((duration, future))
        await future

    def delay(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event):
        value, ok = hsm.Get(ctx, inst, "delay")
        assert ok is True
        return value

    model = hsm.Define(
        "AttributeDurationMachine",
        hsm.Attribute("delay", attribute_value),
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(
                timer_factory(delay),
                hsm.Target("../done"),
            ),
        ),
        hsm.State("done"),
    )

    ctx = hsm.Context()
    await hsm.Started(
        ctx, instance, model, hsm.Config(Clock=hsm.Clock(sleep=manual_sleep))
    )

    for _ in range(10):
        if sleeps:
            break
        await asyncio.sleep(0)

    assert len(sleeps) == 1
    assert sleeps[0][0] == attribute_value
    assert instance.state() == "/AttributeDurationMachine/waiting"

    sleeps[0][1].set_result(None)
    for _ in range(100):
        if instance.state() == "/AttributeDurationMachine/done":
            break
        await asyncio.sleep(0)
    assert instance.state() == "/AttributeDurationMachine/done"

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_attribute_timepoint_drives_at_transition():
    instance = ParityInstance()
    sleeps: list[tuple[object, asyncio.Future[None]]] = []

    async def manual_sleep(duration):
        future = asyncio.get_running_loop().create_future()
        sleeps.append((duration, future))
        await future

    deadline = datetime.now() + timedelta(hours=1)

    def deadline_source(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event):
        value, ok = hsm.Get(ctx, inst, "deadline")
        assert ok is True
        return value

    model = hsm.Define(
        "AttributeTimepointMachine",
        hsm.Attribute("deadline", deadline),
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(
                hsm.At(deadline_source),
                hsm.Target("../done"),
            ),
        ),
        hsm.State("done"),
    )

    ctx = hsm.Context()
    await hsm.Started(
        ctx, instance, model, hsm.Config(Clock=hsm.Clock(sleep=manual_sleep))
    )

    for _ in range(10):
        if sleeps:
            break
        await asyncio.sleep(0)

    assert len(sleeps) == 1
    assert timedelta(minutes=59) < sleeps[0][0] <= timedelta(hours=1)
    assert instance.state() == "/AttributeTimepointMachine/waiting"

    sleeps[0][1].set_result(None)
    for _ in range(100):
        if instance.state() == "/AttributeTimepointMachine/done":
            break
        await asyncio.sleep(0)
    assert instance.state() == "/AttributeTimepointMachine/done"

    await instance.stop(instance.context())


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
    await hsm.Started(ctx, instance, model)

    result = await hsm.Call(ctx, instance, "do", 7)

    assert result == "ok:7"
    assert instance.log == ["call:7"]
    assert instance.state() == "/CallMachine/called"

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_snake_case_named_operation_dsl_builds_and_runs_model():
    class NamedOperationInstance(ParityInstance):
        def enter_idle(self, event: hsm.Event) -> None:
            self.log.append(f"enter_idle:{event.Name}")

    instance = NamedOperationInstance()

    def allow(
        ctx: hsm.Context, inst: NamedOperationInstance, event: hsm.Event
    ) -> bool:
        assert isinstance(ctx, hsm.Context)
        assert inst is instance
        inst.log.append(f"guard:{event.Name}")
        return True

    def leave_idle(
        ctx: hsm.Context, inst: NamedOperationInstance, event: hsm.Event
    ) -> None:
        inst.log.append(f"exit:{event.Name}")

    def effect(
        ctx: hsm.Context, inst: NamedOperationInstance, event: hsm.Event
    ) -> None:
        inst.log.append(f"effect:{event.Name}")

    def enter_done(
        ctx: hsm.Context, inst: NamedOperationInstance, event: hsm.Event
    ) -> None:
        inst.log.append(f"enter_done:{event.Name}")

    model = hsm.define(
        "NamedOperationDslMachine",
        hsm.operation("enter_idle"),
        hsm.operation("allow", allow),
        hsm.operation("leave_idle", leave_idle),
        hsm.operation("effect", effect),
        hsm.operation("enter_done", enter_done),
        hsm.initial(hsm.target("idle")),
        hsm.state(
            "idle",
            hsm.entry("enter_idle"),
            hsm.exit("leave_idle"),
            hsm.transition(
                hsm.on("go"),
                hsm.guard("allow"),
                hsm.effect("effect"),
                hsm.target("../done"),
            ),
        ),
        hsm.state("done", hsm.entry("enter_done")),
    )

    ctx = hsm.Context()
    await hsm.started(ctx, instance, model)
    await hsm.dispatch(ctx, instance, hsm.Event(name="go"))

    assert instance.state() == "/NamedOperationDslMachine/done"
    assert instance.log == [
        "enter_idle:hsm/initial",
        "guard:go",
        "exit:go",
        "effect:go",
        "enter_done:go",
    ]

    await hsm.stop(instance)


def test_named_operation_dsl_rejects_missing_operation_references():
    with pytest.raises(hsm.ValidationError, match='missing operation "missing"'):
        hsm.Define(
            "MissingNamedOperationMachine",
            hsm.Initial(hsm.Target("idle")),
            hsm.State("idle", hsm.Entry("missing")),
        )

    with pytest.raises(hsm.ValidationError, match='missing operation "missing"'):
        hsm.Define(
            "MissingNamedGuardMachine",
            hsm.Initial(hsm.Target("idle")),
            hsm.State(
                "idle",
                hsm.Transition(
                    hsm.On("go"),
                    hsm.Guard("missing"),
                    hsm.Target("../done"),
                ),
            ),
            hsm.State("done"),
        )

    with pytest.raises(
        hsm.ValidationError, match='operation name "bad/name" cannot contain "/"'
    ):
        def work(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event) -> None:
            del ctx, inst, event

        hsm.Define(
            "BadNamedOperationReferenceMachine",
            hsm.Operation("work", work),
            hsm.Initial(hsm.Target("idle")),
            hsm.State("idle", hsm.Entry("bad/name")),
        )

    with pytest.raises(
        hsm.ValidationError, match='operation name "bad/name" cannot contain "/"'
    ):
        def allow(ctx: hsm.Context, inst: ParityInstance, event: hsm.Event) -> bool:
            del ctx, inst, event
            return True

        hsm.Define(
            "BadNamedGuardReferenceMachine",
            hsm.Operation("allow", allow),
            hsm.Initial(hsm.Target("idle")),
            hsm.State(
                "idle",
                hsm.Transition(
                    hsm.On("go"),
                    hsm.Guard("bad/name"),
                    hsm.Target("../done"),
                ),
            ),
            hsm.State("done"),
        )


def test_named_operation_dsl_rejects_async_sequential_operation_references():
    async def async_operation(
        ctx: hsm.Context, inst: ParityInstance, event: hsm.Event
    ) -> None:
        del ctx, inst, event

    with pytest.raises(
        hsm.ValidationError, match="entry must be a synchronous function"
    ):
        hsm.Define(
            "AsyncNamedEntryOperationMachine",
            hsm.Operation("enter", async_operation),
            hsm.Initial(hsm.Target("idle")),
            hsm.State("idle", hsm.Entry("enter")),
        )

    with pytest.raises(
        hsm.ValidationError, match="guard must be a synchronous function"
    ):
        hsm.Define(
            "AsyncNamedGuardOperationMachine",
            hsm.Operation("allow", async_operation),
            hsm.Initial(hsm.Target("idle")),
            hsm.State(
                "idle",
                hsm.Transition(
                    hsm.On("go"),
                    hsm.Guard("allow"),
                    hsm.Target("../done"),
                ),
            ),
            hsm.State("done"),
        )


@pytest.mark.asyncio
async def test_operation_call_requires_context_and_instance():
    class SignatureInstance(ParityInstance):
        pass

    instance = SignatureInstance()

    async def work(ctx: hsm.Context, inst: SignatureInstance, value: int) -> str:
        assert isinstance(ctx, hsm.Context)
        assert inst is instance
        return f"work:{value}"

    def record_call(
        ctx: hsm.Context, inst: SignatureInstance, event: hsm.Event
    ) -> None:
        assert isinstance(event.Data, hsm.CallData)
        inst.log.append(event.Data.name)

    model = hsm.Define(
        "SignatureCallMachine",
        hsm.Operation("work", work),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.OnCall("work"), hsm.Effect(record_call)),
        ),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)

    assert await hsm.Call(ctx, instance, "work", 1) == "work:1"
    assert instance.log == ["/SignatureCallMachine/work"]
    assert instance.take_snapshot().QueueLen == 0

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_oncall_does_not_dispatch_after_operation_exception():
    instance = ParityInstance()

    async def fail(ctx: hsm.Context, inst: ParityInstance, value: int) -> None:
        del ctx, inst
        raise RuntimeError(f"boom:{value}")

    def record_call(
        ctx: hsm.Context, inst: ParityInstance, event: hsm.Event
    ) -> None:
        assert isinstance(event.Data, hsm.CallData)
        inst.log.append(f"oncall:{event.Data.args[0]}")

    model = hsm.Define(
        "FailingCallMachine",
        hsm.Operation("fail", fail),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.OnCall("fail"),
                hsm.Target("../called"),
                hsm.Effect(record_call),
            ),
        ),
        hsm.State("called"),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)

    with pytest.raises(RuntimeError, match="boom:7"):
        await hsm.Call(ctx, instance, "fail", 7)

    assert instance.state() == "/FailingCallMachine/idle"
    assert instance.log == []
    assert instance.take_snapshot().QueueLen == 0

    await instance.stop(instance.context())


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
    await hsm.Started(ctx, instance, model)
    assert instance.state() == "/ObserverHistoryMachine/parent/a"

    await hsm.Dispatch(ctx, instance, hsm.Event(name="advance"))
    assert instance.state() == "/ObserverHistoryMachine/parent/b"

    await hsm.Dispatch(ctx, instance, hsm.Event(name="leave"))

    assert instance.state() == "/ObserverHistoryMachine/outside"

    await hsm.Dispatch(ctx, instance, hsm.Event(name="resume"))
    assert instance.state() == "/ObserverHistoryMachine/parent/b"

    await instance.restart(instance.context())
    assert instance.state() == "/ObserverHistoryMachine/parent/a"

    await instance.stop(instance.context())


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

    ctx = hsm.Context().WithValue(hsm.Keys.Instances, {})
    await hsm.Started(ctx, first, model)
    await hsm.Started(ctx, second, model)

    first_id = first.take_snapshot().ID
    second_id = second.take_snapshot().ID
    assert first_id != second_id

    await hsm.DispatchTo(ctx, hsm.Event(name="go"), first_id)
    assert first.state() == "/GroupMachine/done"
    assert second.state() == "/GroupMachine/idle"

    await hsm.DispatchAll(ctx, hsm.Event(name="go"))
    assert first.state() == "/GroupMachine/done"
    assert second.state() == "/GroupMachine/done"

    group = hsm.MakeGroup(first, hsm.NewGroup(second))
    assert hsm.MakeGroup is hsm.NewGroup
    assert hsm.group is hsm.Group
    assert group.state() == ["/GroupMachine/done", "/GroupMachine/done"]
    group_snapshots = group.take_snapshot()
    assert len(group_snapshots) == 2
    assert [snapshot.QualifiedName for snapshot in group_snapshots] == [
        "/GroupMachine",
        "/GroupMachine",
    ]
    assert [snapshot.State for snapshot in group_snapshots] == [
        "/GroupMachine/done",
        "/GroupMachine/done",
    ]
    assert all(snapshot.QueueLen == 0 for snapshot in group_snapshots)

    await group.stop(group.context())


@pytest.mark.asyncio
async def test_group_can_be_used_as_behavior():
    class Member(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.log: list[str] = []

    def record(ctx: hsm.Context, instance: Member, event: hsm.Event) -> None:
        instance.log.append(event.name)

    member_model = hsm.Define(
        "GroupBehaviorMember",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On(hsm.InitialEvent),
                hsm.Effect(record),
                hsm.Target("../idle"),
            ),
            hsm.Transition(hsm.On("effect"), hsm.Effect(record), hsm.Target("../idle")),
            hsm.Transition(hsm.On("leave"), hsm.Effect(record), hsm.Target("../idle")),
        ),
    )

    ctx = hsm.Context()
    member = Member()
    await hsm.Started(ctx, member, member_model)

    group = hsm.MakeGroup("behavior_group", member)
    assert isinstance(group, hsm.BehaviorElement)

    parent = ParityInstance()
    parent_model = hsm.Define(
        "GroupBehaviorMachine",
        hsm.Initial(hsm.Target("entry_state")),
        hsm.State(
            "entry_state",
            hsm.Entry(group),
            hsm.Transition(
                hsm.On("effect"),
                hsm.Effect(group),
                hsm.Target("../activity_state"),
            ),
        ),
        hsm.State(
            "activity_state",
            hsm.Activity(group),
            hsm.Transition(hsm.On("leave"), hsm.Target("../exit_state")),
        ),
        hsm.State(
            "exit_state",
            hsm.Exit(group),
            hsm.Transition(hsm.On("leave"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )
    await hsm.Started(ctx, parent, parent_model)
    await asyncio.sleep(0.01)
    assert member.log == [hsm.InitialEvent.name]

    await hsm.Dispatch(ctx, parent, hsm.Event(name="effect"))
    await asyncio.sleep(0.01)
    assert member.log == [hsm.InitialEvent.name, "effect", "effect"]

    await hsm.Dispatch(ctx, parent, hsm.Event(name="leave"))
    await hsm.Dispatch(ctx, parent, hsm.Event(name="leave"))
    await asyncio.sleep(0.01)
    assert member.log == [hsm.InitialEvent.name, "effect", "effect", "leave"]

    await hsm.Stop(hsm.MakeGroup(parent, member))
