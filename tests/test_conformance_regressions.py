import asyncio
import os
from pathlib import Path
import subprocess
import sys
from collections import deque
from datetime import timedelta

import pytest

import hsm


class RegressionInstance(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.log: list[str] = []


async def _guard_error_preserves_source_state_and_does_not_take_fallback() -> None:
    instance = RegressionInstance()

    def bad_guard(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> bool:
        inst.log.append("guard:bad")
        raise RuntimeError("boom")

    def entry_target(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:target")

    model = hsm.Define(
        "GuardErrorPreservesSourceRegression",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../target"), hsm.Guard(bad_guard)),
            hsm.Transition(hsm.On("go"), hsm.Target("../fallback")),
        ),
        hsm.State("target", hsm.Entry(entry_target)),
        hsm.State("fallback"),
    )

    await hsm.Start(hsm.Context(), instance, model)
    with pytest.raises(RuntimeError, match="boom"):
        await hsm.Dispatch(hsm.Context(), instance, hsm.Event("go"))

    assert instance.state() == "/GuardErrorPreservesSourceRegression/idle"
    assert instance.log == ["guard:bad"]


def test_guard_error_preserves_source_state_and_does_not_take_fallback():
    asyncio.run(_guard_error_preserves_source_state_and_does_not_take_fallback())


async def _async_effect_error_stops_before_target_entry() -> None:
    instance = RegressionInstance()

    def bad_effect(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect:before-yield")
        inst.log.append("effect:after-yield")
        raise RuntimeError("effect boom")

    def effect_after(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect:after")

    def entry_target(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:target")

    model = hsm.Define(
        "AsyncEffectErrorStopsBeforeEntryRegression",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("go"),
                hsm.Target("../target"),
                hsm.Effect(bad_effect),
                hsm.Effect(effect_after),
            ),
        ),
        hsm.State("target", hsm.Entry(entry_target)),
    )

    await hsm.Start(hsm.Context(), instance, model)
    with pytest.raises(RuntimeError, match="effect boom"):
        await hsm.Dispatch(hsm.Context(), instance, hsm.Event("go"))

    assert instance.state() == "/AsyncEffectErrorStopsBeforeEntryRegression/idle"
    assert instance.log == ["effect:before-yield", "effect:after-yield"]


def test_async_effect_error_stops_before_target_entry():
    asyncio.run(_async_effect_error_stops_before_target_entry())


async def _behavior_context_dispatch_all_sees_root_context_machines() -> None:
    class Producer(hsm.Instance):
        pass

    class Worker(RegressionInstance):
        pass

    def dispatch_all(ctx: hsm.Context, inst: Producer, event: hsm.Event) -> None:
        asyncio.ensure_future(hsm.DispatchAll(ctx, hsm.Event("audit")))

    def mark_worker(ctx: hsm.Context, inst: Worker, event: hsm.Event) -> None:
        inst.log.append("effect:audit")

    producer_model = hsm.Define(
        "BehaviorContextDispatchAllProducer",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("go"), hsm.Target("../sent"), hsm.Effect(dispatch_all)
            ),
        ),
        hsm.State("sent"),
    )
    worker_model = hsm.Define(
        "BehaviorContextDispatchAllWorker",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("audit"), hsm.Target("../done"), hsm.Effect(mark_worker)
            ),
        ),
        hsm.State("done"),
    )

    ctx = hsm.Context()
    producer = Producer()
    started = Worker()
    stopped = Worker()
    await hsm.Start(ctx, producer, producer_model, hsm.Config(ID="producer"))
    await hsm.Start(ctx, started, worker_model, hsm.Config(ID="started"))
    await hsm.Start(ctx, stopped, worker_model, hsm.Config(ID="stopped"))
    await hsm.Stop(stopped)

    await hsm.Dispatch(ctx, producer, hsm.Event("go"))

    assert producer.state() == "/BehaviorContextDispatchAllProducer/sent"
    assert started.state() == "/BehaviorContextDispatchAllWorker/done"
    assert stopped.state() == "/BehaviorContextDispatchAllWorker"
    assert started.log == ["effect:audit"]
    assert stopped.log == []


def test_behavior_context_dispatch_all_sees_root_context_machines():
    asyncio.run(_behavior_context_dispatch_all_sees_root_context_machines())


async def _activity_explicit_dispatch_is_not_deferred_as_generated_change_event() -> (
    None
):
    instance = RegressionInstance()

    async def activity(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        await asyncio.sleep(0)
        await inst.dispatch(hsm.Event("audit"))
        inst.log.append("activity:after-dispatch")

    def audit_effect(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect:audit")

    model = hsm.Define(
        "ActivityExplicitDispatchRegression",
        hsm.Initial(hsm.Target("active")),
        hsm.State(
            "active",
            hsm.Activity(activity),
            hsm.Transition(
                hsm.On("audit"), hsm.Target("../done"), hsm.Effect(audit_effect)
            ),
        ),
        hsm.State("done"),
    )

    await hsm.Start(hsm.Context(), instance, model)
    await asyncio.sleep(0.01)

    assert instance.state() == "/ActivityExplicitDispatchRegression/done"
    assert instance.log == ["effect:audit"]


def test_activity_explicit_dispatch_is_not_deferred_as_generated_change_event():
    asyncio.run(_activity_explicit_dispatch_is_not_deferred_as_generated_change_event())


async def _entry_snapshot_observes_entered_state() -> None:
    instance = RegressionInstance()

    def snapshot_entry(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append(hsm.TakeSnapshot(ctx, inst).state)

    model = hsm.Define(
        "EntrySnapshotRegression",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Entry(snapshot_entry)),
    )

    await hsm.Start(hsm.Context(), instance, model)

    assert instance.log == ["/EntrySnapshotRegression/idle"]
    assert instance.state() == "/EntrySnapshotRegression/idle"


def test_entry_snapshot_observes_entered_state():
    asyncio.run(_entry_snapshot_observes_entered_state())


async def _failed_nested_initial_preserves_entered_parent_state() -> None:
    instance = RegressionInstance()

    def parent_entry(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:parent")

    def bad_initial(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("initial:bad")
        raise RuntimeError("nested initial boom")

    def child_entry(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:child")

    model = hsm.Define(
        "FailedNestedInitialPreservesParentRegression",
        hsm.Initial(hsm.Target("parent")),
        hsm.State(
            "parent",
            hsm.Entry(parent_entry),
            hsm.Initial(hsm.Target("child"), hsm.Effect(bad_initial)),
            hsm.State("child", hsm.Entry(child_entry)),
        ),
    )

    with pytest.raises(Exception, match="nested initial boom"):
        await hsm.Start(hsm.Context(), instance, model)

    assert instance.state() == "/FailedNestedInitialPreservesParentRegression/parent"
    assert instance.log == ["entry:parent", "initial:bad"]


def test_failed_nested_initial_preserves_entered_parent_state():
    asyncio.run(_failed_nested_initial_preserves_entered_parent_state())


async def _nested_initial_effect_snapshot_observes_pre_initial_source() -> None:
    instance = RegressionInstance()

    def parent_entry(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:parent")

    def snapshot_initial(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append(hsm.TakeSnapshot(ctx, inst).state)

    def child_entry(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:child")

    model = hsm.Define(
        "NestedInitialSnapshotRegression",
        hsm.Initial(hsm.Target("parent")),
        hsm.State(
            "parent",
            hsm.Entry(parent_entry),
            hsm.Initial(hsm.Target("child"), hsm.Effect(snapshot_initial)),
            hsm.State("child", hsm.Entry(child_entry)),
        ),
    )

    await hsm.Start(hsm.Context(), instance, model)

    assert instance.state() == "/NestedInitialSnapshotRegression/parent/child"
    assert instance.log == [
        "entry:parent",
        "/NestedInitialSnapshotRegression",
        "entry:child",
    ]


def test_nested_initial_effect_snapshot_observes_pre_initial_source():
    asyncio.run(_nested_initial_effect_snapshot_observes_pre_initial_source())


async def _duplicate_timer_trigger_fallback_replays_guard_events_after_entry() -> None:
    instance = RegressionInstance()

    async def duration(ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event):
        return timedelta(milliseconds=1)

    def false_guard(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> bool:
        inst.log.append("guard:false")
        return False

    def schedule_audit(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        asyncio.ensure_future(inst.dispatch(hsm.Event("audit")))

    def fallback_effect(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect:fallback")

    def fallback_entry(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:fallback")

    def audit_effect(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect:audit")

    model = hsm.Define(
        "DuplicateTimerTriggerFallbackRegression",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(hsm.After(duration), hsm.Target("../route")),
        ),
        hsm.Choice(
            "route",
            hsm.Transition(hsm.Guard(false_guard), hsm.Target("wrong")),
            hsm.Transition(
                hsm.Target("fallback"),
                hsm.Effect(fallback_effect),
                hsm.Effect(schedule_audit),
            ),
        ),
        hsm.State(
            "fallback",
            hsm.Entry(fallback_entry),
            hsm.Transition(
                hsm.On("audit"), hsm.Target("../done"), hsm.Effect(audit_effect)
            ),
        ),
        hsm.State("done"),
        hsm.State("wrong"),
    )

    await hsm.Started(hsm.Context(), instance, model)
    await asyncio.sleep(0.05)

    assert instance.state() == "/DuplicateTimerTriggerFallbackRegression/done"
    assert instance.log == [
        "guard:false",
        "effect:fallback",
        "entry:fallback",
        "effect:audit",
    ]


def test_duplicate_timer_trigger_fallback_replays_guard_events_after_entry():
    asyncio.run(_duplicate_timer_trigger_fallback_replays_guard_events_after_entry())


async def _false_guarded_deferred_event_replays_after_release() -> None:
    instance = RegressionInstance()

    def false_guard(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> bool:
        inst.log.append("guard:false")
        return False

    def release_effect(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect:release")

    def maybe_effect(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect:maybe")

    model = hsm.Define(
        "FalseGuardDeferredReplayRegression",
        hsm.Initial(hsm.Target("blocked")),
        hsm.State(
            "blocked",
            hsm.Defer("maybe"),
            hsm.Transition(
                hsm.On("maybe"), hsm.Guard(false_guard), hsm.Target("../wrong")
            ),
            hsm.Transition(
                hsm.On("release"), hsm.Target("../ready"), hsm.Effect(release_effect)
            ),
        ),
        hsm.State(
            "ready",
            hsm.Transition(
                hsm.On("maybe"), hsm.Target("../done"), hsm.Effect(maybe_effect)
            ),
        ),
        hsm.State("done"),
        hsm.State("wrong"),
    )

    await hsm.Start(hsm.Context(), instance, model)
    await hsm.Dispatch(hsm.Context(), instance, hsm.Event("maybe"))
    await hsm.Dispatch(hsm.Context(), instance, hsm.Event("release"))

    assert instance.state() == "/FalseGuardDeferredReplayRegression/done"
    assert instance.log == ["guard:false", "effect:release", "effect:maybe"]


def test_false_guarded_deferred_event_replays_after_release():
    asyncio.run(_false_guarded_deferred_event_replays_after_release())


async def _transition_to_deep_history_preserves_previous_history_snapshot() -> None:
    instance = RegressionInstance()

    def entry_a(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:a")

    def entry_leaf(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:leaf")

    model = hsm.Define(
        "DeepHistoryTargetPreservesSnapshotRegression",
        hsm.Initial(hsm.Target("comp")),
        hsm.State(
            "comp",
            hsm.Initial(hsm.Target("a")),
            hsm.State("a", hsm.Entry(entry_a)),
            hsm.State(
                "b",
                hsm.Initial(hsm.Target("leaf")),
                hsm.State("leaf", hsm.Entry(entry_leaf)),
            ),
            hsm.DeepHistory("h", hsm.Transition(hsm.Target("a"))),
            hsm.Transition(hsm.Source("a"), hsm.On("to_b"), hsm.Target("b/leaf")),
            hsm.Transition(hsm.Source("b"), hsm.On("to_a"), hsm.Target("a")),
            hsm.Transition(hsm.Source("a"), hsm.On("resume"), hsm.Target("h")),
        ),
    )

    await hsm.Start(hsm.Context(), instance, model)
    await hsm.Dispatch(hsm.Context(), instance, hsm.Event("to_b"))
    await hsm.Dispatch(hsm.Context(), instance, hsm.Event("to_a"))
    await hsm.Dispatch(hsm.Context(), instance, hsm.Event("resume"))

    assert (
        instance.state() == "/DeepHistoryTargetPreservesSnapshotRegression/comp/b/leaf"
    )
    assert instance.log == ["entry:a", "entry:leaf", "entry:a", "entry:leaf"]


def test_transition_to_deep_history_preserves_previous_history_snapshot():
    asyncio.run(_transition_to_deep_history_preserves_previous_history_snapshot())


async def _restart_after_stop_requires_started_machine() -> None:
    instance = RegressionInstance()
    model = hsm.Define(
        "RestartAfterStopRequiresStartedRegression",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )

    await hsm.Start(hsm.Context(), instance, model)
    await hsm.Stop(instance)

    with pytest.raises(hsm.ValidationError, match="started"):
        await hsm.Restart(instance)

    assert instance.state() == "/RestartAfterStopRequiresStartedRegression"


def test_restart_after_stop_requires_started_machine():
    asyncio.run(_restart_after_stop_requires_started_machine())


async def _lifecycle_errors_are_normalized_for_started_and_stopped_machines() -> None:
    instance = RegressionInstance()
    model = hsm.Define(
        "LifecycleErrorNormalizationRegression",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )

    await hsm.Started(hsm.Context(), instance, model)
    with pytest.raises(hsm.ValidationError, match="already"):
        await hsm.Started(hsm.Context(), instance, model)

    await hsm.Stop(instance)
    await hsm.Stop(instance)


def test_lifecycle_errors_are_normalized_for_started_and_stopped_machines():
    asyncio.run(_lifecycle_errors_are_normalized_for_started_and_stopped_machines())


async def _async_when_predicate_uses_async_dispatch_path() -> None:
    instance = RegressionInstance()

    def when_ready(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> bool:
        inst.log.append("when")
        return True

    def effect_done(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect")

    model = hsm.Define(
        "AsyncWhenPredicateRegression",
        hsm.Attribute("ready", False),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.When(when_ready), hsm.Target("../done"), hsm.Effect(effect_done)
            ),
        ),
        hsm.State("done"),
    )

    await hsm.Start(hsm.Context(), instance, model)
    await hsm.Set(hsm.Context(), instance, "ready", True)

    assert instance.state() == "/AsyncWhenPredicateRegression/done"
    assert instance.log == ["when", "effect"]


def test_async_when_predicate_uses_async_dispatch_path():
    asyncio.run(_async_when_predicate_uses_async_dispatch_path())


async def _history_default_guard_reentrant_events_replay_after_default_entry() -> None:
    instance = RegressionInstance()

    def guard(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> bool:
        inst.log.append(hsm.TakeSnapshot(ctx, inst).state)
        return True

    def dispatch_audit(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        asyncio.ensure_future(inst.dispatch(hsm.Event("audit")))

    def entry_leaf(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("entry:leaf")

    def effect_audit(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect:audit")

    model = hsm.Define(
        "HistoryDefaultGuardRegression",
        hsm.Initial(hsm.Target("outside")),
        hsm.State("outside", hsm.Transition(hsm.On("enter"), hsm.Target("../comp/h"))),
        hsm.State(
            "comp",
            hsm.State(
                "parent",
                hsm.Initial(hsm.Target("leaf")),
                hsm.State(
                    "leaf",
                    hsm.Entry(entry_leaf),
                    hsm.Transition(
                        hsm.On("audit"), hsm.Target("../seen"), hsm.Effect(effect_audit)
                    ),
                ),
                hsm.State("seen"),
            ),
            hsm.DeepHistory(
                "h",
                hsm.Transition(
                    hsm.Guard(guard),
                    hsm.Target("parent"),
                    hsm.Effect(dispatch_audit),
                ),
            ),
        ),
    )

    await hsm.Start(hsm.Context(), instance, model)
    await hsm.Dispatch(hsm.Context(), instance, hsm.Event("enter"))

    assert instance.log == [
        "/HistoryDefaultGuardRegression/outside",
        "entry:leaf",
        "effect:audit",
    ]
    assert instance.state() == "/HistoryDefaultGuardRegression/comp/parent/seen"


def test_history_default_guard_reentrant_events_replay_after_default_entry():
    asyncio.run(_history_default_guard_reentrant_events_replay_after_default_entry())


async def _explicit_dynamic_attribute_accepts_none_after_default_value() -> None:
    instance = RegressionInstance()
    model = hsm.Define(
        "ExplicitDynamicAttributeRegression",
        hsm.Attribute("value", None, "present"),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )

    await hsm.Start(hsm.Context(), instance, model)
    await hsm.Set(hsm.Context(), instance, "value", None)

    assert hsm.TakeSnapshot(hsm.Context(), instance).Attributes == {
        "/ExplicitDynamicAttributeRegression/value": None,
    }


def test_explicit_dynamic_attribute_accepts_none_after_default_value():
    asyncio.run(_explicit_dynamic_attribute_accepts_none_after_default_value())


def test_conformance_runner_reports_unknown_group_with_group_context():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    result = subprocess.run(
        [
            sys.executable,
            "conformance/run_case.py",
            "../conformance/cases/group_dispatch_unknown_group_error.json",
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stdout + result.stderr


async def _snapshot_events_include_ancestor_and_targetless_transitions() -> None:
    instance = RegressionInstance()
    model = hsm.Define(
        "SnapshotEventDetailsRegression",
        hsm.Initial(hsm.Target("parent")),
        hsm.State(
            "parent",
            hsm.Initial(hsm.Target("child")),
            hsm.Transition(hsm.On("parent_go"), hsm.Target("../done")),
            hsm.State(
                "child",
                hsm.Transition(hsm.On("ping")),
            ),
        ),
        hsm.State("done"),
    )

    await hsm.Start(hsm.Context(), instance, model)
    snapshot = hsm.TakeSnapshot(hsm.Context(), instance)

    event_details = {event.Name: event for event in snapshot.Events}
    assert event_details["parent_go"].Target == "/SnapshotEventDetailsRegression/done"
    assert event_details["ping"].Target is None


def test_snapshot_events_include_ancestor_and_targetless_transitions():
    asyncio.run(_snapshot_events_include_ancestor_and_targetless_transitions())


async def _deferred_queue_replay_does_not_require_event_identity() -> None:
    instance = RegressionInstance()
    events: deque[hsm.Event] = deque()

    def clone_event(event: hsm.Event) -> hsm.Event:
        return hsm.Event(
            name=event.name,
            data=event.data,
            kind=event.kind,
            id=event.id,
            source=event.source,
            target=event.target,
            qualified_name=event.qualified_name,
            schema=event.schema,
        )

    class CloneFifo(hsm.Fifo):
        def push(self, event: hsm.Event) -> hsm.QueuePushResult:
            events.append(clone_event(event))
            return (None,)

        def pop(self) -> hsm.QueuePopResult:
            if not events:
                return (hsm.Event(), False, None)
            return (clone_event(events.popleft()), True, None)

    model = hsm.Define(
        "DeferredCloneQueueRegression",
        hsm.Initial(hsm.Target("blocked")),
        hsm.State(
            "blocked",
            hsm.Defer("work"),
            hsm.Transition(hsm.On("release"), hsm.Target("../ready")),
        ),
        hsm.State("ready", hsm.Transition(hsm.On("work"), hsm.Target("../done"))),
        hsm.State("done"),
    )

    ctx = hsm.Context()
    await hsm.Start(ctx, instance, model, hsm.Config(Queue=hsm.MultiQueue(CloneFifo())))
    await hsm.Dispatch(ctx, instance, hsm.Event("work"))
    await hsm.Dispatch(ctx, instance, hsm.Event("work"))
    await hsm.Dispatch(ctx, instance, hsm.Event("release"))

    snapshot = hsm.TakeSnapshot(ctx, instance)
    assert snapshot.state == "/DeferredCloneQueueRegression/done"
    assert snapshot.queue_len == 0


def test_deferred_queue_replay_does_not_require_event_identity():
    asyncio.run(_deferred_queue_replay_does_not_require_event_identity())


async def _every_timer_reschedules_after_dispatch_processing() -> None:
    instance = RegressionInstance()
    sleeps: list[int] = []
    sleepers: deque[asyncio.Future[None]] = deque()

    async def sleep(duration: timedelta) -> None:
        sleeps.append(round(duration.total_seconds() * 1000))
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        sleepers.append(future)
        await future

    async def interval(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> timedelta:
        value, _ = hsm.Get(ctx, inst, "interval_ms")
        return timedelta(milliseconds=value)

    async def tick(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> None:
        inst.log.append("effect:tick")
        await hsm.Set(ctx, inst, "interval_ms", 50)

    model = hsm.Define(
        "EveryTimerRescheduleAfterDispatchRegression",
        hsm.Attribute("interval_ms", 10),
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(
                hsm.Every(interval),
                hsm.Target("../waiting"),
                hsm.Effect(tick),
            ),
            hsm.Transition(hsm.On("stop"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )

    await hsm.Started(
        hsm.Context(),
        instance,
        model,
        hsm.Config(Clock=hsm.Clock(sleep=sleep)),
    )
    for _ in range(10):
        await asyncio.sleep(0)
        if sleeps:
            break
    assert sleeps == [10]

    sleepers.popleft().set_result(None)
    for _ in range(10):
        await asyncio.sleep(0)
        if sleeps == [10, 50]:
            break

    assert instance.log == ["effect:tick"]
    assert sleeps == [10, 50]

    await hsm.Dispatch(hsm.Context(), instance, hsm.Event("stop"))


def test_every_timer_reschedules_after_dispatch_processing():
    asyncio.run(_every_timer_reschedules_after_dispatch_processing())


async def _same_source_same_callback_timers_do_not_collapse() -> None:
    instance = RegressionInstance()
    sleepers: deque[asyncio.Future[None]] = deque()

    async def sleep(duration: timedelta) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        sleepers.append(future)
        await future

    async def interval(
        ctx: hsm.Context, inst: RegressionInstance, event: hsm.Event
    ) -> timedelta:
        return timedelta(milliseconds=10)

    model = hsm.Define(
        "DuplicateTimerTriggerRegression",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(hsm.After(interval), hsm.Target("../first")),
            hsm.Transition(hsm.After(interval), hsm.Target("../second")),
        ),
        hsm.State("first"),
        hsm.State("second"),
    )

    await hsm.Started(
        hsm.Context(),
        instance,
        model,
        hsm.Config(Clock=hsm.Clock(sleep=sleep)),
    )
    for _ in range(10):
        await asyncio.sleep(0)
        if len(sleepers) == 2:
            break

    assert len(sleepers) == 2
    waiting = model.get("/DuplicateTimerTriggerRegression/waiting", hsm.StateElement)
    assert waiting is not None
    first_transition = model.get(waiting.transitions[0], hsm.TransitionElement)
    second_transition = model.get(waiting.transitions[1], hsm.TransitionElement)
    assert first_transition is not None and second_transition is not None
    assert first_transition.events != second_transition.events


def test_same_source_same_callback_timers_do_not_collapse():
    asyncio.run(_same_source_same_callback_timers_do_not_collapse())


def test_conformance_runner_places_timer_fired_after_true_guard_ops():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    result = subprocess.run(
        [
            sys.executable,
            "conformance/run_case.py",
            "../conformance/cases/timer_guard_all_ops_reentrancy.json",
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_timer_source_behavior_reentrant_ops_do_not_deadlock():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    result = subprocess.run(
        [
            sys.executable,
            "conformance/run_case.py",
            "../conformance/cases/timer_behavior_source_all_ops_reentrancy.json",
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_conformance_runner_trace_yield_sleep_waits_for_logical_ticks():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    result = subprocess.run(
        [
            sys.executable,
            "conformance/run_case.py",
            "../conformance/cases/config_clock_timer_every_attribute_reschedule_changed_interval.json",
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_conformance_runner_does_not_trace_nested_behavior_calls_as_script_calls():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    result = subprocess.run(
        [
            sys.executable,
            "conformance/run_case.py",
            "../conformance/cases/operation_call_from_operation_body_reentrancy.json",
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_conformance_runner_traces_state_entry_behavior_calls():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    result = subprocess.run(
        [
            sys.executable,
            "conformance/run_case.py",
            "../conformance/cases/submodel_operation_body_executes_in_child_context.json",
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_conformance_runner_traces_defer_after_guard_fallthrough():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    result = subprocess.run(
        [
            sys.executable,
            "conformance/run_case.py",
            "../conformance/cases/nested_child_false_guard_falls_through_to_parent_defer.json",
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_conformance_runner_pretraces_direct_defer_before_queue_hooks():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    result = subprocess.run(
        [
            sys.executable,
            "conformance/run_case.py",
            "../conformance/cases/config_queue_deferred_replay_fifo.json",
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stdout + result.stderr
