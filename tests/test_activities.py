"""
Test state activities (concurrent operations)
Tests async activities that run while in a state and can be aborted on cancellation
"""

import asyncio
import pytest
import typing
from hsm import hsm
from hsm.hsm import Instance, Event, Context


class ActivityInstance(Instance):
    def __init__(self):
        super().__init__()
        self.log: list[str] = []
        self.data: dict[str, typing.Any] = {
            "activity_started": 0,
            "activity_completed": 0,
            "activity_aborted": 0,
            "sync_activity_ran": False,
            "parent_activity_active": False,
            "child_activity_active": False,
            "activity_event": None,
            "processed_trigger": None,
            "error_data": None,
            "final_count": 0,
            "activity_count": 0,
        }

    def log_action(self, action: str):
        self.log.append(action)


@pytest.mark.asyncio
async def test_basic_activity_starts_on_entry_stops_on_exit():
    """Basic activity - starts on entry, stops on exit"""
    instance = ActivityInstance()
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    async def basic_activity(ctx: Context, inst: ActivityInstance, event: Event):
        inst.log_action("activity-started")
        inst.data["activity_started"] += 1
        started.set()

        async def handle_cancellation():
            try:
                await release.wait()
                if not ctx.is_done():
                    inst.log_action("activity-completed")
                    inst.data["activity_completed"] += 1
            except asyncio.CancelledError:
                inst.log_action("activity-aborted")
                inst.data["activity_aborted"] += 1
                cancelled.set()
                raise

        return await handle_cancellation()

    model = hsm.define(
        "BasicActivityMachine",
        hsm.initial(hsm.target("active")),
        hsm.state(
            "active",
            hsm.activity(basic_activity),
            hsm.transition(hsm.on("stop"), hsm.target("../inactive")),
        ),
        hsm.state("inactive"),
    )

    ctx = Context()
    sm = await hsm.started(ctx=ctx, instance=instance, model=model)

    await asyncio.wait_for(started.wait(), timeout=1)

    # Activity should start immediately
    assert "activity-started" in instance.log
    assert instance.data["activity_started"] == 1

    # Exit state before activity completes
    await sm.dispatch(ctx, Event(name="stop"))

    await asyncio.wait_for(cancelled.wait(), timeout=1)

    # Activity should be aborted
    assert "activity-aborted" in instance.log
    assert instance.data["activity_aborted"] == 1
    assert instance.data["activity_completed"] == 0

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_multiple_concurrent_activities():
    """Multiple concurrent activities"""
    instance = ActivityInstance()
    activity1_started = asyncio.Event()
    activity1_release = asyncio.Event()
    activity1_completed = asyncio.Event()
    activity2_started = asyncio.Event()
    activity2_cancelled = asyncio.Event()

    async def activity1(ctx: Context, inst: ActivityInstance, event: Event):
        inst.log_action("activity1-started")
        activity1_started.set()
        try:
            await activity1_release.wait()
            inst.log_action("activity1-completed")
            activity1_completed.set()
        except asyncio.CancelledError:
            inst.log_action("activity1-aborted")
            raise

    async def activity2(ctx: Context, inst: ActivityInstance, event: Event):
        inst.log_action("activity2-started")
        activity2_started.set()
        try:
            await asyncio.Event().wait()
            inst.log_action("activity2-completed")
        except asyncio.CancelledError:
            inst.log_action("activity2-aborted")
            activity2_cancelled.set()
            raise

    async def activity3(ctx: Context, inst: ActivityInstance, event: Event):
        inst.log_action("activity3-started")
        # Synchronous activity
        inst.data["sync_activity_ran"] = True

    model = hsm.define(
        "MultipleActivityMachine",
        hsm.initial(hsm.target("busy")),
        hsm.state(
            "busy",
            hsm.activity(activity1, activity2, activity3),
            hsm.transition(hsm.on("stop"), hsm.target("../done")),
        ),
        hsm.state("done"),
    )

    ctx = Context()
    sm = await hsm.started(ctx, instance, model)

    await asyncio.wait_for(activity1_started.wait(), timeout=1)
    await asyncio.wait_for(activity2_started.wait(), timeout=1)

    # All activities should start
    assert "activity1-started" in instance.log
    assert "activity2-started" in instance.log
    assert "activity3-started" in instance.log
    assert instance.data["sync_activity_ran"] is True

    activity1_release.set()
    await asyncio.wait_for(activity1_completed.wait(), timeout=1)
    assert "activity1-completed" in instance.log
    assert "activity2-completed" not in instance.log

    await sm.dispatch(ctx, Event(name="stop"))
    await asyncio.wait_for(activity2_cancelled.wait(), timeout=1)

    # Activity2 should be aborted
    assert "activity2-aborted" in instance.log
    assert "activity2-completed" not in instance.log

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_activity_error_handling():
    """Activity error handling"""
    instance = ActivityInstance()
    error_event_received = False
    error_handled = asyncio.Event()

    async def error_activity(ctx: Context, inst: ActivityInstance, event: Event):
        inst.log_action("activity-throwing")
        raise Exception("Activity error!")

    def error_effect(ctx: Context, inst: ActivityInstance, event: Event):
        nonlocal error_event_received
        inst.log_action("error-handled")
        inst.data["error_data"] = event.data
        error_event_received = True
        error_handled.set()

    model = hsm.define(
        "ActivityErrorMachine",
        hsm.initial(hsm.target("working")),
        hsm.state(
            "working",
            hsm.activity(error_activity),
            hsm.transition(
                hsm.on("hsm/error"), hsm.target("../error"), hsm.effect(error_effect)
            ),
        ),
        hsm.state("error"),
    )

    ctx = Context()
    sm = await hsm.started(ctx, instance, model)

    await asyncio.wait_for(error_handled.wait(), timeout=1)

    assert error_event_received is True
    assert sm.state() == "/ActivityErrorMachine/error"
    assert "Activity error!" in str(instance.data["error_data"])

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_activities_in_hierarchical_states():
    """Activities in hierarchical states"""
    instance = ActivityInstance()
    parent_started = asyncio.Event()
    child_started = asyncio.Event()
    parent_aborted = asyncio.Event()
    child_aborted = asyncio.Event()

    async def parent_activity(ctx: Context, inst: ActivityInstance, event: Event):
        inst.log_action("parent-activity-started")
        inst.data["parent_activity_active"] = True
        parent_started.set()
        try:
            # Keep running until cancelled
            while not ctx.is_done():
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            inst.log_action("parent-activity-aborted")
            inst.data["parent_activity_active"] = False
            parent_aborted.set()
            raise

    async def child_activity(ctx: Context, inst: ActivityInstance, event: Event):
        inst.log_action("child-activity-started")
        inst.data["child_activity_active"] = True
        child_started.set()
        try:
            # Keep running until cancelled
            while not ctx.is_done():
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            inst.log_action("child-activity-aborted")
            inst.data["child_activity_active"] = False
            child_aborted.set()
            raise

    model = hsm.define(
        "HierarchicalActivityMachine",
        hsm.initial(hsm.target("parent/child")),
        hsm.state(
            "parent",
            hsm.activity(parent_activity),
            hsm.state(
                "child",
                hsm.activity(child_activity),
                hsm.transition(
                    hsm.on("up"),
                    hsm.target(".."),  # Exit to parent level
                ),
            ),
            hsm.transition(hsm.on("out"), hsm.target("../outside")),
        ),
        hsm.state("outside"),
    )

    ctx = Context()
    sm = await hsm.started(ctx, instance, model)

    await asyncio.wait_for(parent_started.wait(), timeout=1)
    await asyncio.wait_for(child_started.wait(), timeout=1)

    # Both activities should start
    assert instance.log == ["parent-activity-started", "child-activity-started"]
    assert instance.data["parent_activity_active"] is True
    assert instance.data["child_activity_active"] is True

    # Exit child state
    await sm.dispatch(ctx, Event(name="up"))
    await asyncio.wait_for(child_aborted.wait(), timeout=1)

    # Only child activity should be aborted
    assert "child-activity-aborted" in instance.log
    assert instance.data["child_activity_active"] is False
    assert instance.data["parent_activity_active"] is True

    # Exit parent state
    await sm.dispatch(ctx, Event(name="out"))
    await asyncio.wait_for(parent_aborted.wait(), timeout=1)

    # Parent activity should now be aborted
    assert "parent-activity-aborted" in instance.log
    assert instance.data["parent_activity_active"] is False

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_activity_with_event_data_access():
    """Activity with event data access"""
    instance = ActivityInstance()
    instance.data["trigger"] = "test-value"
    activity_started = asyncio.Event()

    async def event_data_activity(ctx: Context, inst: ActivityInstance, event: Event):
        inst.log_action(f"activity-event-{event.name}")
        inst.data["activity_event"] = event
        inst.data["processed_trigger"] = inst.data["trigger"]
        activity_started.set()

    model = hsm.define(
        "EventDataActivityMachine",
        hsm.initial(hsm.target("processing")),
        hsm.state("processing", hsm.activity(event_data_activity)),
    )

    ctx = Context()
    sm = await hsm.started(ctx, instance, model)

    await asyncio.wait_for(activity_started.wait(), timeout=1)

    # Activity should receive the initial event
    assert instance.data["activity_event"].name == "hsm/initial"
    assert instance.data["processed_trigger"] == "test-value"
    assert instance.log == ["activity-event-hsm/initial"]

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_long_running_activity_completion():
    """Long-running activity completion"""
    instance = ActivityInstance()
    completed = asyncio.Event()

    async def long_running_activity(ctx: Context, inst: ActivityInstance, event: Event):
        inst.log_action("long-activity-started")

        count = 0
        try:
            while not ctx.is_done() and count < 3:
                await asyncio.sleep(0.02)  # 20ms
                count += 1
                inst.log_action(f"long-activity-tick-{count}")

            if count >= 3:
                inst.log_action("long-activity-completed")
                inst.data["final_count"] = count
                completed.set()
        except asyncio.CancelledError:
            inst.log_action(f"long-activity-aborted-at-{count}")
            raise

    model = hsm.define(
        "LongRunningActivityMachine",
        hsm.initial(hsm.target("working")),
        hsm.state("working", hsm.activity(long_running_activity)),
    )

    ctx = Context()
    sm = await hsm.started(ctx, instance, model)

    await asyncio.wait_for(completed.wait(), timeout=1)

    assert "long-activity-started" in instance.log
    assert "long-activity-tick-1" in instance.log
    assert "long-activity-tick-2" in instance.log
    assert "long-activity-tick-3" in instance.log
    assert "long-activity-completed" in instance.log
    assert instance.data["final_count"] == 3

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_activity_reentry_behavior():
    """Activity re-entry behavior"""
    instance = ActivityInstance()
    activity_count_changed = asyncio.Condition()

    async def reentry_activity(ctx: Context, inst: ActivityInstance, event: Event):
        async with activity_count_changed:
            inst.data["activity_count"] = inst.data.get("activity_count", 0) + 1
            inst.log_action(f"activity-run-{inst.data['activity_count']}")
            activity_count_changed.notify_all()

    async def wait_for_activity_count(count: int) -> None:
        async with activity_count_changed:
            await asyncio.wait_for(
                activity_count_changed.wait_for(lambda: instance.data["activity_count"] >= count),
                timeout=1,
            )

    model = hsm.define(
        "ReentryActivityMachine",
        hsm.initial(hsm.target("active")),
        hsm.state(
            "active",
            hsm.activity(reentry_activity),
            hsm.transition(
                hsm.on("restart"),
                hsm.target("."),  # Self transition - stay in current state
            ),
        ),
    )

    ctx = Context()
    sm = await hsm.started(ctx, instance, model)

    await wait_for_activity_count(1)

    assert instance.data["activity_count"] == 1
    assert instance.log == ["activity-run-1"]

    # Self transition should restart activity
    await sm.dispatch(ctx, Event(name="restart"))
    await wait_for_activity_count(2)

    assert instance.data["activity_count"] == 2
    assert "activity-run-2" in instance.log

    # Another restart
    await sm.dispatch(ctx, Event(name="restart"))
    await wait_for_activity_count(3)

    assert instance.data["activity_count"] == 3
    assert "activity-run-3" in instance.log

    await hsm.stop(sm)
