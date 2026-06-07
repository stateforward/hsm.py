import asyncio
import inspect

import pytest

import hsm


class ResultInstance(hsm.Instance):
    pass


def _result_model() -> hsm.Model:
    return hsm.Define(
        "ResultMachine",
        hsm.Attribute("flag", False),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )


def _async_result_model() -> hsm.Model:
    async def effect(ctx: hsm.Context, inst: ResultInstance, event: hsm.Event) -> None:
        await asyncio.sleep(0)

    return hsm.Define(
        "AsyncResultMachine",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../done"), hsm.Effect(effect)),
        ),
        hsm.State("done"),
    )


def _deferred_model(name: str = "DeferredResultMachine") -> hsm.Model:
    return hsm.Define(
        name,
        hsm.Initial(hsm.Target("holding")),
        hsm.State(
            "holding",
            hsm.Defer(hsm.Event(name="work")),
            hsm.Transition(hsm.On("release"), hsm.Target("../processing")),
        ),
        hsm.State(
            "processing",
            hsm.Transition(hsm.On("work"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )


def _string_deferred_model(name: str = "StringDeferredResultMachine") -> hsm.Model:
    return hsm.Define(
        name,
        hsm.Initial(hsm.Target("holding")),
        hsm.State(
            "holding",
            hsm.Defer("work"),
            hsm.Transition(hsm.On("release"), hsm.Target("../processing")),
        ),
        hsm.State(
            "processing",
            hsm.Transition(hsm.On("work"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )


def test_set_result_constants_are_not_public():
    assert not hasattr(hsm, "QueueFull")
    assert not hasattr(hsm, "Processed")
    assert not hasattr(hsm, "queue_full")
    assert not hasattr(hsm, "processed")
    assert {"QueueFull", "Processed", "queue_full", "processed"}.isdisjoint(hsm.__all__)
    assert "Deferred" not in hsm.__all__
    assert not hasattr(hsm, "Deferred")


def test_top_level_runtime_helpers_are_not_coroutine_functions():
    assert not inspect.iscoroutinefunction(hsm.Start)
    assert not inspect.iscoroutinefunction(hsm.Started)
    assert not inspect.iscoroutinefunction(hsm.Stop)
    assert not inspect.iscoroutinefunction(hsm.Restart)
    assert not inspect.iscoroutinefunction(hsm.Dispatch)
    assert not inspect.iscoroutinefunction(hsm.DispatchAll)
    assert not inspect.iscoroutinefunction(hsm.DispatchTo)
    assert not inspect.iscoroutinefunction(hsm.Set)
    assert not inspect.iscoroutinefunction(hsm.Call)


@pytest.mark.asyncio
async def test_top_level_dispatch_resolves_none_on_success():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _result_model())

    result = await hsm.Dispatch(ctx, instance, hsm.Event(name="go"))

    assert result is None
    assert instance.state() == "/ResultMachine/done"

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_top_level_dispatch_returns_machine_completion_handle():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _result_model())

    completion = hsm.Dispatch(ctx, instance, hsm.Event(name="go"))

    assert isinstance(completion, asyncio.Future)
    assert await completion is None
    assert instance.state() == "/ResultMachine/done"

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_cancelling_dispatch_completion_does_not_cancel_submitted_event():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _async_result_model())

    completion = instance.dispatch(instance.context(), hsm.Event(name="go"))
    completion.cancel()
    with pytest.raises(asyncio.CancelledError):
        await completion

    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert instance.state() == "/AsyncResultMachine/done"

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_top_level_dispatch_resolves_none_when_submitted_event_is_deferred():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _deferred_model())

    result = await hsm.Dispatch(ctx, instance, hsm.Event(name="work"))

    assert result is None
    assert instance.state() == "/DeferredResultMachine/holding"
    assert instance.take_snapshot().QueueLen == 1

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_defer_accepts_string_event_names():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _string_deferred_model())

    result = await hsm.Dispatch(ctx, instance, hsm.Event(name="work"))

    assert result is None
    assert instance.state() == "/StringDeferredResultMachine/holding"
    assert instance.take_snapshot().QueueLen == 1

    release_result = await hsm.Dispatch(ctx, instance, hsm.Event(name="release"))

    assert release_result is None
    assert instance.state() == "/StringDeferredResultMachine/done"

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_deferred_replay_dispatch_resolves_none_for_releasing_event():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _deferred_model())

    assert await hsm.Dispatch(ctx, instance, hsm.Event(name="work")) is None
    result = await hsm.Dispatch(ctx, instance, hsm.Event(name="release"))

    assert result is None
    assert instance.state() == "/DeferredResultMachine/done"
    assert instance.take_snapshot().QueueLen == 0

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_instance_dispatch_awaitable_resolves_none_for_deferred_event():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _deferred_model())

    result = await instance.dispatch(instance.context(), hsm.Event(name="work"))

    assert result is None
    assert instance.state() == "/DeferredResultMachine/holding"

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_group_dispatch_resolves_none_when_any_member_defers():
    deferred_instance = ResultInstance()
    processed_instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, deferred_instance, _deferred_model("DeferredGroupMember"))
    await hsm.Started(ctx, processed_instance, _result_model())
    group = hsm.Group(deferred_instance, processed_instance)

    result = await hsm.Dispatch(ctx, group, hsm.Event(name="work"))

    assert result is None
    assert deferred_instance.state() == "/DeferredGroupMember/holding"
    assert processed_instance.state() == "/ResultMachine/idle"

    await group.stop(group.context())


@pytest.mark.asyncio
async def test_dispatch_all_resolves_none_for_all_members():
    deferred_instance = ResultInstance()
    processed_instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, deferred_instance, _deferred_model("DeferredBroadcastMember"))
    await hsm.Started(ctx, processed_instance, _result_model())

    result = await hsm.DispatchAll(ctx, hsm.Event(name="work"))

    assert result is None
    assert deferred_instance.state() == "/DeferredBroadcastMember/holding"
    assert processed_instance.state() == "/ResultMachine/idle"

    release_result = await hsm.DispatchAll(ctx, hsm.Event(name="release"))

    assert release_result is None
    assert deferred_instance.state() == "/DeferredBroadcastMember/done"

    await hsm.Group(deferred_instance, processed_instance).stop(ctx)


@pytest.mark.asyncio
async def test_dispatch_to_resolves_none_for_selected_ids():
    selected = ResultInstance()
    skipped = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, selected, _deferred_model("DeferredTargetMember"), hsm.Config(ID="target-1"))
    await hsm.Started(ctx, skipped, _result_model(), hsm.Config(ID="skip-1"))

    result = await hsm.DispatchTo(ctx, hsm.Event(name="work"), "target-*")

    assert result is None
    assert selected.state() == "/DeferredTargetMember/holding"
    assert skipped.state() == "/ResultMachine/idle"

    release_result = await hsm.DispatchTo(ctx, hsm.Event(name="release"), "target-*")

    assert release_result is None
    assert selected.state() == "/DeferredTargetMember/done"
    assert skipped.state() == "/ResultMachine/idle"

    await hsm.Group(selected, skipped).stop(ctx)


@pytest.mark.asyncio
async def test_dispatch_broadcast_helpers_resolve_none_for_no_recipients():
    ctx = hsm.Context()

    assert await hsm.DispatchAll(None, hsm.Event(name="noop")) is None
    assert await hsm.DispatchTo(None, hsm.Event(name="noop"), "missing-*") is None
    assert await hsm.DispatchAll(ctx, hsm.Event(name="noop")) is None
    assert await hsm.DispatchTo(ctx, hsm.Event(name="noop"), "missing-*") is None

    ctx.cancel()

    assert await hsm.DispatchAll(ctx, hsm.Event(name="noop")) is None
    assert await hsm.DispatchTo(ctx, hsm.Event(name="noop"), "missing-*") is None


@pytest.mark.asyncio
async def test_top_level_set_resolves_none_on_success():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _result_model())

    result = await hsm.Set(ctx, instance, "flag", True)

    assert result is None
    assert hsm.Get(ctx, instance, "flag") == (True, True)

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_top_level_set_raises_validation_error_for_unknown_attribute():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _result_model())

    with pytest.raises(hsm.ValidationError, match='missing attribute "missing"'):
        await hsm.Set(ctx, instance, "missing", True)

    assert hsm.Get(ctx, instance, "missing") == (None, False)

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_set_raises_validation_error_for_exact_default_type_mismatch():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _result_model())

    with pytest.raises(
        hsm.ValidationError,
        match='attribute "flag" requires value of type bool, got int',
    ):
        await hsm.Set(ctx, instance, "flag", 1)

    assert hsm.Get(ctx, instance, "flag") == (False, True)

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_instance_set_resolves_none_for_unchanged_attribute():
    instance = ResultInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, _result_model())

    result = await instance.Set("flag", False)

    assert result is None
    assert instance.Get("flag") == (False, True)

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_set_uses_explicit_attribute_type_metadata():
    instance = ResultInstance()
    model = hsm.Define(
        "TypedAttributeMachine",
        hsm.Attribute("count", int),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)

    assert await hsm.Set(ctx, instance, "count", 1) is None
    assert hsm.Get(ctx, instance, "count") == (1, True)
    with pytest.raises(
        hsm.ValidationError,
        match='attribute "count" requires value of type int, got bool',
    ):
        await hsm.Set(ctx, instance, "count", True)
    assert hsm.Get(ctx, instance, "count") == (1, True)

    await instance.stop(instance.context())


@pytest.mark.asyncio
async def test_set_resolves_after_on_set_reaction_completes():
    instance = ResultInstance()
    model = hsm.Define(
        "OnSetResultMachine",
        hsm.Attribute("flag", False),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.OnSet("flag"), hsm.Target("../changed")),
        ),
        hsm.State("changed"),
    )
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)

    result = await hsm.Set(ctx, instance, "flag", True)

    assert result is None
    assert instance.state() == "/OnSetResultMachine/changed"
    assert hsm.Get(ctx, instance, "flag") == (True, True)

    await instance.stop(instance.context())
