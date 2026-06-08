import asyncio

import pytest

import hsm


class ObservationInstance(hsm.Instance):
    def __init__(self) -> None:
        super().__init__()
        self.observed: list[tuple[str, str, str, str]] = []
        self.log: list[str] = []


def observe(ctx: hsm.Context, instance: ObservationInstance, event: hsm.Event) -> None:
    del ctx
    observed = event.data["event"]
    occurrence = event.data["occurrence"]
    instance.observed.append((event.name, event.source, occurrence, observed.name))


async def running(ctx: hsm.Context, instance: ObservationInstance, event: hsm.Event) -> None:
    del event
    instance.log.append("running")
    await asyncio.wrap_future(ctx.Done())


def effect(ctx: hsm.Context, instance: ObservationInstance, event: hsm.Event) -> None:
    del ctx, event
    instance.log.append("effect")


@pytest.mark.asyncio
async def test_observe_wraps_targeted_behavior_member() -> None:
    model = hsm.Define(
        "ObservedBehavior",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Activity(running),
            hsm.Transition(hsm.On("go"), hsm.Target("../done")),
        ),
        hsm.State("done"),
        hsm.Observe(observe, "/ObservedBehavior/idle/running"),
    )
    instance = ObservationInstance()

    await hsm.Started(hsm.Context(), instance, model)
    await hsm.Dispatch(instance.context(), instance, hsm.Event(name="go"))
    await instance.stop(instance.context())

    assert instance.observed == [
        (
            "hsm/observation",
            "/ObservedBehavior/idle/running",
            "behavior",
            "hsm/initial",
        ),
    ]
    assert instance.log == ["running"]


@pytest.mark.asyncio
async def test_observe_can_target_event_name() -> None:
    model = hsm.Define(
        "ObservedEvent",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../done"), hsm.Effect(effect)),
        ),
        hsm.State("done"),
        hsm.Observe(observe, "go"),
    )
    instance = ObservationInstance()

    await hsm.Started(hsm.Context(), instance, model)
    await hsm.Dispatch(instance.context(), instance, hsm.Event(name="go"))
    await instance.stop(instance.context())

    assert instance.observed == [
        ("hsm/observation", "/ObservedEvent/idle/transition_4", "event", "go"),
    ]
    assert instance.log == ["effect"]


@pytest.mark.asyncio
async def test_observe_can_redefine_finalized_model() -> None:
    model = hsm.Define(
        "ObservedLate",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../done"))),
        hsm.State("done"),
    )
    observed_model = model.redefine(model, (hsm.Observe(observe, "go"),))

    original = ObservationInstance()
    await hsm.Started(hsm.Context(), original, model)
    await hsm.Dispatch(original.context(), original, hsm.Event(name="go"))
    await original.stop(original.context())

    assert original.observed == []

    instance = ObservationInstance()
    assert isinstance(observed_model, hsm.Model)
    await hsm.Started(hsm.Context(), instance, observed_model)
    await hsm.Dispatch(instance.context(), instance, hsm.Event(name="go"))
    await instance.stop(instance.context())

    assert instance.observed == [
        ("hsm/observation", "/ObservedLate/idle/transition_4", "event", "go"),
    ]
