import asyncio
import os
from datetime import timedelta

import pytest

import hsm


pytestmark = pytest.mark.skipif(
    os.environ.get("HSM_SOAK") != "1",
    reason="set HSM_SOAK=1 to run long deterministic soak tests",
)


class SoakInstance(hsm.Instance):
    pass


async def _dispatch_soak(iterations: int) -> None:
    instance = SoakInstance()
    ctx = hsm.Context()
    model = hsm.Define(
        "DispatchSoak",
        hsm.Initial(hsm.Target("a")),
        hsm.State("a", hsm.Transition(hsm.On("flip"), hsm.Target("../b"))),
        hsm.State("b", hsm.Transition(hsm.On("flip"), hsm.Target("../a"))),
    )
    await hsm.Start(ctx, instance, model)

    for index in range(iterations):
        await hsm.Dispatch(ctx, instance, hsm.Event("flip"))
        expected = "b" if index % 2 == 0 else "a"
        assert instance.state() == f"/DispatchSoak/{expected}"
        if index % 100 == 0:
            assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0

    assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
    await hsm.Stop(instance)


def test_long_dispatch_soak() -> None:
    asyncio.run(_dispatch_soak(10_000))


async def _timer_restart_soak(iterations: int) -> None:
    class TimerInstance(hsm.Instance):
        pass

    sleeps: list[asyncio.Future[None]] = []
    cancelled = 0

    async def manual_sleep(duration: timedelta) -> None:
        nonlocal cancelled
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        sleeps.append(future)
        try:
            await future
        except asyncio.CancelledError:
            cancelled += 1
            raise

    async def delay(ctx, inst, event) -> timedelta:
        return timedelta(seconds=1)

    model = hsm.Define(
        "TimerSoak",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State("waiting", hsm.Transition(hsm.After(delay), hsm.Target("../done"))),
        hsm.State("done"),
    )
    instance = TimerInstance()
    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model, hsm.Config(Clock=hsm.Clock(sleep=manual_sleep)))

    for _ in range(iterations):
        for _ in range(20):
            if sleeps:
                break
            await asyncio.sleep(0)
        old_sleep = sleeps.pop(0)
        await hsm.Restart(instance)
        assert instance.state() == "/TimerSoak/waiting"
        assert hsm.TakeSnapshot(ctx, instance).QueueLen == 0
        if not old_sleep.done():
            old_sleep.set_result(None)

    assert cancelled >= iterations
    await hsm.Stop(instance)


def test_timer_restart_soak() -> None:
    asyncio.run(_timer_restart_soak(500))
