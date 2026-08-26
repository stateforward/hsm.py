import asyncio
import types

import hsm


def _runtime_context() -> hsm.Context:
    return hsm.hsm.context.new_context().WithValue(hsm.Keys.Instances, {})


def _machine() -> hsm.HSM[hsm.Instance]:
    model = hsm.Define(
        "RestartRace",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )
    return hsm.HSM(hsm.Instance(), model)


async def _restart_starts_after_stop_wins() -> None:
    sm = _machine()
    ctx = _runtime_context()
    starts: list[object] = []

    async def stop(self, stop_ctx):
        assert stop_ctx is ctx

    async def start(self, start_ctx, data=None):
        assert start_ctx is ctx
        starts.append(data)
        return self

    sm._stop = types.MethodType(stop, sm)
    sm._start = types.MethodType(start, sm)

    await sm._restart(ctx, "again")

    assert starts == ["again"]


def test_restart_starts_after_stop_wins():
    asyncio.run(_restart_starts_after_stop_wins())


async def _restart_cancels_stop_when_context_wins() -> None:
    sm = _machine()
    ctx = _runtime_context()
    stop_started = asyncio.Event()
    stop_cancelled = asyncio.Event()
    starts: list[object] = []

    async def stop(self, stop_ctx):
        assert stop_ctx is ctx
        stop_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            stop_cancelled.set()
            raise

    async def start(self, start_ctx, data=None):
        starts.append(data)
        return self

    sm._stop = types.MethodType(stop, sm)
    sm._start = types.MethodType(start, sm)

    restart_task = asyncio.create_task(sm._restart(ctx, "cancelled"))
    await asyncio.wait_for(stop_started.wait(), timeout=1)

    ctx.cancel()

    await asyncio.wait_for(restart_task, timeout=1)

    assert stop_cancelled.is_set()
    assert starts == []


def test_restart_cancels_stop_when_context_wins():
    asyncio.run(_restart_cancels_stop_when_context_wins())


async def _stop_removes_machine_from_shared_instance_registry() -> None:
    model = hsm.Define(
        "RegistryStop",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )
    ctx = _runtime_context()
    alpha = hsm.HSM(hsm.Instance(), model)
    bravo = hsm.HSM(hsm.Instance(), model)
    await alpha._start(ctx)
    await bravo._start(ctx)

    registry = ctx.value(hsm.Keys.Instances)
    assert registry == {alpha.id: alpha._instance, bravo.id: bravo._instance}

    await alpha._stop(ctx)
    assert registry == {bravo.id: bravo._instance}

    await alpha._start(ctx)
    assert registry == {alpha.id: alpha._instance, bravo.id: bravo._instance}

    await alpha._stop(ctx)
    await bravo._stop(ctx)
    assert registry == {}


def test_stop_removes_machine_from_shared_instance_registry():
    asyncio.run(_stop_removes_machine_from_shared_instance_registry())
