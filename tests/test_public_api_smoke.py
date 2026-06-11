import asyncio
from datetime import timedelta

import hsm


def test_package_import_exports_public_api() -> None:
    assert all(hasattr(hsm, name) for name in hsm.__all__)
    assert "Define" in hsm.__all__
    assert hsm.Define is not None
    assert hsm.Config is not None


def test_config_accepts_documented_pascal_case_options() -> None:
    clock = hsm.DefaultClock()
    queue = hsm.Fifo()

    config = hsm.Config(
        ID="machine-1",
        Name="RuntimeName",
        Data={"ok": True},
        Clock=clock,
        Queue=queue,
    )

    assert config.ID == "machine-1"
    assert config.Name == "RuntimeName"
    assert config.Data == {"ok": True}
    assert config.Clock is clock
    assert config.Queue is queue


def test_public_queue_types_preserve_fifo_order() -> None:
    fifo = hsm.Fifo()
    assert fifo.push(hsm.Event(name="first")) == (None,)
    assert fifo.push(hsm.Event(name="second")) == (None,)
    assert fifo.len() == (2, None)
    event, ok, error = fifo.pop()
    assert ok and error is None and event.name == "first"
    event, ok, error = fifo.pop()
    assert ok and error is None and event.name == "second"

    queue = hsm.Queue(fifo)
    assert isinstance(queue, hsm.MultiQueue)


def test_public_clock_sleep_alias() -> None:
    durations: list[timedelta] = []

    async def sleep(duration: timedelta) -> None:
        durations.append(duration)

    async def run() -> None:
        clock = hsm.Clock(Sleep=sleep)
        await clock.Sleep(timedelta(milliseconds=5))

    asyncio.run(run())

    assert durations == [timedelta(milliseconds=5)]


def test_define_populates_model_members() -> None:
    model = hsm.Define(
        "Smoke",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )

    assert model.qualified_name == "/Smoke"
    assert "/Smoke" in model.members
    assert "/Smoke/idle" in model.members
    assert "/Smoke/.initial" in model.members
    assert model.initial == "/Smoke/.initial"
