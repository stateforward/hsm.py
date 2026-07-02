import asyncio
import collections.abc
import concurrent.futures
from datetime import timedelta
import typing

import pytest

import hsm
import hsm.hsm as core


def test_package_import_exports_public_api() -> None:
    assert all(hasattr(hsm, name) for name in hsm.__all__)
    assert "Define" in hsm.__all__
    assert hsm.Define is not None
    assert hsm.Config is not None


def test_context_export_is_protocol_not_default_implementation() -> None:
    context_factory = typing.cast(typing.Callable[[], object], hsm.Context)

    with pytest.raises(TypeError, match="Protocols cannot be instantiated"):
        context_factory()

    ctx = core.context.new_context()
    assert isinstance(ctx, hsm.Context)
    assert type(ctx).__name__ == "_Context"
    assert "_Context" not in hsm.__all__
    assert not hasattr(hsm, "_Context")


def test_custom_context_satisfies_public_protocol() -> None:
    class CustomContext:
        def __init__(
            self,
            parent: hsm.Context | None = None,
            values: collections.abc.Mapping[typing.Hashable, object] | None = None,
        ) -> None:
            self._done = concurrent.futures.Future[None]()
            self._parent = parent
            self._values = dict(values or {})

        def is_done(self) -> bool:
            return self._done.done()

        def Deadline(self) -> tuple[None, bool]:
            return None, False

        deadline = Deadline

        def Err(self) -> BaseException | None:
            if self._done.done():
                return RuntimeError("context canceled")
            return None

        err = Err

        def cancel(self) -> None:
            try:
                self._done.set_result(None)
            except concurrent.futures.InvalidStateError:
                pass

        def Done(self) -> concurrent.futures.Future[None]:
            return self._done

        done = Done

        def Value(self, key: typing.Hashable) -> object | None:
            if key in self._values:
                return self._values[key]
            if self._parent is not None:
                return self._parent.Value(key)
            return None

        value = Value

        def WithValue(self, key: typing.Hashable, value: object) -> hsm.Context:
            return CustomContext(self, {key: value})

        with_value = WithValue

        def WithCancel(self) -> tuple[hsm.Context, typing.Callable[[], None]]:
            child = CustomContext(self)
            return child, child.cancel

        with_cancel = WithCancel

    class CustomContextInstance(hsm.Instance):
        def __init__(self) -> None:
            super().__init__()
            self.log: list[object] = []

    def enter_idle(
        ctx: hsm.Context, instance: CustomContextInstance, event: hsm.Event
    ) -> None:
        del event
        instance.log.append(ctx.Value("request"))

    ctx = CustomContext().WithValue("request", "custom")
    instance = CustomContextInstance()
    model = hsm.Define(
        "CustomContextMachine",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Entry(enter_idle)),
    )

    async def run() -> None:
        await hsm.Started(ctx, instance, model)
        await hsm.Stop(instance)

    assert isinstance(ctx, hsm.Context)

    asyncio.run(run())

    assert instance.log == ["custom"]


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
