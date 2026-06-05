import pytest

import hsm


class HistoryInstance(hsm.Instance):
    def __init__(self) -> None:
        super().__init__()
        self.log: list[str] = []


@pytest.mark.asyncio
async def test_shallow_history_restores_direct_child_default() -> None:
    instance = HistoryInstance()
    model = hsm.Define(
        "ShallowHistoryMachine",
        hsm.Initial(hsm.Target("parent")),
        hsm.State(
            "parent",
            hsm.ShallowHistory(
                "memory",
                hsm.Transition(hsm.Target("a")),
            ),
            hsm.Initial(hsm.Target("a")),
            hsm.State(
                "a",
                hsm.Transition(hsm.On("advance"), hsm.Target("../b/two")),
            ),
            hsm.State(
                "b",
                hsm.Initial(hsm.Target("one")),
                hsm.State("one"),
                hsm.State("two"),
                hsm.Transition(hsm.On("leave"), hsm.Target("../../outside")),
            ),
        ),
        hsm.State(
            "outside",
            hsm.Transition(hsm.On("resume"), hsm.Target("../parent/memory")),
        ),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)
    await hsm.Dispatch(ctx, instance, hsm.Event(name="advance"))
    assert instance.state() == "/ShallowHistoryMachine/parent/b/two"

    await hsm.Dispatch(ctx, instance, hsm.Event(name="leave"))
    assert instance.state() == "/ShallowHistoryMachine/outside"

    await hsm.Dispatch(ctx, instance, hsm.Event(name="resume"))
    assert instance.state() == "/ShallowHistoryMachine/parent/b/one"


@pytest.mark.asyncio
async def test_deep_history_restores_nested_leaf() -> None:
    instance = HistoryInstance()
    model = hsm.Define(
        "DeepHistoryMachine",
        hsm.Initial(hsm.Target("parent")),
        hsm.State(
            "parent",
            hsm.DeepHistory(
                "memory",
                hsm.Transition(hsm.Target("a")),
            ),
            hsm.Initial(hsm.Target("a")),
            hsm.State(
                "a",
                hsm.Transition(hsm.On("advance"), hsm.Target("../b/two")),
            ),
            hsm.State(
                "b",
                hsm.Initial(hsm.Target("one")),
                hsm.State("one"),
                hsm.State("two"),
                hsm.Transition(hsm.On("leave"), hsm.Target("../../outside")),
            ),
        ),
        hsm.State(
            "outside",
            hsm.Transition(hsm.On("resume"), hsm.Target("../parent/memory")),
        ),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)
    await hsm.Dispatch(ctx, instance, hsm.Event(name="advance"))
    assert instance.state() == "/DeepHistoryMachine/parent/b/two"

    await hsm.Dispatch(ctx, instance, hsm.Event(name="leave"))
    assert instance.state() == "/DeepHistoryMachine/outside"

    await hsm.Dispatch(ctx, instance, hsm.Event(name="resume"))
    assert instance.state() == "/DeepHistoryMachine/parent/b/two"


@pytest.mark.asyncio
async def test_history_uses_default_transition_when_empty() -> None:
    instance = HistoryInstance()

    def allow(ctx: hsm.Context, inst: HistoryInstance, event: hsm.Event) -> bool:
        del ctx, event
        inst.log.append("guard")
        return True

    def effect(ctx: hsm.Context, inst: HistoryInstance, event: hsm.Event) -> None:
        del ctx, event
        inst.log.append("effect")

    model = hsm.Define(
        "DefaultHistoryMachine",
        hsm.Initial(hsm.Target("outside")),
        hsm.State(
            "outside",
            hsm.Transition(hsm.On("enter"), hsm.Target("../parent/memory")),
        ),
        hsm.State(
            "parent",
            hsm.DeepHistory(
                "memory",
                hsm.Transition(
                    hsm.Guard(allow),
                    hsm.Target("child"),
                    hsm.Effect(effect),
                ),
            ),
            hsm.State("child"),
        ),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)
    await hsm.Dispatch(ctx, instance, hsm.Event(name="enter"))

    assert instance.state() == "/DefaultHistoryMachine/parent/child"
    assert instance.log == ["guard", "effect"]


@pytest.mark.asyncio
async def test_transition_to_deep_history_preserves_previous_snapshot() -> None:
    instance = HistoryInstance()

    def entry_a(ctx: hsm.Context, inst: HistoryInstance, event: hsm.Event) -> None:
        del ctx, event
        inst.log.append("entry:a")

    def entry_leaf(ctx: hsm.Context, inst: HistoryInstance, event: hsm.Event) -> None:
        del ctx, event
        inst.log.append("entry:leaf")

    model = hsm.Define(
        "DeepHistorySnapshotMachine",
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
            hsm.DeepHistory(
                "h",
                hsm.Transition(hsm.Target("a")),
            ),
            hsm.Transition(hsm.Source("a"), hsm.On("to_b"), hsm.Target("b/leaf")),
            hsm.Transition(hsm.Source("b"), hsm.On("to_a"), hsm.Target("a")),
            hsm.Transition(hsm.Source("a"), hsm.On("resume"), hsm.Target("h")),
        ),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)
    await hsm.Dispatch(ctx, instance, hsm.Event(name="to_b"))
    await hsm.Dispatch(ctx, instance, hsm.Event(name="to_a"))
    await hsm.Dispatch(ctx, instance, hsm.Event(name="resume"))

    assert instance.state() == "/DeepHistorySnapshotMachine/comp/b/leaf"
    assert instance.log == ["entry:a", "entry:leaf", "entry:a", "entry:leaf"]


@pytest.mark.asyncio
async def test_history_default_transition_does_not_record_history_vertex() -> None:
    instance = HistoryInstance()

    model = hsm.Define(
        "HistoryDefaultDoesNotRecordHistoryVertex",
        hsm.Initial(hsm.Target("outside")),
        hsm.State(
            "outside",
            hsm.Transition(hsm.On("enter"), hsm.Target("../comp/h")),
        ),
        hsm.State(
            "comp",
            hsm.DeepHistory(
                "h",
                hsm.Transition(hsm.Target("b")),
            ),
            hsm.State("b", hsm.Transition(hsm.On("again"), hsm.Target("../h"))),
        ),
    )

    ctx = hsm.Context()
    await hsm.Started(ctx, instance, model)
    await hsm.Dispatch(ctx, instance, hsm.Event(name="enter"))
    await hsm.Dispatch(ctx, instance, hsm.Event(name="again"))

    assert instance.state() == "/HistoryDefaultDoesNotRecordHistoryVertex/comp/b"
