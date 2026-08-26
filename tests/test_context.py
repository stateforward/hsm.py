import asyncio
import importlib.util
import pathlib
import typing

import pytest

_CONTEXT_PATH = pathlib.Path(__file__).parents[1] / "hsm" / "context.py"
_SPEC = importlib.util.spec_from_file_location("hsm_context_under_test", _CONTEXT_PATH)
assert _SPEC is not None
assert _SPEC.loader is not None
context = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(context)


Context = typing.cast(typing.Callable[[], typing.Any], context.Context)
with_cancel = typing.cast(
    typing.Callable[[typing.Any], tuple[typing.Any, typing.Callable[[], None]]],
    context.with_cancel,
)
with_value = typing.cast(
    typing.Callable[[typing.Any, typing.Hashable, object], typing.Any],
    context.with_value,
)


@pytest.mark.asyncio
async def test_parent_cancel_cancels_child_context_and_waiters():
    parent = Context()
    child, _ = with_cancel(parent)

    waiter = asyncio.wrap_future(child.Done())

    parent.cancel()

    await asyncio.wait_for(waiter, timeout=0.1)
    assert child.Done().done()
    assert child.is_done()


@pytest.mark.asyncio
async def test_done_returns_shared_completion_future():
    ctx = Context()
    done = ctx.Done()

    assert ctx.done() is done
    assert done is ctx.Done()
    assert not done.done()

    waiter = asyncio.wrap_future(done)
    ctx.cancel()

    await asyncio.wait_for(waiter, timeout=0.1)
    assert done.done()


def test_child_cancel_does_not_cancel_parent_context():
    parent = Context()
    child, cancel_child = with_cancel(parent)

    cancel_child()

    assert child.Done().done()
    assert not parent.Done().done()


def test_child_context_reads_and_shadows_parent_values():
    parent_key = object()
    child_key = object()
    parent = with_value(Context(), parent_key, "parent")
    child = with_value(parent, child_key, "child")
    shadowed = with_value(child, parent_key, "shadowed")

    assert child.Value(parent_key) == "parent"
    assert child.Value(child_key) == "child"
    assert shadowed.Value(parent_key) == "shadowed"
    assert shadowed.Value(object()) is None


def test_subcontext_is_a_path_view_over_flattened_values():
    root = Context().WithPathValue("/root/state", "ready")
    root = root.WithPathValue("/root/child/count", 3)
    root = root.WithPathValue("/root/child/grandchild/value", 4)

    view = root.Subcontext("/root")
    child = view.Subcontext("child")

    assert view.paths == ("/root/state", "/root/child")
    assert child.paths == ("/root/child/count", "/root/child/grandchild")
    assert child.Value("count") == 3
    assert view.Value("child/grandchild/value") == 4
    assert child.Value("/root/state") == "ready"
    assert child.Value("missing") is None


def test_subcontext_with_value_preserves_view_paths_and_snapshot_semantics():
    root = Context().WithPathValue("/root/child/value", "old")
    view = root.Subcontext("/root").Subcontext("child")
    updated = view.WithValue("value", "new")

    assert view.Value("value") == "old"
    assert updated.Value("value") == "new"
    assert view.paths == ("/root/child/value",)
    assert updated.paths == view.paths
