"""
Test entry and exit behaviors
Tests entry and exit behavior execution, ordering, and error handling
"""

import pytest
import asyncio
import hsm


@pytest.mark.asyncio
async def test_entry_behavior():
    class EntryInstance(hsm.Instance):
        def __init__(self):
            super().__init__()
            self.log: list[str] = []

    def entry_behavior(ctx: hsm.Context, inst: EntryInstance, event: hsm.Event):
        inst.log.append("entry_behavior")

    def exit_behavior(ctx: hsm.Context, inst: EntryInstance, event: hsm.Event):
        inst.log.append("exit_behavior")

    sm = hsm.define(
        "root",
        hsm.state("s1", hsm.entry(entry_behavior), hsm.exit(exit_behavior)),
        hsm.state("s2"),
        hsm.transition(hsm.source("s1"), hsm.on("go"), hsm.target("s2")),
        hsm.initial(hsm.target("s1")),
    )
    instance = await hsm.start(None, hsm.New(EntryInstance(), sm))
    await hsm.dispatch(None, instance, hsm.Event(name="go"))

    assert instance.log == ["entry_behavior", "exit_behavior"]
