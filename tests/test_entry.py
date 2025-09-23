"""
Test entry and exit behaviors
Tests entry and exit behavior execution, ordering, and error handling
"""

import pytest
import asyncio
import hsm


@pytest.mark.asyncio
async def test_entry_behavior():
    async def entry_behavior(ctx: hsm.Context, inst: hsm.Instance, event: hsm.Event):
        print("entry_behavior")
        return await inst.dispatch(hsm.Event(name="entry_behavior"))

    async def exit_behavior(ctx: hsm.Context, inst: hsm.Instance, event: hsm.Event):
        print("exit_behavior")
        inst.dispatch(hsm.Event(name="exit_behavior"))
        print("exit_behavior done")

    sm = hsm.define(
        "root",
        hsm.state("s1", hsm.entry(entry_behavior)),
        hsm.state("s2", hsm.exit(exit_behavior)),
        hsm.initial(hsm.target("s1")),
    )
    print("starting")
    print(await hsm.start(None, hsm.Instance(), sm))
    print("done")
