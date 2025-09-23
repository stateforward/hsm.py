import hsm
import asyncio

start_event = hsm.Event(name="start")


class SM(hsm.Instance):
    pass


async def s2_entry(hsm: SM, event: hsm.Event):
    print("s2 entry")


async def s2_exit(hsm: SM, event: hsm.Event):
    print("s2 exit")

async def s1_entry(hsm: SM, event: hsm.Event):
    print("s1 entry")


model = hsm.define(
    "root",
    hsm.initial(hsm.target("s2")),
    hsm.state("s1", hsm.entry(s1_entry)),
    hsm.state(
        "s2",
        hsm.entry(s2_entry),
        hsm.exit(s2_exit),
        hsm.transition(hsm.on(start_event), hsm.target("../s1")),
    ),
)


async def main():
    base = SM(id="test")
    sm = await hsm.start(base, model)
    print(sm.state())
    await sm.dispatch(start_event)
    print(sm.state())


asyncio.run(main())
