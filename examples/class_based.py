import hsm

class TrafficLight(hsm.Instance):

    @staticmethod
    async def green_entry(ctx, self, event):
        print("Green light")

    @staticmethod
    async def green_exit(ctx, self, event):
        print("Green light off")


    @staticmethod
    async def yellow_entry(ctx, self, event):
        print("Yellow light")

    @staticmethod
    async def yellow_exit(ctx, self, event):
        print("Yellow light off")


    @staticmethod
    async def red_entry(ctx, self, event):
        print("Red light")

    @staticmethod
    async def red_exit(ctx, self, event):
        print("Red light off")


    model = hsm.define(
        "TrafficLight",
        hsm.state("red",
            hsm.entry(red_entry),
            hsm.exit(red_exit),
            hsm.transition(hsm.on("timer_complete"), hsm.target("green")),
        ),
        hsm.state("green",
            hsm.entry(green_entry),
            hsm.exit(green_exit),
            hsm.transition(hsm.on("timer_complete"), hsm.target("yellow")),
        ),
        hsm.state("yellow",
            hsm.entry(yellow_entry),
            hsm.exit(yellow_exit),
            hsm.transition(hsm.on("timer_complete"), hsm.target("red")),
        ),
    )

    