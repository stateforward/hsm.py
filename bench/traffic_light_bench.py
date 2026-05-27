#!/usr/bin/env python3
import hsm
import time
import asyncio
import os
import json
import resource
import sys
import inspect

TimerEvent = hsm.Event("TimerEvent")
CarArrival = hsm.Event("CarArrival")
MaintenanceSwitch = hsm.Event("MaintenanceSwitch")
PedestrianButton = hsm.Event("PedestrianButton")
Tick = hsm.Event("Tick")

WARMUP_MS = max(1, int(os.environ.get("HSM_BENCH_WARMUP_MS", "250")))
DURATION_MS = max(1, int(os.environ.get("HSM_BENCH_DURATION_MS", "2000")))
VALIDATE = os.environ.get("HSM_BENCH_VALIDATE", "0") not in ("", "0", "false", "False")
TARGET_BATCH_MS = 10.0

class TrafficLight(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.maintenance_mode = False
        self.cars_waiting = 0
        self.timer = 0

    @staticmethod
    def reset_cars(ctx, inst, event):
        inst.cars_waiting = 0

    @staticmethod
    def add_car(ctx, inst, event):
        inst.cars_waiting += 1

    @staticmethod
    def no_cars_waiting(ctx, inst, event):
        return inst.cars_waiting == 0

    @staticmethod
    def is_maintenance(ctx, inst, event):
        return inst.maintenance_mode == True

    @staticmethod
    def is_not_maintenance(ctx, inst, event):
        return inst.maintenance_mode == False

    @staticmethod
    def check_cars_for_choice(ctx, inst, event):
        return inst.cars_waiting > 10

    @staticmethod
    def set_timer_extended(ctx, inst, event):
        inst.timer = 60

    @staticmethod
    def set_timer_standard(ctx, inst, event):
        inst.timer = 40

    model = hsm.define("TrafficLight",
        hsm.initial(hsm.target("operational")),
        
        hsm.state("operational",
            hsm.transition(
                hsm.on(MaintenanceSwitch),
                hsm.guard(is_maintenance),
                hsm.target("../maintenance")
            ),
            hsm.initial(hsm.target("red")),

            hsm.state("red",
                hsm.transition(
                    hsm.on(TimerEvent),
                    hsm.guard(check_cars_for_choice),
                    hsm.effect(set_timer_extended),
                    hsm.target("../green")
                ),
                hsm.transition(
                    hsm.on(TimerEvent),
                    hsm.effect(set_timer_standard),
                    hsm.target("../green")
                ),
                hsm.transition(
                    hsm.on(CarArrival),
                    hsm.effect(add_car)
                )
            ),

            hsm.state("green",
                hsm.transition(
                    hsm.on(TimerEvent),
                    hsm.target("../yellow")
                ),
                hsm.transition(
                    hsm.on(PedestrianButton),
                    hsm.guard(no_cars_waiting),
                    hsm.target("../yellow")
                )
            ),

            hsm.state("yellow",
                hsm.defer(CarArrival),
                hsm.transition(
                    hsm.on(TimerEvent),
                    hsm.target("../red")
                )
            )
        ),

        hsm.state("maintenance",
            hsm.entry(reset_cars),
            hsm.transition(
                hsm.on(Tick),
                # Effect lambda in Python needs to be async or we can just ignore effect here 
                # since we mainly want to test throughput. But let's add a sync effect if possible or async wrapper.
            ),
            hsm.transition(
                hsm.on(MaintenanceSwitch),
                hsm.guard(is_not_maintenance),
                hsm.target("../operational")
            )
        )
    )

def assert_traffic_light(light, state, cars_waiting, timer, step):
    actual_state = light.state()
    if actual_state != state:
        raise AssertionError(f"{step}: state {actual_state!r}, expected {state!r}")
    if light.cars_waiting != cars_waiting:
        raise AssertionError(f"{step}: cars_waiting {light.cars_waiting}, expected {cars_waiting}")
    if light.timer != timer:
        raise AssertionError(f"{step}: timer {light.timer}, expected {timer}")

async def validate_traffic_light():
    light = TrafficLight()
    await hsm.start(None, light, TrafficLight.model)
    assert_traffic_light(light, "/TrafficLight/operational/red", 0, 0, "initial")

    completion = light.dispatch(CarArrival)
    if not inspect.isawaitable(completion):
        raise AssertionError("dispatch did not return an awaitable completion")
    await completion
    assert_traffic_light(light, "/TrafficLight/operational/red", 1, 0, "after CarArrival")

    await light.dispatch(TimerEvent)
    assert_traffic_light(light, "/TrafficLight/operational/green", 1, 40, "after first TimerEvent")

    await light.dispatch(TimerEvent)
    assert_traffic_light(light, "/TrafficLight/operational/yellow", 1, 40, "after second TimerEvent")

    await light.dispatch(TimerEvent)
    assert_traffic_light(light, "/TrafficLight/operational/red", 1, 40, "after third TimerEvent")

    await light.stop()

async def run_benchmark():
    async def dispatch_batch(light, cycles):
        for _ in range(cycles):
            await light.dispatch(CarArrival)
            await light.dispatch(TimerEvent)
            await light.dispatch(TimerEvent)
            await light.dispatch(TimerEvent)

    async def calibrate_batch(light):
        cycles = 1
        while True:
            start_time = time.perf_counter()
            await dispatch_batch(light, cycles)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            if elapsed_ms >= TARGET_BATCH_MS or cycles >= (1 << 20):
                return cycles
            cycles *= 2

    async def run_for(light, duration_ms, batch_cycles):
        start_time = time.perf_counter()
        deadline = start_time + (duration_ms / 1000)
        cycles = 0
        while time.perf_counter() < deadline:
            await dispatch_batch(light, batch_cycles)
            cycles += batch_cycles
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        return cycles, elapsed_ms

    if VALIDATE:
        await validate_traffic_light()

    warmup_light = TrafficLight()
    await hsm.start(None, warmup_light, TrafficLight.model)
    batch_cycles = await calibrate_batch(warmup_light)
    await run_for(warmup_light, WARMUP_MS, batch_cycles)
    await warmup_light.stop()

    light_bench = TrafficLight()
    await hsm.start(None, light_bench, TrafficLight.model)
    completed_cycles, duration_ms = await run_for(light_bench, DURATION_MS, batch_cycles)
    await light_bench.stop()

    duration_s = duration_ms / 1000
    total_dispatches = completed_cycles * 4
    
    ops_per_sec = int(total_dispatches / duration_s) if duration_s > 0 else 0
    
    usage = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == "darwin":
        memory_mb = usage.ru_maxrss / (1024 * 1024)
    else:
        memory_mb = usage.ru_maxrss / 1024
    
    print(json.dumps({
        "language": "Python",
        "iterations": total_dispatches,
        "duration_ms": round(duration_ms),
        "memory_mb": round(memory_mb, 2),
        "throughput_ops_per_sec": ops_per_sec
    }))

if __name__ == "__main__":
    asyncio.run(run_benchmark())
