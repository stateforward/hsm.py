#!/usr/bin/env python3
"""Throughput benchmark for the current asyncio HSM runtime."""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import time
import typing

import hsm


class BenchInstance(hsm.Instance):
    def __init__(self) -> None:
        super().__init__()
        self.flip = False


async def alternating_guard(
    ctx: hsm.Context, instance: BenchInstance, event: hsm.Event
) -> bool:
    instance.flip = not instance.flip
    return instance.flip


def build_ping_pong() -> tuple[hsm.Model, list[hsm.Event]]:
    model = hsm.Define(
        "PingPong",
        hsm.Initial(hsm.Target("a")),
        hsm.State("a", hsm.Transition(hsm.On("ping"), hsm.Target("../b"))),
        hsm.State("b", hsm.Transition(hsm.On("pong"), hsm.Target("../a"))),
    )
    return model, [hsm.Event(name="ping"), hsm.Event(name="pong")]


def build_hierarchical() -> tuple[hsm.Model, list[hsm.Event]]:
    model = hsm.Define(
        "Hierarchical",
        hsm.Initial(hsm.Target("parent")),
        hsm.State(
            "parent",
            hsm.Initial(hsm.Target("left")),
            hsm.State("left", hsm.Transition(hsm.On("next"), hsm.Target("../right"))),
            hsm.State("right", hsm.Transition(hsm.On("prev"), hsm.Target("../left"))),
        ),
    )
    return model, [hsm.Event(name="next"), hsm.Event(name="prev")]


def build_guarded_self() -> tuple[hsm.Model, list[hsm.Event]]:
    model = hsm.Define(
        "GuardedSelf",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("tick"), hsm.Guard(alternating_guard), hsm.Target(".")),
            hsm.Transition(hsm.On("tick"), hsm.Target(".")),
        ),
    )
    return model, [hsm.Event(name="tick")]


def build_traffic_ring() -> tuple[hsm.Model, list[hsm.Event]]:
    model = hsm.Define(
        "TrafficRing",
        hsm.Initial(hsm.Target("ns_green")),
        hsm.State(
            "ns_green",
            hsm.Transition(hsm.On("tick"), hsm.Target("../ns_yellow")),
        ),
        hsm.State(
            "ns_yellow",
            hsm.Transition(hsm.On("tick"), hsm.Target("../all_red_1")),
        ),
        hsm.State(
            "all_red_1",
            hsm.Transition(hsm.On("tick"), hsm.Target("../ew_green")),
        ),
        hsm.State(
            "ew_green",
            hsm.Transition(hsm.On("tick"), hsm.Target("../ew_yellow")),
        ),
        hsm.State(
            "ew_yellow",
            hsm.Transition(hsm.On("tick"), hsm.Target("../all_red_2")),
        ),
        hsm.State(
            "all_red_2",
            hsm.Transition(hsm.On("tick"), hsm.Target("../ns_green")),
        ),
    )
    return model, [hsm.Event(name="tick")]


SCENARIOS: dict[
    str, tuple[int, typing.Callable[[], tuple[hsm.Model, list[hsm.Event]]]]
] = {
    "ping_pong": (20_000, build_ping_pong),
    "hierarchical": (20_000, build_hierarchical),
    "guarded_self": (40_000, build_guarded_self),
    "traffic_ring": (60_000, build_traffic_ring),
}


async def benchmark_case(
    name: str,
    model: hsm.Model,
    events: list[hsm.Event],
    iterations: int,
    rounds: int,
    warmup: int,
) -> dict[str, float]:
    timings: list[float] = []
    startup: list[float] = []
    ops = iterations * len(events)

    for _ in range(rounds):
        instance = BenchInstance()
        ctx = hsm.Context()

        start_ns = time.perf_counter_ns()
        machine = await hsm.Start(ctx, instance, model)
        startup.append(time.perf_counter_ns() - start_ns)

        for _ in range(warmup):
            for event in events:
                await hsm.Dispatch(ctx, machine, event)

        start_ns = time.perf_counter_ns()
        for _ in range(iterations):
            for event in events:
                await hsm.Dispatch(ctx, machine, event)
        elapsed_ns = time.perf_counter_ns() - start_ns
        timings.append(elapsed_ns / ops)
        await hsm.Stop(machine)

    ordered = sorted(timings)
    p95_index = max(0, min(len(ordered) - 1, int(round(rounds * 0.95)) - 1))
    median_ns = statistics.median(ordered)
    startup_median_ns = statistics.median(startup)
    return {
        "ops": float(ops),
        "median_ns_per_op": median_ns,
        "p95_ns_per_op": ordered[p95_index],
        "ops_per_sec": 1e9 / median_ns,
        "startup_us": startup_median_ns / 1000.0,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        action="append",
        choices=sorted(SCENARIOS),
        help="Scenario to run. Repeat to run multiple scenarios. Defaults to all.",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=5,
        help="Measurement rounds per scenario.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1_000,
        help="Warmup loop count per round.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        help="Override the default iteration count for every selected scenario.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    selected = args.scenario or list(SCENARIOS)

    print(f"python={os.sys.version.split()[0]}|cpus={os.cpu_count()}")
    for name in selected:
        default_iterations, factory = SCENARIOS[name]
        model, events = factory()
        result = await benchmark_case(
            name=name,
            model=model,
            events=events,
            iterations=args.iterations or default_iterations,
            rounds=args.rounds,
            warmup=args.warmup,
        )
        print(
            f"{name}|ops={int(result['ops'])}"
            f"|median_ns_per_op={result['median_ns_per_op']:.1f}"
            f"|p95_ns_per_op={result['p95_ns_per_op']:.1f}"
            f"|ops_per_sec={result['ops_per_sec']:.0f}"
            f"|startup_us={result['startup_us']:.1f}"
        )


if __name__ == "__main__":
    asyncio.run(main())
