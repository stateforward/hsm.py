#!/usr/bin/env python
"""
Benchmark for HSM state transitions in Go-style format
"""

import hsm
import time
import asyncio
import gc
import os
import psutil
import tracemalloc
import yappi


class THSM(hsm.Instance):
    pass


async def noBehavior(sm: THSM, event: hsm.Event) -> None:
    pass


def print_go_style_benchmark(
    name: str,
    iterations: int,
    time_per_op_ns: float,
    bytes_per_op: float,
    allocs_per_op: float,
) -> None:
    """Print benchmark results in Go-style format"""
    print(
        f"{name}\t{iterations}\t{time_per_op_ns:.1f} ns/op\t{bytes_per_op:.0f} B/op\t{allocs_per_op:.0f} allocs/op"
    )


foo_event = hsm.Event(name="foo")
bar_event = hsm.Event(name="bar")


model = hsm.define(
    "TestHSM",
    hsm.state("foo", hsm.entry(noBehavior), hsm.exit(noBehavior)),
    hsm.state("bar", hsm.entry(noBehavior), hsm.exit(noBehavior)),
    hsm.transition(
        hsm.on(foo_event),
        hsm.source("foo"),
        hsm.target("bar"),
        hsm.effect(noBehavior),
    ),
    hsm.transition(
        hsm.on(bar_event),
        hsm.source("bar"),
        hsm.target("foo"),
        hsm.effect(noBehavior),
    ),
    hsm.initial(hsm.target("foo"), hsm.effect(noBehavior)),
)


async def run_benchmark():
    # Setup benchmark

    # Determine CPU count for naming convention
    cpu_count = os.cpu_count() or 8

    # Auto-determine iteration count (similar to Go's benchmarking approach)
    # Start with a small number and increase until we get stable timing
    iterations = 10000
    min_duration = 1.0  # Target minimum duration in seconds

    # Warmup
    sm = THSM()
    await hsm.start(sm, model)
    for _ in range(1000):
        await sm.dispatch(foo_event)
        await sm.dispatch(bar_event)

    # Find appropriate iteration count
    while True:
        sm = THSM()
        await hsm.start(sm, model)

        start = time.time()
        for _ in range(iterations):
            await sm.dispatch(foo_event)
            await sm.dispatch(bar_event)
        duration = time.time() - start

        if duration >= min_duration:
            break

        iterations *= 2
    print("iterations", iterations)
    # Actual benchmark
    sm = THSM()
    await hsm.start(sm, model)

    # Force garbage collection before measurement
    gc.collect()

    # Start memory tracking
    tracemalloc.start()
    start_snapshot = tracemalloc.take_snapshot()

    # Record start memory state
    process = psutil.Process(os.getpid())
    start_memory = process.memory_info().rss

    # Start timing
    start_time = time.time()

    # Run iterations
    # yappi.start()
    for _ in range(iterations):
        await sm.dispatch(foo_event)
        await sm.dispatch(bar_event)
    # yappi.stop()
    # End timing
    end_time = time.time()
    # stats = yappi.get_func_stats()
    # stats.save("yappi.prof", type="pstat")
    # Record end memory state
    end_memory = process.memory_info().rss

    # Calculate memory usage from tracemalloc
    end_snapshot = tracemalloc.take_snapshot()
    tracemalloc.stop()

    memory_diff = end_snapshot.compare_to(start_snapshot, "lineno")
    total_bytes = sum(stat.size_diff for stat in memory_diff if stat.size_diff > 0)
    total_allocs = len([stat for stat in memory_diff if stat.size_diff > 0])

    # Calculate stats
    total_time_ns = (end_time - start_time) * 1e9  # Convert to nanoseconds
    time_per_op_ns = total_time_ns / (iterations * 2)  # Two dispatches per iteration
    bytes_per_op = total_bytes / (iterations * 2) if iterations > 0 else 0
    if bytes_per_op == 0:
        # Fallback to psutil measured memory
        bytes_per_op = (end_memory - start_memory) / (iterations * 2)
    allocs_per_op = total_allocs / (iterations * 2) if iterations > 0 else 0

    # Print in Go format
    print_go_style_benchmark(
        f"BenchmarkHSM-{cpu_count}",
        iterations,
        time_per_op_ns,
        bytes_per_op,
        allocs_per_op,
    )


if __name__ == "__main__":
    asyncio.run(run_benchmark())
