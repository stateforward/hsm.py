"""
Benchmark for HSM state transitions
"""

import hsm
import time
import pytest
from typing import Any, Tuple, Callable
# Import memory_profiler with type ignore since it doesn't have stub files
import memory_profiler  # type: ignore

class THSM(hsm.Instance):
    pass

async def noBehavior(sm: THSM, event: hsm.Event) -> None:
    pass

@pytest.fixture  # type: ignore
def hsm_setup() -> Tuple[Any, hsm.Event, hsm.Event]:
    """Setup HSM instance with the test model"""
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
    
    return model, foo_event, bar_event

def test_hsm_benchmark(benchmark: Callable[..., Any], hsm_setup: Tuple[Any, hsm.Event, hsm.Event]) -> None:
    """Benchmark HSM transitions"""
    import asyncio
    
    model, foo_event, bar_event = hsm_setup
    
    async def run_single_benchmark():
        sm = THSM()
        await hsm.start(sm, model)
        await sm.dispatch(foo_event)
        await sm.dispatch(bar_event)
        return sm
    
    # Run the benchmark
    benchmark(lambda: asyncio.run(run_single_benchmark()))

# Format benchmark results to match Go's benchmark output format
def print_go_style_benchmark(name: str, iterations: int, time_per_op_ns: float, 
                             bytes_per_op: float, allocs_per_op: float) -> None:
    """Print benchmark results in Go-style format"""
    print(f"{name}\t{iterations}\t{time_per_op_ns:.1f} ns/op\t{bytes_per_op:.0f} B/op\t{allocs_per_op:.0f} allocs/op")

if __name__ == "__main__":
    # Run custom benchmark with memory profiling to match Go's output format
    import asyncio
    import gc
    import tracemalloc
    
    async def run_benchmark_iterations(iterations: int) -> Tuple[float, float, float]:
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
        sm = THSM()
        await hsm.start(sm, model)
        
        # Force garbage collection before measurement
        gc.collect()
        
        # Start memory tracking
        tracemalloc.start()
        start_snapshot = tracemalloc.take_snapshot()
        
        # Start timing
        start_time = time.time()
        
        # Run iterations
        for _ in range(iterations):
            await sm.dispatch(foo_event)
            await sm.dispatch(bar_event)
        
        # End timing
        end_time = time.time()
        
        # Calculate memory usage
        end_snapshot = tracemalloc.take_snapshot()
        tracemalloc.stop()
        
        memory_diff = end_snapshot.compare_to(start_snapshot, 'lineno')
        total_bytes = sum(stat.size_diff for stat in memory_diff if stat.size_diff > 0)
        total_allocs = len([stat for stat in memory_diff if stat.size_diff > 0])
        
        # Calculate stats
        total_time_ns = (end_time - start_time) * 1e9  # Convert to nanoseconds
        time_per_op_ns = total_time_ns / (iterations * 2)  # Two dispatches per iteration
        bytes_per_op = total_bytes / (iterations * 2) if iterations > 0 else 0
        allocs_per_op = total_allocs / (iterations * 2) if iterations > 0 else 0
        
        return time_per_op_ns, bytes_per_op, allocs_per_op
    
    async def main():
        # Run with increasing iterations to get stable measurements
        iterations = 1000000  # Adjust as needed for your machine
        cpu_count = 8  # Replace with actual CPU count or detection
        
        benchmark_results = await run_benchmark_iterations(iterations)
        time_per_op, bytes_per_op, allocs_per_op = benchmark_results
        
        # Print in Go format
        print_go_style_benchmark(f"BenchmarkHSM-{cpu_count}", iterations, 
                                time_per_op, bytes_per_op, allocs_per_op)

    asyncio.run(main())