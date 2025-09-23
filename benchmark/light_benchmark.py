"""
Simple on/off light state machine benchmark
"""

import hsm
import time
import asyncio
import tracemalloc
import gc
from typing import Tuple


class LightHSM(hsm.Instance):
    """Simple light HSM instance"""
    pass


async def no_behavior(sm: LightHSM, event: hsm.Event) -> None:
    """No-op behavior function"""
    pass


def create_light_model() -> hsm.Model:
    """Create a simple on/off light state machine model"""
    on_event = hsm.Event(name="on")
    off_event = hsm.Event(name="off")
    
    return hsm.define(
        "LightHSM",
        hsm.state("off"),
        hsm.state("on"),
        hsm.transition(
            hsm.on(on_event),
            hsm.source("off"),
            hsm.target("on")
        ),
        hsm.transition(
            hsm.on(off_event),
            hsm.source("on"),
            hsm.target("off")
        ),
        hsm.initial(hsm.target("off"))
    )


async def run_light_benchmark(iterations: int = 100000) -> Tuple[float, float, int]:
    """
    Run light on/off benchmark
    
    Returns:
        (transitions_per_second, memory_bytes_per_op, total_transitions)
    """
    model = create_light_model()
    sm = LightHSM()
    await hsm.start(sm, model)
    
    on_event = hsm.Event(name="on")
    off_event = hsm.Event(name="off")
    
    # Warmup
    for _ in range(1000):
        await sm.dispatch(on_event)
        await sm.dispatch(off_event)
    
    # Force garbage collection before measurement
    gc.collect()
    
    # Start memory tracking
    tracemalloc.start()
    start_snapshot = tracemalloc.take_snapshot()
    
    # Start timing
    start_time = time.time()
    
    # Run benchmark iterations
    for _ in range(iterations):
        await sm.dispatch(on_event)
        await sm.dispatch(off_event)
    
    # End timing
    end_time = time.time()
    
    # Calculate memory usage
    end_snapshot = tracemalloc.take_snapshot()
    tracemalloc.stop()
    
    memory_diff = end_snapshot.compare_to(start_snapshot, 'lineno')
    total_bytes = sum(stat.size_diff for stat in memory_diff if stat.size_diff > 0)
    
    # Calculate results
    total_time = end_time - start_time
    total_transitions = iterations * 2  # Two transitions per iteration
    transitions_per_second = total_transitions / total_time
    bytes_per_op = total_bytes / total_transitions if total_transitions > 0 else 0
    
    return transitions_per_second, bytes_per_op, total_transitions


async def main():
    """Run the light benchmark and print results"""
    print("Light State Machine Benchmark (Python)")
    print("=====================================")
    
    iterations = 100000
    trans_per_sec, bytes_per_op, total_trans = await run_light_benchmark(iterations)
    
    print(f"Iterations: {iterations}")
    print(f"Total transitions: {total_trans}")
    print(f"Transitions per second: {trans_per_sec:,.0f}")
    print(f"Memory bytes per operation: {bytes_per_op:.1f}")
    print(f"Time per transition: {1e9 / trans_per_sec:.1f} ns")


if __name__ == "__main__":
    asyncio.run(main())