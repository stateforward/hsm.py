# stateforward.hsm

A high-performance, hierarchical state machine (HSM) implementation for Python with asyncio support. This library provides a declarative, async-first approach to building complex state machines with efficient O(1) runtime performance.

## Features

* **Hierarchical States**: Support for nested state machines with proper scoping
* **Async/Await**: Full asyncio support for actions, guards, effects, and activities
* **Precomputed Lookups**: O(1) performance through precomputed transition and element maps
* **Event-Driven**: Deterministic event processing with guards and effects
* **Timers**: Built-in support for one-shot and recurring timers
* **Activities**: Long-running async operations that auto-cancel on state exit
* **Error Handling**: Automatic error event dispatching for robust error recovery
* **Type Safety**: Strong typing with comprehensive type hints

## Installation

```bash
pip install stateforward.hsm
```

## Quick Start

```python
import asyncio
import hsm

class Counter(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.value = 0

    @staticmethod
    async def increment(ctx, self, event):
        self.value += 1
        print(f"Count: {self.value}")

    model = hsm.define('Counter',
        hsm.initial(hsm.target('counting')),
        hsm.state('counting',
            hsm.transition(
                hsm.on('inc'),
                hsm.target('.'),  # Self-transition
                hsm.effect(increment)
            )
        )
    )

async def main():
    instance = Counter()
    ctx = hsm.Context()

    # Start the state machine
    sm = await hsm.start(ctx, instance, Counter.model)
    print(f"Initial state: {sm.state()}")

    # Dispatch events
    await sm.dispatch(hsm.Event('inc'))
    await sm.dispatch(hsm.Event('inc'))

    print(f"Final count: {instance.value}")

    # Clean shutdown
    await hsm.stop(sm)

if __name__ == "__main__":
    asyncio.run(main())
```

## Core Concepts

### Declarative Definition

State machines are defined using a declarative API that precomputes all lookups for optimal runtime performance:

```python
model = hsm.define(
    "MyMachine",
    hsm.initial(hsm.target("ready")),
    hsm.state("ready",
        hsm.transition(hsm.on("start"), hsm.target("running"))
    ),
    hsm.state("running",
        hsm.transition(hsm.on("stop"), hsm.target("ready"))
    )
)
```

### States and Hierarchy

* **`hsm.state(name, ...children)`**: Regular states that can contain nested states
* **`hsm.initial(hsm.target(path))`**: Defines the default entry point
* **`hsm.final(name)`**: Terminal states for completion handling
* **`hsm.choice(name, ...transitions)`**: Conditional branching logic

### Transitions

Transitions define how the machine responds to events:

```python
hsm.transition(
    hsm.on("event_name"),           # Event trigger
    hsm.source("current_state"),    # Optional explicit source
    hsm.target("next_state"),       # Destination state
    hsm.guard(async_guard_func),    # Optional condition
    hsm.effect(async_effect_func)   # Optional side effect
)
```

### Actions and Behaviors

All behavioral functions are async and receive `(ctx, instance, event)`:

* **`hsm.entry(action)`**: Executed when entering a state
* **`hsm.exit(action)`**: Executed when exiting a state
* **`hsm.effect(action)`**: Executed during transitions
* **`hsm.guard(guard_func)`**: Returns boolean to allow/block transitions
* **`hsm.activity(activity_func)`**: Long-running operations that auto-cancel

### Timers

Built-in timer support for time-based transitions:

```python
from datetime import timedelta

@staticmethod
async def one_second(ctx, self, event):
    return timedelta(seconds=1)

hsm.transition(
    hsm.after(one_second),  # One-shot timer
    hsm.target("next_state")
)

hsm.transition(
    hsm.every(one_second),  # Recurring timer
    hsm.target(".")
)
```

### Error Handling

Exceptions in actions automatically dispatch `hsm_error` events:

```python
class RobustMachine(hsm.Instance):
    @staticmethod
    async def risky_action(ctx, self, event):
        if random.random() < 0.1:
            raise ValueError("Something went wrong!")

    @staticmethod
    async def handle_error(ctx, self, event):
        print(f"Error occurred: {event.data}")
        # Recovery logic here

    model = hsm.define('RobustMachine',
        hsm.state('working',
            hsm.effect(risky_action),
            hsm.transition(hsm.on('hsm_error'), hsm.target('error_state'))
        ),
        hsm.state('error_state',
            hsm.entry(handle_error)
        )
    )
```

## Advanced Usage

### Class-Based Pattern

The recommended pattern is to define your state machine as a class:

```python
class TrafficLight(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.cycles = 0

    @staticmethod
    async def change_to_green(ctx, self, event):
        print("🟢 Green light")
        self.cycles += 1

    model = hsm.define('TrafficLight',
        hsm.initial(hsm.target('red')),
        hsm.state('red',
            hsm.transition(hsm.on('timer'), hsm.target('green'))
        ),
        hsm.state('green',
            hsm.entry(change_to_green),
            hsm.transition(hsm.on('timer'), hsm.target('yellow'))
        ),
        hsm.state('yellow',
            hsm.transition(hsm.on('timer'), hsm.target('red'))
        )
    )
```

### Path Resolution

State paths are resolved relative to the transition's source state:

* **Child**: `'child_state'`
* **Parent**: `'..'`
* **Sibling**: `'../sibling_state'`
* **Absolute**: `'/MachineName/state'`
* **Self**: `'.'`

### Runtime Lifecycle

```python
# Create instance and context
instance = MyMachine()
ctx = hsm.Context()

# Start the machine
sm = await hsm.start(ctx, instance, MyMachine.model)

# Check current state
current_state = sm.state()

# Dispatch events
await sm.dispatch(hsm.Event('my_event', data={'key': 'value'}))

# Stop the machine
await hsm.stop(sm)
```

## API Reference

### Core Functions

* **`hsm.define(name, ...elements)`**: Create a state machine model
* **`hsm.start(ctx, instance, model)`**: Start a state machine instance
* **`hsm.stop(instance)`**: Stop and cleanup a state machine

### State Elements

* **`hsm.state(name, ...children)`**: Define a state
* **`hsm.initial(target, ...effects)`**: Define initial state
* **`hsm.final(name)`**: Define final state
* **`hsm.choice(name, ...transitions)`**: Define choice pseudostate

### Transitions

* **`hsm.transition(...conditions)`**: Define a transition
* **`hsm.on(event_name)`**: Event condition
* **`hsm.after(duration_func)`**: Timer condition (one-shot)
* **`hsm.every(duration_func)`**: Timer condition (recurring)
* **`hsm.target(path)`**: Target state
* **`hsm.guard(func)`**: Guard condition
* **`hsm.effect(func)`**: Transition effect

### Actions

* **`hsm.entry(func)`**: Entry action
* **`hsm.exit(func)`**: Exit action
* **`hsm.activity(func)`**: Long-running activity

### Runtime

* **`hsm.Instance`**: Base class for state machine instances
* **`hsm.Context`**: Execution context with cancellation support
* **`hsm.Event(name, data=None)`**: Event object

## Performance

This implementation uses precomputed lookup tables for O(1) transition resolution, making it suitable for high-performance applications. All behavioral functions are async-first, enabling efficient concurrent execution.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please see the main [HSM repository](https://github.com/stateforward/hsm) for contribution guidelines.
