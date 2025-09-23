# hsm-python Guide

This document provides a comprehensive guide for an LLM to generate state machines using the `hsm-python` library, referencing the HSM Framework Specification v1.0.

## Core Concepts (from Spec)

The `hsm-python` library implements a deterministic, hierarchical state machine framework. It uses a declarative, `async`-first approach.

- **Declarative Definition**: You define the structure of the state machine in an immutable model.
- **Precomputed Lookups**: During definition (`hsm.define()`), the framework MUST precompute all transition and element lookups into maps for O(1) runtime performance.
- **Element Map**: All states and other elements are stored in a flat map keyed by their fully qualified path (e.g., `'/MyMachine/parent/child'`).
- **String-Based References**: All references between elements (like transition targets) use these qualified path strings, which allows for easy serialization.
- **Concurrency**: The Python binding uses `asyncio` for all operations, including actions, guards, and activities.

---

## API and Usage

### 1. Defining a State Machine

Use `hsm.define()` to create the root of the state machine. This function computes the necessary lookup maps for efficient execution.

```python
import hsm

model = hsm.define(
    "MyMachine",
    # ... states and transitions ...
)
```

### 2. States

- **`hsm.state(name, ...children)`**: Defines a regular state. States can be nested to create a hierarchy.
- **`hsm.initial(hsm.target(path), ...)`**: A pseudo-state that defines the default entry point for a machine or a composite state. The target path is relative.
- **`hsm.final(name)`**: A terminal pseudo-state. When a composite state enters a final substate, it has finished its work and will not process further events within that composite state.
- **`hsm.choice(name, ...transitions)`**: A pseudo-state for dynamic, conditional branching.

### 3. Transitions

Transitions define how the machine reacts to events.

- **`hsm.transition(...)`**: Defines a transition.
- **`hsm.on(event_name)`**: Specifies the event name that triggers the transition.
- **`hsm.target(path)`**: Specifies the destination state.

#### Transition Scoping (CRITICAL)
The **source state** of a transition is implicitly determined by where it is defined in the code.
- A `hsm.transition(...)` passed as an argument to `hsm.state('my_state', ...)` can **only** be triggered when the machine is in `'my_state'` or one of its children.
- A `hsm.transition(...)` passed as a top-level argument to `hsm.define(...)` applies to the entire machine and is evaluated if no more specific transition is found in the current state.

#### Transition Types:
- **External**: `hsm.target('../other_state')`. Exits the source state, runs effects, and enters the target state.
- **Self**: `hsm.target('.')`. Exits and re-enters the *same* state.
- **Internal**: Omit `hsm.target()`. Runs effects without exiting or entering any state.

### 4. Path Resolution

The `hsm.target()` path is resolved relative to the transition's **source state**, which is determined by where the transition is defined in the model structure (see Transition Scoping).

- **Child**: `'child'`
- **Relative**: `'../sibling'`
- **Absolute**: `'/MyMachine/some/state'`
- **Self**: `'.'`
- **Parent**: `'..'`

### 5. Actions, Effects, and Guards

All behavioral functions are `async` and receive `(ctx, instance, event)`.

- **`hsm.entry(action)`**: Executed upon entering a state.
- **`hsm.exit(action)`**: Executed upon exiting a state.
- **`hsm.effect(action)`**: Executed during a transition. It runs *after* the source state's `exit` action and *before* the target state's `entry` action.
- **`hsm.guard(guard_func)`**: An `async` function that returns a boolean. If it returns `False`, the transition is blocked.

### 6. Choice Pseudostates

For if/elif/else logic. Guards are evaluated in order. The **last** transition **must not** have a guard and serves as the fallback `else` case.

### 7. Activities

Long-running `async` operations that start on state entry and are automatically cancelled on state exit.

### 8. Timers

Trigger transitions based on time. The duration function must be `async` and return a `timedelta`.

- **`hsm.after(duration_func)`**: One-shot timer.
- **`hsm.every(duration_func)`**: Recurring timer.

### 9. Error Handling

- Exceptions in any behavioral function automatically dispatch a special `hsm_error` event.
- The `event.data` of this event contains the original exception.
- Handle these events with normal transitions to create error-recovery states.

---

## Structuring Your State Machine

### Class-Based Pattern (Recommended)

A highly effective way to organize your state machine is to define the model and its actions within a class that inherits from `hsm.Instance`. This co-locates all related logic.

- **Inherit from `hsm.Instance`**: This class will serve as both the definition container and the runtime instance.
- **Define State on the Instance**: Use the `__init__` method to define instance attributes for your state. This is the Pythonic way to handle state, avoiding generic `data` dictionaries.
- **Define `model` as a Class Attribute**: Use `hsm.define()` to create the model at the class level.
- **Use `@staticmethod` for Actions**: Define entry, exit, effect, and guard functions as static methods. This is because they don't operate on the class itself, but on the runtime instance (`self`) passed to them by the HSM engine.

```python
import hsm

class TrafficLight(hsm.Instance):
    # Define instance attributes for state in __init__
    def __init__(self):
        super().__init__()
        self.cycles = 0

    @staticmethod
    async def green_entry(ctx, self, event):
        print("Green light")

    @staticmethod
    async def yellow_entry(ctx, self, event):
        print("Yellow light")

    @staticmethod
    async def red_entry(ctx, self, event):
        print("Red light")
        self.cycles += 1 # Modify instance attributes directly

    # Define the model as a class attribute
    model = hsm.define(
        "TrafficLight",
        hsm.initial(hsm.target("red")),
        hsm.state("red",
            hsm.entry(red_entry),
            hsm.transition(hsm.on("timer_complete"), hsm.target("../green")),
        ),
        hsm.state("green",
            hsm.entry(green_entry),
            hsm.transition(hsm.on("timer_complete"), hsm.target("../yellow")),
        ),
        hsm.state("yellow",
            hsm.entry(yellow_entry),
            hsm.transition(hsm.on("timer_complete"), hsm.target("../red")),
        ),
    )
```

---

## Runtime

### Instance and Context

- **`hsm.Instance`**: Subclass this to hold runtime data as instance attributes (e.g., `self.value`, `self.log`).
- **`hsm.Context`**: Provides the execution context, including cancellation signals. Create a new one for each run: `ctx = hsm.Context()`.

### Lifecycle

1.  **`hsm.start(ctx, instance, model)`**: Starts the state machine. Returns the running instance.
2.  **`instance.dispatch(hsm.Event(name, data))`**: Sends an event to the machine. `data` is an optional dictionary.
3.  **`hsm.stop(instance)`**: Stops the machine and cleans up all resources (activities, timers).

### Full Example (Class-Based)

```python
import asyncio
import hsm
from hsm.hsm import Instance, Event, Context

# 1. Define the machine and its logic in a class
class Counter(Instance):
    def __init__(self):
        super().__init__()
        self.log = []
        self.value = 0 # Use instance attributes for state

    @staticmethod
    async def increment(ctx, self, event):
        self.value += 1 # Modify attributes directly
        self.log.append(f"incremented to {self.value}")

    model = hsm.define('Counter',
        hsm.initial(hsm.target('counting')),
        hsm.state('counting',
            hsm.transition(
                hsm.on('inc'),
                hsm.target('.'), # Self-transition
                hsm.effect(increment)
            )
        )
    )

# 2. Run the machine
async def main():
    instance = Counter()
    ctx = Context()
    # Pass the class's model to the start function
    sm = await hsm.start(ctx, instance, Counter.model)

    print(f"Initial state: {sm.state()}")
    
    await sm.dispatch(Event('inc'))
    await sm.dispatch(Event('inc'))

    print(f"Final value: {instance.value}")
    print(f"Log: {instance.log}")

    await hsm.stop(sm)

# To run:
# if __name__ == "__main__":
#     asyncio.run(main())
```
