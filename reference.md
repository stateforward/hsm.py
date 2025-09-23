# Python HSM Library Reference

## Overview

The Python HSM library provides a complete implementation of UML-compliant hierarchical state machines (HSM) with support for async/await patterns, concurrent activities, timer-based events, and advanced features like choice pseudostates and deferred events.

## Core Architecture

### Key Components

- **`hsm.Instance`**: Base class for state machine instances
- **`hsm.Context`**: Cancellation and lifecycle management
- **`hsm.Model`**: State machine definition (created by `hsm.define()`)
- **`hsm.HSM`**: Runtime state machine executor
- **`hsm.Event`**: Event objects for state transitions
- **`hsm.Profiler`**: Performance monitoring tool

### Type Definitions

```python
# Core async function types
Operation = Callable[[Context, Instance, Event], Coroutine[None, None, None]]
Expression = Callable[[Context, Instance, Event], Coroutine[None, None, bool]]
Duration = Callable[[Context, Instance, Event], Coroutine[None, None, timedelta]]
```

## API Reference

### State Machine Definition

#### `hsm.define(name: str, *elements: NamedElement) -> Model`
Creates a state machine model with absolute path naming (prefixed with `/`).

```python
model = hsm.define('MyMachine',
    hsm.initial(hsm.target('idle')),
    hsm.state('idle', ...),
    hsm.state('active', ...)
)
```

#### `hsm.state(name: str, *elements: NamedElement) -> PartialState`
Defines a state with nested elements like transitions, entry/exit actions, and activities.

```python
hsm.state('idle',
    hsm.entry(idle_entry),
    hsm.exit(idle_exit),
    hsm.transition(hsm.on('start'), hsm.target('../active'))
)
```

#### `hsm.initial(target_or_element: Union[str, NamedElement], *elements: NamedElement) -> PartialInitial`
Defines the initial pseudostate for a state machine or composite state.

```python
hsm.initial(hsm.target('idle'))  # Simple initial transition
hsm.initial('custom_name', hsm.target('idle'))  # Named initial
```

**Important**: Initial pseudostates don't create namespace boundaries. You can target sibling states directly without using `../`:

```python
hsm.state('parent',
    hsm.initial(hsm.target('child1')),  # Direct reference, no ../
    hsm.state('child1'),
    hsm.state('child2')
)
```

### Transitions

#### `hsm.transition(name_or_element: Union[str, PartialElement], *elements: NamedElement) -> PartialTransition`
Creates a transition with optional name, events, guards, effects, and target.

```python
hsm.transition(
    hsm.on('event_name'),
    hsm.guard(my_guard),
    hsm.effect(my_effect),
    hsm.target('../target_state')
)
```

#### Transition Types (automatically determined):
- **External**: Different source and target states
- **Internal**: No target (empty string) - doesn't exit/enter state
- **Self**: Same source and target - exits and re-enters state  
- **Local**: Target is descendant of source

#### `hsm.on(*events: Union[str, Event]) -> PartialTrigger`
Specifies triggering events for a transition.

```python
hsm.on('start', 'begin')  # Multiple events
hsm.on(my_event)  # Event object
```

#### `hsm.target(name_or_element: Union[str, NamedElement]) -> PartialTarget`
Specifies the target state for a transition.

```python
hsm.target('../sibling_state')
hsm.target('/root/absolute/path')
hsm.target('nested_state')  # Relative path
```

#### `hsm.source(name_or_element: Union[str, NamedElement]) -> PartialSource`
Explicitly sets the source state (usually auto-determined).

```python
hsm.source('specific_state')
```

### Actions and Behaviors

#### `hsm.entry(*operations: Operation) -> PartialBehaviors`
Defines entry actions that run when entering a state.

```python
async def my_entry(ctx: Context, self: MyInstance, event: Event):
    self.log.append('entered')

hsm.entry(my_entry)
```

**Note**: When defining behaviors as class methods, use the `@staticmethod` decorator since the instance is passed as a parameter, not as `self`:

```python
class MyInstance(hsm.Instance):
    @staticmethod
    async def my_entry(ctx: Context, self: 'MyInstance', event: Event):
        self.log.append('entered')
    
    # Usage: hsm.entry(MyInstance.my_entry)
```

#### `hsm.exit(*operations: Operation) -> PartialBehaviors`
Defines exit actions that run when leaving a state.

```python
async def my_exit(ctx: Context, self: MyInstance, event: Event):
    self.log.append('exited')

hsm.exit(my_exit)
```

#### `hsm.activity(*operations: Operation) -> PartialBehaviors`
Defines concurrent activities that run while in a state. Activities are automatically cancelled when exiting the state.

```python
# Simple activity - no cleanup needed
async def my_activity(ctx: Context, self: MyInstance, event: Event):
    while not ctx.is_done():
        await asyncio.sleep(0.1)
        self.tick_count += 1

# Activity with cleanup handling
async def my_activity_with_cleanup(ctx: Context, self: MyInstance, event: Event):
    try:
        while not ctx.is_done():
            await asyncio.sleep(0.1)
            self.tick_count += 1
    except asyncio.CancelledError:
        # Perform cleanup if needed
        self.cleanup_resources()
        raise

hsm.activity(my_activity)
```

#### `hsm.effect(*operations: Operation) -> PartialBehaviors`
Defines transition effects that run during state transitions.

```python
async def my_effect(ctx: Context, self: MyInstance, event: Event):
    self.transition_count += 1

hsm.effect(my_effect)
```

### Guards and Conditions

#### `hsm.guard(expression: Expression) -> PartialGuard`
Defines a guard condition for a transition.

```python
async def my_guard(ctx: Context, self: MyInstance, event: Event) -> bool:
    return self.value > 10

hsm.guard(my_guard)
```

**Note**: When defining guards as class methods, use the `@staticmethod` decorator:

```python
class MyInstance(hsm.Instance):
    @staticmethod
    async def my_guard(ctx: Context, self: 'MyInstance', event: Event) -> bool:
        return self.value > 10
    
    # Usage: hsm.guard(MyInstance.my_guard)
```

### Timer-Based Events

#### `hsm.after(duration: Duration) -> PartialAfter`
Creates a one-time timer event that fires after a specified duration.

```python
async def my_delay(ctx: Context, self: MyInstance, event: Event) -> timedelta:
    return timedelta(seconds=5)

hsm.transition(
    hsm.after(my_delay),
    hsm.target('../timeout_state')
)
```

#### `hsm.every(duration: Duration) -> PartialEvery`
Creates a recurring timer event that fires at regular intervals.

```python
async def my_interval(ctx: Context, self: MyInstance, event: Event) -> timedelta:
    return timedelta(milliseconds=100)

hsm.transition(
    hsm.every(my_interval),
    hsm.effect(periodic_action)
)
```

**Timer Behavior:**
- Timers are automatically cancelled when exiting the state
- Zero or negative durations are ignored (no timer created)
- Timers can access event data and instance state for dynamic durations

### Choice Pseudostates

#### `hsm.choice(element_or_name: Union[str, PartialTransition], *transitions: PartialTransition) -> PartialChoice`
Creates a choice pseudostate for dynamic branching based on runtime conditions.

```python
hsm.choice('decision',
    hsm.transition(
        hsm.guard(condition1),
        hsm.target('path1')
    ),
    hsm.transition(
        hsm.guard(condition2), 
        hsm.target('path2')
    ),
    hsm.transition(  # Default path - must have no guard
        hsm.target('default_path')
    )
)
```

**Choice Requirements:**
- Last transition must have no guard (serves as default/else path)
- Guards are evaluated in order until one returns true
- Validation error if no guardless default transition exists

**Important**: Choice pseudostates don't create namespace boundaries. You can target sibling states directly without using `../`:

```python
hsm.state('parent',
    hsm.choice('decision',
        hsm.transition(
            hsm.guard(condition),
            hsm.target('child1')  # Direct reference, no ../
        ),
        hsm.transition(hsm.target('child2'))  # Direct reference, no ../
    ),
    hsm.state('child1'),
    hsm.state('child2')
)
```

### Final States

#### `hsm.final(name_or_element: Union[str, NamedElement]) -> PartialFinal`
Creates a final state that triggers completion events.

```python
hsm.final('done')
```

### Deferred Events

#### `hsm.defer(*events: Event) -> PartialDefer`
Defers events in a state - they're queued and processed when entering a state that can handle them.

```python
hsm.defer(Event('deferred_event'))
```

## Instance and Lifecycle Management

### Custom Instance Class

```python
class MyInstance(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.log = []
        self.data = {}
        
    def log_action(self, action: str):
        self.log.append(action)
```

### Starting a State Machine

#### `hsm.start(ctx: Optional[Context], instance: Instance, model: Model) -> HSM`
Starts a state machine instance.

```python
ctx = hsm.Context()
instance = MyInstance()
sm = await hsm.start(ctx, instance, model)
```

### Event Dispatching

#### `instance.dispatch(event: Event) -> Future[None]`
Dispatches an event to the state machine.

```python
await instance.dispatch(hsm.Event(name='start'))
await instance.dispatch(hsm.Event(name='data_event', data={'key': 'value'}))
```

### State Querying

#### `instance.state() -> str`
Gets the current state path.

```python
current_state = instance.state()  # e.g., '/MyMachine/active/substate'
```

### Stopping

#### `hsm.stop(sm: Union[HSM, Instance]) -> None`
Stops the state machine and cancels all activities.

```python
await hsm.stop(sm)
```

## Event System

### Event Class

```python
@dataclass
class Event:
    name: str = ""
    data: Any = None
    kind: Kinds = Kinds.Event
    qualified_name: str = ""
```

### Built-in Events

- **`hsm_initial`**: Automatically dispatched when entering states
- **`hsm_error`**: Dispatched when activities throw exceptions
- **Timer events**: Generated by `after()` and `every()` timers

### Event Types (Kinds)

```python
class Kinds(IntEnum):
    Event = ...
    CompletionEvent = ...
    ErrorEvent = ...
    TimeEvent = ...
```

## Context and Cancellation

### Context Class

```python
class Context:
    @property
    def done(self) -> bool: ...
    def cancel(self) -> None: ...
    def add_listener(self, event: str, callback: Callable[[], None]) -> None: ...
    async def wait_done(self) -> None: ...
```

### Usage in Activities

```python
async def my_activity(ctx: Context, self: MyInstance, event: Event):
    try:
        while not ctx.is_done():
            await asyncio.sleep(0.1)
            # Do work
    except asyncio.CancelledError:
        # Cleanup
        raise
```

## Error Handling

### Activity Errors
When activities throw exceptions, an `hsm_error` event is automatically dispatched with the exception in the event data.

```python
hsm.transition(
    hsm.on('hsm_error'),
    hsm.target('../error_state'),
    hsm.effect(handle_error)
)
```

### Validation Errors
The library performs extensive validation and throws `ValidationError` for:
- Invalid state machine structure
- Missing required elements
- Invalid choice pseudostate configurations
- Invalid initial transitions

## Performance Monitoring

### Profiler

```python
profiler = hsm.Profiler()  # or hsm.Profiler(disabled=True)

profiler.start('operation_name')
# ... do work ...
profiler.end('operation_name')

profiler.report()  # Print results
results = profiler.get_results()  # Get programmatic results
```

## Path Resolution

### Path Syntax
- **Absolute paths**: `/root/state/substate`
- **Relative paths**: `../sibling`, `child/grandchild`
- **Current state**: `.`
- **Parent state**: `..`

### Path Functions
- `is_ancestor(source: str, target: str) -> bool`: Check if source is ancestor of target
- `least_common_ancestor(path1: str, path2: str) -> str`: Find LCA of two paths

### Pseudostate Path Behavior
**Important**: Pseudostates (`initial` and `choice`) don't create namespace boundaries. When defining transitions within pseudostates, you can reference sibling states directly without using `../` prefixes. This is different from regular states which do create namespace boundaries.

## Advanced Features

### Hierarchical States
States can contain other states, creating a hierarchy with inheritance of behaviors.

```python
hsm.state('parent',
    hsm.activity(parent_activity),  # Runs for all child states
    hsm.state('child1', ...),
    hsm.state('child2', ...)
)
```

### Concurrent Behaviors
Multiple entry/exit/effect/activity functions can be specified and will run concurrently.

```python
hsm.entry(action1, action2, action3)  # All run concurrently
```

### Transition Optimization
The library builds optimized transition and deferred event lookup tables for O(1) performance.

## Common Patterns

### State Machine with Lifecycle

```python
class MyInstance(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.status = 'idle'

async def start_effect(ctx, self, event):
    self.status = 'running'

model = hsm.define('MyMachine',
    hsm.initial(hsm.target('idle')),
    hsm.state('idle',
        hsm.transition(
            hsm.on('start'),
            hsm.target('../running'),
            hsm.effect(start_effect)
        )
    ),
    hsm.state('running',
        hsm.transition(hsm.on('stop'), hsm.target('../idle'))
    )
)
```

### Timer-Based State Machine

```python
async def timeout_duration(ctx, self, event):
    return timedelta(seconds=self.timeout_value)

model = hsm.define('TimerMachine',
    hsm.initial(hsm.target('waiting')),
    hsm.state('waiting',
        hsm.transition(
            hsm.after(timeout_duration),
            hsm.target('../timeout')
        ),
        hsm.transition(
            hsm.on('cancel'),
            hsm.target('../cancelled')
        )
    ),
    hsm.state('timeout'),
    hsm.state('cancelled')
)
```

### Choice-Based Branching

```python
async def low_value_guard(ctx, self, event):
    return self.value < 10

async def high_value_guard(ctx, self, event):
    return self.value >= 50

model = hsm.define('BranchingMachine',
    hsm.initial(hsm.target('evaluate')),
    hsm.state('evaluate',
        hsm.transition(hsm.on('process'), hsm.target('../decision'))
    ),
    hsm.choice('decision',
        hsm.transition(
            hsm.guard(low_value_guard),
            hsm.target('low_processing')
        ),
        hsm.transition(
            hsm.guard(high_value_guard),
            hsm.target('high_processing')
        ),
        hsm.transition(hsm.target('normal_processing'))  # Default
    ),
    hsm.state('low_processing'),
    hsm.state('normal_processing'),
    hsm.state('high_processing')
)
```

### Activity-Based State Machine

```python
# Simple monitoring activity
async def monitoring_activity(ctx, self, event):
    while not ctx.is_done():
        await asyncio.sleep(1.0)
        self.check_count += 1
        if self.check_count > 10:
            self.dispatch(hsm.Event('threshold_reached'))

# Or with explicit cleanup handling
async def monitoring_activity_with_cleanup(ctx, self, event):
    try:
        while not ctx.is_done():
            await asyncio.sleep(1.0)
            self.check_count += 1
            if self.check_count > 10:
                self.dispatch(hsm.Event('threshold_reached'))
    except asyncio.CancelledError:
        self.log.append('monitoring_cancelled')
        raise

model = hsm.define('MonitoringMachine',
    hsm.initial(hsm.target('monitoring')),
    hsm.state('monitoring',
        hsm.activity(monitoring_activity),
        hsm.transition(
            hsm.on('threshold_reached'),
            hsm.target('../alert')
        )
    ),
    hsm.state('alert')
)
```

## Best Practices

1. **Always use async/await**: All operations, expressions, and durations should be async
2. **Use @staticmethod for class methods**: When defining behaviors, guards, or duration functions as class methods, use `@staticmethod` since the instance is passed as a parameter rather than `self`
3. **Handle cancellation**: Activities should check `ctx.is_done()` and optionally handle `CancelledError` for cleanup
4. **Use relative paths**: Prefer relative paths (`../sibling`) over absolute paths for maintainability. Remember that pseudostates (initial/choice) don't create namespace boundaries
5. **Validate at definition time**: The library validates state machines at definition time, catching errors early
6. **Profile performance**: Use the built-in profiler for performance-critical applications
7. **Use Context properly**: Pass context through async operations for proper cancellation handling
8. **Design for testability**: Create testable instance classes with observable state and logging

## Compatibility

- **Python 3.7+**: Requires async/await and typing support
- **AsyncIO**: Built on asyncio for concurrency
- **UML Compliance**: Follows UML state machine semantics
- **Cross-platform**: Works on all platforms supporting Python and asyncio 