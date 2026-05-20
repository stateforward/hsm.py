# stateforward.hsm

`stateforward.hsm` is an asyncio hierarchical state machine runtime for Python. It uses the same canonical PascalCase DSL as the rest of the Stateforward HSM implementations, with lowercase aliases available for Python callers that prefer them.

Install:

```bash
pip install stateforward.hsm
```

Import:

```python
import hsm
```

## Minimal Example

```python
import asyncio
import hsm

class Counter(hsm.Instance):
    def __init__(self):
        super().__init__()
        self.value = 0

async def increment(ctx: hsm.Context, inst: Counter, event: hsm.Event) -> None:
    inst.value += event.Data or 1

model = hsm.Define(
    "Counter",
    hsm.Attribute("count", 0),
    hsm.Initial(hsm.Target("idle")),
    hsm.State(
        "idle",
        hsm.Transition(
            hsm.On("inc"),
            hsm.Target("."),
            hsm.Effect(increment),
        ),
    ),
)

async def main() -> None:
    ctx = hsm.Context()
    instance = Counter()

    sm = await hsm.Started(ctx, instance, model, hsm.Config(ID="counter-1"))
    await hsm.Dispatch(ctx, instance, hsm.Event("inc").WithData(2))

    assert instance.State() == "/Counter/idle"
    assert instance.value == 2
    assert hsm.ID(instance) == "counter-1"

    await hsm.Stop(instance)

asyncio.run(main())
```

## Canonical Naming

The canonical API is PascalCase: `Define`, `State`, `Transition`, `Start`, `Dispatch`, `TakeSnapshot`, and so on. Lowercase aliases such as `define`, `state`, and `dispatch` exist for convenience, but docs and cross-language examples should use PascalCase.

## API Map

| Area | API |
| --- | --- |
| Model DSL | `Define`, `State`, `Initial`, `Final`, `Choice`, `ShallowHistory`, `DeepHistory` |
| Transitions | `Transition`, `Source`, `Target`, `On`, `OnSet`, `OnCall`, `After`, `At`, `Every`, `When`, `Guard`, `Effect`, `Defer` |
| State behavior | `Entry`, `Exit`, `Activity` |
| Model metadata | `Attribute`, `Operation` |
| Runtime lifecycle | `New`, `Start`, `Started`, `Stop`, `Restart` |
| Runtime event flow | `Event`, `Dispatch`, `DispatchAll`, `DispatchTo` |
| Runtime data | `Get`, `Set`, `Call` |
| Runtime identity | `Config`, `ID`, `Name`, `QualifiedName` |
| Timers | `Clock`, `DefaultClock`, `Config(Clock=...)` |
| Observability | `TakeSnapshot`, `AfterDispatch`, `AfterProcess`, `AfterEntry`, `AfterExit`, `AfterExecuted` |
| Utilities | `Match`, `LCA`, `IsAncestor`, `MakeKind`, `IsKind`, kind constants |

## Model DSL

A model is built once with `Define(name, *partials)` and then reused by runtime instances.

```python
model = hsm.Define(
    "Door",
    hsm.Initial(hsm.Target("closed")),
    hsm.State(
        "closed",
        hsm.Transition(hsm.On("open"), hsm.Target("../open")),
    ),
    hsm.State(
        "open",
        hsm.Transition(hsm.On("close"), hsm.Target("../closed")),
    ),
)
```

State paths are qualified under the model name. From inside a state, relative paths are accepted:

| Path | Meaning |
| --- | --- |
| `"child"` | Child of the current state |
| `"../sibling"` | Sibling state |
| `"."` | Current source state, used for self-transitions |
| `"/Door/open"` | Absolute path |

## Transitions

A transition combines a trigger, optional source, optional target, optional guard, and optional effects.

```python
hsm.Transition(
    hsm.On("submit"),
    hsm.Source("draft"),
    hsm.Target("review"),
    hsm.Guard(can_submit),
    hsm.Effect(record_submit, notify_reviewer),
)
```

Behavior callbacks receive `(ctx, instance, event)`. They may be sync or async unless a specific API documents otherwise.

```python
async def can_submit(ctx, inst, event) -> bool:
    return inst.ready

async def record_submit(ctx, inst, event) -> None:
    inst.submitted = True
```

Transition kinds are inferred:

| Shape | Runtime behavior |
| --- | --- |
| `Target(".")` or target equals source | Self-transition; exits and re-enters the state |
| No target | Internal transition; executes effects without state exit/entry |
| Target below source | Local transition |
| Other target | External transition |

## State Behavior

```python
hsm.State(
    "running",
    hsm.Entry(on_enter),
    hsm.Activity(run_until_exit),
    hsm.Exit(on_exit),
)
```

`Activity` callbacks run concurrently while the state is active. They are canceled on state exit or machine stop.

## Events

Create events with `Event(name, data=None)`. `WithData` and `WithDataAndID` mirror the Go API and return a new event.

```python
event = hsm.Event("update").WithData({"message": "hello"})
event_with_id = hsm.Event("update").WithDataAndID({"message": "hello"}, "evt-1")

await hsm.Dispatch(ctx, instance, event)
```

Common built-ins:

| Constant | Meaning |
| --- | --- |
| `InitialEvent` | Startup transition event |
| `FinalEvent` | Final/completion event |
| `ErrorEvent` | Error event dispatched when behavior raises |
| `AnyEvent` | Wildcard fallback event |

## Attributes

Declare model attributes with `Attribute`. Read and write runtime values with `Get` and `Set`. `OnSet(name)` transitions fire when an attribute changes.

```python
model = hsm.Define(
    "Thermostat",
    hsm.Attribute("temperature", 70),
    hsm.Initial(hsm.Target("idle")),
    hsm.State(
        "idle",
        hsm.Transition(hsm.OnSet("temperature"), hsm.Target("../changed")),
    ),
    hsm.State("changed"),
)

value, ok = hsm.Get(ctx, instance, "temperature")
await hsm.Set(ctx, instance, "temperature", 72)
```

Short names are accepted by `Get`, `Set`, `Attribute`, and `OnSet`. Snapshots use fully-qualified attribute names, for example `"/Thermostat/temperature"`.

## Operations

`Operation(name, callback=None)` declares a callable operation. `OnCall(name)` transitions fire when the operation is called through `Call`.

```python
async def approve(ctx, inst, request_id: str) -> str:
    inst.approved.append(request_id)
    return "ok"

model = hsm.Define(
    "Approval",
    hsm.Operation("approve", approve),
    hsm.Initial(hsm.Target("waiting")),
    hsm.State(
        "waiting",
        hsm.Transition(hsm.OnCall("approve"), hsm.Target("../approved")),
    ),
    hsm.State("approved"),
)

result = await hsm.Call(ctx, instance, "approve", "req-7")
```

If no callback is supplied to `Operation`, `Call` looks for a method with the same name on the instance.

## Timers And Clock

`After(duration_fn)` fires once after a relative duration. `At(timepoint_fn)` fires once at an absolute `datetime.datetime`. `Every(duration_fn)` fires repeatedly while the source state remains active. The timing function receives `(ctx, instance, event)`.

```python
from datetime import timedelta

async def one_second(ctx, inst, event) -> timedelta:
    return timedelta(seconds=1)

hsm.State(
    "waiting",
    hsm.Transition(
        hsm.After(one_second),
        hsm.Target("../done"),
    ),
)
```

Use `At` for absolute deadlines:

```python
from datetime import datetime, timedelta

async def two_hours_from_now(ctx, inst, event) -> datetime:
    return datetime.now() + timedelta(hours=2)

hsm.Transition(
    hsm.At(two_hours_from_now),
    hsm.Target("../done"),
)
```

Timers use the runtime clock. The default clock uses `asyncio.sleep`. Inject a clock to make tests deterministic:

```python
pending = []

async def manual_sleep(duration: timedelta) -> None:
    future = asyncio.get_running_loop().create_future()
    pending.append((duration, future))
    await future

clock = hsm.Clock(sleep=manual_sleep)
sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))

# Release the timer manually.
pending[0][1].set_result(None)
```

`DefaultClock` is the fallback clock. A partial `Clock` inherits missing behavior from `DefaultClock`.

## Runtime Lifecycle

Use `Started` to construct and start in one call:

```python
sm = await hsm.Started(ctx, instance, model)
```

Use `New` and `Start` when construction and start need to be separate:

```python
sm = hsm.New(instance, model, hsm.Config(ID="alpha"))
await hsm.Start(ctx, sm)
```

Runtime configuration:

```python
config = hsm.Config(
    ID="alpha",
    Name="/RuntimeName",
    Data={"boot": True},
    Clock=hsm.DefaultClock,
)

sm = await hsm.Started(ctx, instance, model, config)
```

Lifecycle calls:

```python
await hsm.Dispatch(ctx, instance, hsm.Event("go"))
await hsm.Restart(instance, {"reason": "reset"})
await hsm.Stop(instance)
```

`Dispatch`, `Set`, `Call`, `Restart`, and `Stop` are awaitable in Python. Await them before asserting post-transition state.

## Groups And Broadcast

`NewGroup` flattens nested groups and forwards runtime operations to all members.

```python
group = hsm.NewGroup(first, hsm.NewGroup(second))

await hsm.Dispatch(ctx, group, hsm.Event("refresh"))
await hsm.Set(ctx, group, "temperature", 72)
await hsm.Stop(group)
```

`DispatchAll(ctx, event)` dispatches to all started machines registered in the context. `DispatchTo(ctx, event, *patterns)` dispatches to matching machine IDs. Patterns use `Match` wildcard semantics.

## Snapshots And Identity

`TakeSnapshot(ctx, machine)` returns a `Snapshot`:

```python
snapshot = hsm.TakeSnapshot(ctx, instance)

snapshot.ID             # Runtime instance ID
snapshot.QualifiedName  # Runtime machine name
snapshot.State          # Current active state path
snapshot.Attributes     # Fully-qualified attribute map
snapshot.QueueLen       # Pending queue length
snapshot.Events         # Enabled event/transition details
```

Identity helpers read from snapshots:

```python
hsm.ID(instance)
hsm.Name(instance)
hsm.QualifiedName(instance)
```

`Config(ID=..., Name=...)` controls runtime identity. The model path and active state paths still come from the model definition.

## Observability Waiters

These helpers are for deterministic tests and instrumentation:

```python
entered = hsm.AfterEntry(ctx, instance, "/Machine/ready")
processed = hsm.AfterProcess(ctx, instance, hsm.Event("go"))
dispatched = hsm.AfterDispatch(ctx, instance, hsm.Event("go"))
exited = hsm.AfterExit(ctx, instance, "/Machine/idle")
executed = hsm.AfterExecuted(ctx, instance, "/Machine/running")

await hsm.Dispatch(ctx, instance, hsm.Event("go"))
await entered
```

## Error Handling

Exceptions in guards are treated as failed guards. Exceptions in actions or activities dispatch `ErrorEvent` with the exception as event data.

```python
hsm.Transition(
    hsm.On(hsm.ErrorEvent),
    hsm.Target("../error"),
)
```

## Lowercase Aliases

Lowercase aliases are exported for Python ergonomics, for example `define`, `state`, `transition`, `on`, `target`, `start`, and `dispatch`. They map directly to the PascalCase APIs. Prefer PascalCase in shared docs and generated code because it matches `dsl.md` and sibling implementations.

## Current Python Notes

`After`, `At`, `Every`, and `When` are implemented.

This package requires Python 3.13 or newer.

## License

MIT License. See [LICENSE](LICENSE).
