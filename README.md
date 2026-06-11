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

The canonical API is PascalCase: `Define`, `State`, `Transition`, `Start`, `Started`, `Dispatch`, `TakeSnapshot`, and so on. Lowercase and snake_case aliases such as `define`, `state`, `started`, `dispatch`, `take_snapshot`, `make_group`, `event`, `clock`, and `state_kind` exist for Python callers. Docs and cross-language examples should use PascalCase.

## API Map

| Area | API |
| --- | --- |
| Model DSL | `Define`, `State`, `Initial`, `Final`, `Choice`, `ShallowHistory`, `DeepHistory` |
| Transitions | `Transition`, `Source`, `Target`, `On`, `OnSet`, `OnCall`, `After`, `At`, `Every`, `When`, `Guard`, `Effect`, `Defer` |
| State behavior | `Entry`, `Exit`, `Activity` |
| Model metadata | `Attribute`, `Operation` |
| Runtime lifecycle | `New`, `Start`, `Started`, `Stop`, `Restart` |
| Runtime event flow | `Event`, `Dispatch`, `DispatchAll`, `DispatchTo` |
| Runtime queue | `Fifo`, `Queue`, `MultiQueue`, `Config(Queue=...)` |
| Runtime data | `Get`, `Set`, `Call` |
| Runtime identity | `Config`, `ID`, `Name`, `QualifiedName` |
| Timers | `Clock`, `DefaultClock`, `Config(Clock=...)` |
| Observability | `TakeSnapshot`, `AfterDispatch`, `AfterProcess`, `AfterEntry`, `AfterExit`, `AfterExecuted` |
| Utilities | `Match`, `LCA`, `IsAncestor`, `MakeKind`, `IsKind`, `MakeGroup`, kind constants |

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

`Guard("operation_name")` and `Effect("operation_name")` resolve a declared `Operation` and pass the triggering event as the operation argument. This is the same DSL shape used by TypeScript, Go, and `dsl.md`.

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

`Entry`, `Exit`, and `Activity` also accept operation names:

```python
hsm.Define(
    "Worker",
    hsm.Operation("enter_running", enter_running),
    hsm.Initial(hsm.Target("running")),
    hsm.State("running", hsm.Entry("enter_running")),
)
```

## Events

Create events with `Event(name, data=None)`. `Event` is generic for static analysis: `Event("update", {"message": "hello"})` is inferred as `Event[dict[str, str]]`, and `event.Data` has that payload type. `WithData` and `WithDataAndID` mirror the Go API and return a new event with the new payload type.

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

Dispatch uses an immutable event envelope (`name`, `source`, `target`, `id`, and `kind`) for routing. `Event.Data` and `Event.schema` are intentionally shared by reference; payload and metadata ownership belong to the caller/application. Use immutable values or make an application-level copy when handlers must not share mutable objects.

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

Short names are accepted by `Get`, `Set`, `Attribute`, and `OnSet`. Direct `Set` stores the provided value by reference. Group `Set` deep-copies the provided value once per member, so member handlers cannot mutate the caller's value or each other's stored value. Snapshots use fully-qualified attribute names, for example `"/Thermostat/temperature"`, and snapshot attribute values are deep-copied from runtime storage.

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

If no callback is supplied to `Operation`, `Call` and named-operation behaviors look for a method with the same name on the instance.

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

`NewGroup` flattens nested groups for broadcast dispatch and lifecycle operations. `MakeGroup` is also exported for DSL parity with TypeScript and `dsl.md`; `new_group` and `make_group` are the Python snake_case aliases.

```python
group = hsm.MakeGroup(first, hsm.MakeGroup(second))

await hsm.Dispatch(ctx, group, hsm.Event("refresh"))
await hsm.Stop(group)
```

`DispatchAll(ctx, event)` dispatches to all started machines registered in the context. `DispatchTo(ctx, event, *patterns)` dispatches to matching machine IDs. Patterns use `Match` wildcard semantics.

Group `Restart(data)` deep-copies `data` once per member before startup entry handlers receive it. Attribute `Get`/`Set` and operation `Call` are instance or HSM operations, not group operations.

Groups are behavior elements, so they can be used in `Entry`, `Exit`, `Effect`, and `Activity` to fire-and-forget dispatch the current event to each member.

## Snapshots And Identity

`TakeSnapshot(ctx, machine)` returns a `Snapshot`. `TakeSnapshot(ctx, group)` returns one `Snapshot` per grouped instance:

```python
snapshot = hsm.TakeSnapshot(ctx, instance)
snapshots = hsm.TakeSnapshot(ctx, group)

snapshot.ID             # Runtime instance ID
snapshot.QualifiedName  # Runtime machine name
snapshot.State          # Current active state path
snapshot.Attributes     # Fully-qualified attribute map
snapshot.QueueLen       # Pending queue length
snapshot.Transitions    # Enabled transition details

snapshot.queue_len      # Same value, using Python snake_case
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

## Python Aliases

<!-- Python aliases and runtime value exports from hsm/hsm.py -->

Implemented PascalCase DSL and runtime functions, except the acronym class `HSM`, expose direct Python snake_case aliases. Current builder and runtime aliases include `define`, `state`, `submachine_state`, `entry_point`, `exit_point`, `initial`, `transition`, `source`, `target`, `entry`, `exit`, `activity`, `effect`, `guard`, `on`, `after`, `at`, `every`, `when`, `defer`, `choice`, `shallow_history`, `deep_history`, `final`, `validator`, `finalizer`, `start`, `started`, `stop`, `restart`, `take_snapshot`, `id`, `qualified_name`, `name`, `new_group`, `make_group`, `dispatch`, `dispatch_all`, and `dispatch_to`. The `hsm.kind` helper module exposes `Make`, `MakeKind`, `IsKind`, `List`, and `Bases` plus Python aliases `make`, `make_kind`, `is_kind`, `list`, and `bases`.

Runtime values and types exported from the top-level package include `Event`, `CompletionEvent`, `Snapshot`, `Model`, `FinalizedModel`, `ModelValidator`, `DefaultModelValidator`, `ValidatorElement`, `ModelFinalizer`, `DefaultModelFinalizer`, `FinalizerElement`, `Clock`, `DefaultClock`, `Fifo`, `Queue`, `MultiQueue`, queue result tuples such as `QueuePushResult`, `Config`, `Context`, `ContextKey`, `Keys`, `Instance`, `Group`, `Dispatchable`, lifecycle events such as `InitialEvent`, `ErrorEvent`, `AnyEvent`, and `FinalEvent`, and kind constants such as `StateKind`, `TransitionKind`, `EventKind`, `FinalStateKind`, `SubmachineStateKind`, and `ExitPointKind`. Event helpers expose `with_data` and `with_data_and_id` alongside `WithData` and `WithDataAndID`; `Config` accepts and exposes `id`, `name`, `data`, `clock`, and `queue` alongside `ID`, `Name`, `Data`, `Clock`, and `Queue`; snapshots expose both PascalCase fields such as `QueueLen` and `Transitions` and snake_case properties such as `queue_len` and `transitions`. Prefer PascalCase in shared docs and generated code because it matches `dsl.md` and sibling implementations.

## Current Python Notes

`After`, `At`, `Every`, `When`, `SubmachineState`, `EntryPoint`, and `ExitPoint` are implemented. The Python runtime passes the shared HSM conformance suite.

This package requires Python 3.13 or newer.

## Testing And Hardening

Run the full verification suite before shipping changes:

```bash
uv sync --group dev
uv export --quiet --all-groups --no-emit-project --format requirements.txt --output-file audit-requirements.txt
uv run pip-audit -r audit-requirements.txt --require-hashes --disable-pip --strict --progress-spinner off
uv run pytest -W error --cov=hsm --cov-report=term-missing --cov-fail-under=90
uv run pyright
uv build
uvx twine check dist/*
```

Run shared conformance cases from the monorepo root with the Python runner:

```bash
PYTHONPATH=hsm.py python3 hsm.py/conformance/run_case.py conformance/cases/basic_transition.json
```

The v1.0 release candidate passes all 1,392 shared conformance cases. The runner exits `77` when every selected failure is an explicit unsupported-feature skip.

The suite includes deterministic Hypothesis fuzz tests for generated state
machines, guarded transition order, runtime attribute updates, and invalid timer
callbacks, plus stress tests for concurrent dispatch, broadcast dispatch,
runtime `Set`/`Call`, timers, history re-entry, and activity cancellation
cleanup. CI runs the same tests, dependency vulnerability audit, package
checks, wheel smoke test, typed-marker check, Pyright type check, and coverage
threshold on pushes and pull requests.

Longer deterministic soak tests are available when needed:

```bash
HSM_SOAK=1 uv run pytest tests/test_soak.py
```

The CI workflow also runs soak tests on its nightly schedule, and supports a
manual `workflow_dispatch` run with the `soak` input enabled.

Use [RELEASE.md](RELEASE.md) for the full release checklist, including wheel
smoke tests, PyPI publication, clean PyPI install verification, and CI
monitoring.

## Security

See [SECURITY.md](SECURITY.md) for supported versions, private vulnerability
reporting, the library security model, and security-release verification gates.

## License

MIT License. See [LICENSE](LICENSE).
