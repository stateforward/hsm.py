# Changelog

## 1.1.0 - 2026-06-11

- Passed all 1,392 shared HSM conformance cases with operation, OnCall, history, submachine, exit-point, group, queue, timer, and observation coverage.
- Added operation declaration/call support, `OnCall` transitions, and named-operation references for DSL behaviors.
- Added group dispatch/snapshot behavior, `DispatchAll`, `DispatchTo`, and dispatchable group support.
- Aligned RTC state visibility so snapshots observe only the last settled state during entry, exit-point, operation, and startup behavior execution.
- Added event metadata support for observability payloads while preserving event data by reference.
- Hardened submachine entry/exit point handling, deferred replay, activity cancellation, history restoration, and clock/queue conformance behavior.

## 1.0.0 - 2026-05-29

- Passed the full shared HSM conformance suite for the Python runtime.
- Added submachine state, entry point, and exit point runtime support.
- Aligned context propagation with the canonical `Keys.HSM`, `Keys.Owner`, and `Keys.Instances` model.
- Aligned `DispatchAll`, `DispatchTo`, and group dispatch with fire-and-forget submission plus awaitable completion.
- Hardened timers, activity cancellation, lifecycle restart/stop, queue ownership, deferred replay, event metadata isolation, and snapshot behavior.
- Expanded Python aliases for canonical DSL/runtime exports.
