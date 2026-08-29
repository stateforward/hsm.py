# Changelog

## 1.3.8 - 2026-08-28

- Return dispatch admission results so callers can observe whether an active
  recipient accepted an event into its queue.

## 1.3.7 - 2026-08-27

- Deliver events from completed caller contexts and avoid deadlocks when events
  are submitted during machine stop teardown.

## 1.3.6 - 2026-08-27

- Preserve dispatch delivery during the machine stop window while avoiding activity cancellation deadlocks.

## 1.3.5 - 2026-08-27

- Prevent activity dispatch during machine shutdown from deadlocking stop.

## 1.3.4 - 2026-08-26

- Remove stopped HSM instances from the shared instance registry.

## 1.3.2 - 2026-07-18

- Unified newly created and stopped machine state as the empty string (`""`).
- Preserved inactive lifecycle guards for dispatch, operations, snapshots, restart, groups, and fanout.
- Aligned the shared lifecycle conformance cases with the empty inactive-state contract.

## 1.1.1 - 2026-06-11

- Fixed event metadata preservation when dispatch assigns event IDs and when `DispatchTo` fans out events.
- Updated the Python test suite for the refactored runtime API and current v1.1 conformance behavior.

## 1.1.0 - 2026-06-11

- Advanced shared HSM conformance coverage with operation, OnCall, history, submachine, exit-point, group, queue, timer, and observation coverage while the v1.1 conformance IR is being updated.
- Added operation declaration/call support, `OnCall` transitions, and named-operation references for DSL behaviors.
- Added group dispatch/snapshot behavior, `DispatchAll`, `DispatchTo`, and dispatchable group support.
- Aligned RTC state visibility so snapshots observe only the last settled state during entry, exit-point, operation, and startup behavior execution.
- Added event metadata support for observability payloads while preserving event data by reference.
- Hardened submachine entry/exit point handling, deferred replay, activity cancellation, history restoration, and clock/queue conformance behavior.

## 1.0.0 - 2026-05-29

- Continued alignment with the shared HSM conformance suite for the Python runtime.
- Added submachine state, entry point, and exit point runtime support.
- Aligned context propagation with the canonical `Keys.HSM`, `Keys.Owner`, and `Keys.Instances` model.
- Aligned `DispatchAll`, `DispatchTo`, and group dispatch with fire-and-forget submission plus awaitable completion.
- Hardened timers, activity cancellation, lifecycle restart/stop, queue ownership, deferred replay, event metadata isolation, and snapshot behavior.
- Expanded Python aliases for canonical DSL/runtime exports.
