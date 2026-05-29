# Changelog

## 1.0.0 - 2026-05-29

- Passed the full shared HSM conformance suite for the Python runtime.
- Added submachine state, entry point, and exit point runtime support.
- Aligned context propagation with the canonical `Keys.HSM`, `Keys.Owner`, and `Keys.Instances` model.
- Aligned `DispatchAll`, `DispatchTo`, and group dispatch with fire-and-forget submission plus awaitable completion.
- Hardened timers, activity cancellation, lifecycle restart/stop, queue ownership, deferred replay, event metadata isolation, and snapshot behavior.
- Expanded Python aliases for canonical DSL/runtime exports.
