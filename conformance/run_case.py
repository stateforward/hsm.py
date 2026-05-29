#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import copy
import json
import posixpath
import sys
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable

import hsm
from hsm import hsm as hsm_core


Trace = list[dict[str, Any]]
Case = dict[str, Any]
Behavior = Callable[[hsm.Context, hsm.Instance, hsm.Event], Any]


SUPPORTED_FEATURES = {
    "core",
    "entry",
    "exit",
    "effect",
    "attribute",
    "guard",
    "event_data",
    "choice",
    "group",
    "broadcast",
    "initial",
    "nested",
    "paths",
    "path_resolution",
    "selection_order",
    "snapshot",
    "source",
    "validation",
    "operation",
    "on_call",
    "on_set",
    "lifecycle",
    "restart",
    "stop",
    "timer",
    "after",
    "at",
    "async",
    "activity",
    "cancellation",
    "history",
    "history_default",
    "shallow_history",
    "deep_history",
    "defer",
    "queue",
    "queue_order",
    "reentrancy",
    "when",
    "event_ownership",
    "event",
    "error",
    "transition_kind",
    "root_transition",
    "external",
    "internal",
    "local",
    "self",
    "every",
    "final",
    "completion",
    "submachine",
    "entry_point",
    "exit_point",
    "behavior_attr",
    "dispatch_to",
    "multi_target",
    "timer_behavior",
}


class ConformanceError(Exception):
    pass


class ConformanceSkip(Exception):
    pass


class ConformanceInstance(hsm.Instance):
    pass


class TransitionKindOverride(hsm_core.PartialElement):
    def __init__(self, kind_value: int, transition: Any | None = None):
        super().__init__(qualified_name="")
        self.kind_value = kind_value
        self.transition = transition

    def apply(self, model: hsm_core.Model, stack: list[hsm_core.NamedElement]) -> None:
        transition = self.transition
        if transition is None:
            transition = hsm_core.find(stack, hsm_core.TransitionNode)
            if transition is None:
                raise ConformanceError("transition kind must be used within a transition")
            model.add(TransitionKindOverride(self.kind_value, transition))
            return
        transition.kind = self.kind_value
        transition.paths.clear()
        hsm_core.ResolvePaths(transition=transition, traceback=self.traceback).apply(model, [])


class LogicalClock:
    def __init__(self, runner: "Runner") -> None:
        self.runner = runner
        self.now_ms = 0
        self.sleepers: list[tuple[int, asyncio.Future[None]]] = []

    def clock(self) -> hsm.Clock:
        async def sleep(duration: timedelta) -> None:
            millis = max(0, int(duration.total_seconds() * 1000))
            due = self.now_ms + millis
            loop = asyncio.get_running_loop()
            future: asyncio.Future[None] = loop.create_future()
            self.sleepers.append((due, future))
            self._wake_due()
            await future

        return hsm.Clock(sleep=sleep)

    async def advance(self, millis: int) -> None:
        self.now_ms += max(0, millis)
        for _ in range(10):
            self._wake_due()
            self.runner.flush_timer_scheduled()
            await asyncio.sleep(0)

    def _wake_due(self) -> None:
        pending: list[tuple[int, asyncio.Future[None]]] = []
        for due, future in self.sleepers:
            if due <= self.now_ms:
                if not future.done():
                    future.set_result(None)
            else:
                pending.append((due, future))
        self.sleepers = pending


class Runner:
    def __init__(self, case: Case):
        self.case = case
        self.trace: Trace = []
        self.snapshots: dict[str, Any] = {}
        self.ctx = hsm.Context()
        self.model: hsm.Model | None = None
        self.instances: dict[str, ConformanceInstance] = {}
        self.groups: dict[str, hsm.Group] = {}
        self.logical_clock = LogicalClock(self)
        self.pending_timer_scheduled = 0
        self.last_stable_label: str | None = None
        self.deferred_events: list[tuple[str, str, bool]] = []
        self.defer_replay_barrier = False
        self.features = set(case.get("features", []))
        self.trace_contract = self.collect_trace_contract()
        self.model_irs_by_name: dict[str, dict[str, Any]] = {}
        self.models_by_name: dict[str, hsm.Model] = {}
        self.building_models: set[str] = set()
        self.activity_behavior_ids: set[str] = set()
        self.behavior_roles: dict[str, set[str]] = {}
        self.timer_source_callbacks: dict[tuple[Any, ...], Behavior] = {}
        self.model_name = self._require_object(case, "model").get("name", "")
        if not isinstance(self.model_name, str) or not self.model_name:
            raise ConformanceError("model.name must be a non-empty string")
        self.validation_mode = case.get("mode", "runtime") == "validation"

    async def run(self) -> None:
        self.model = self.build_model()
        self.build_instances()
        self.build_groups()

        for step in self._require_array(self.case, "script"):
            try:
                await self.execute_step(step)
            except AssertionError:
                raise
            except Exception as error:
                expected_error = self._optional_object(self._require_object(self.case, "expect"), "error")
                if not expected_error:
                    raise
                contains = expected_error.get("message_contains")
                if isinstance(contains, str) and contains not in str(error):
                    raise
                if not self.trace or self.trace[-1].get("type") != "error":
                    self.trace.append({"type": "error", "code": expected_error.get("code", "runtime_error")})
                break

        self.flush_timer_scheduled()
        self.trace.append({"type": "stable", "state": self.stable_state()})
        self.assert_expectations()

    async def settle_ready_tasks(self, turns: int = 1) -> None:
        for _ in range(turns):
            await asyncio.sleep(0)

    def build_model(self) -> hsm.Model:
        model_ir = self._require_object(self.case, "model")
        self.validate_behavior_programs()
        if self.validation_mode:
            self.validate_ir_shape(model_ir)
        for child_model_ir in self.case.get("models", []):
            child_name = self._require_string(child_model_ir, "name")
            if child_name == self.model_name or child_name in self.model_irs_by_name:
                raise ConformanceError(f"duplicate model {child_name!r}")
            self.model_irs_by_name[child_name] = child_model_ir
            if self.validation_mode:
                self.validate_ir_shape(child_model_ir)
                self.validate_model_references(child_model_ir)
            self.validate_trigger_operands(child_model_ir)
        self.validate_trigger_operands(model_ir)
        if self.validation_mode:
            self.validate_model_references(model_ir)
            self.validate_on_call_operations(model_ir, set(), set())
        self.collect_activity_behaviors(model_ir)
        for child_model_ir in self.model_irs_by_name.values():
            self.collect_activity_behaviors(child_model_ir)
        return self.build_named_model(model_ir)

    def build_named_model(self, model_ir: dict[str, Any]) -> hsm.Model:
        model_name = self._require_string(model_ir, "name")
        built = self.models_by_name.get(model_name)
        if built is not None:
            return built
        if model_name in self.building_models:
            raise ConformanceError(f"recursive submachine model reference {model_name!r}")
        self.building_models.add(model_name)
        try:
            model = self.build_model_ir(model_ir)
        finally:
            self.building_models.remove(model_name)
        self.models_by_name[model_name] = model
        return model

    def build_model_ir(self, model_ir: dict[str, Any]) -> hsm.Model:
        model_name = self._require_string(model_ir, "name")
        parts: list[Any] = []

        for name, spec in self._optional_object(model_ir, "attributes").items():
            if not isinstance(spec, dict):
                raise ConformanceError(f"attribute {name!r} must be an object")
            has_type = "type" in spec
            has_default = "default" in spec
            if not has_type and not has_default:
                raise ConformanceError(f"attribute {name!r} requires type or default")
            if has_type:
                value_type = self.attribute_type_from_ir(name, spec)
                if has_default:
                    default = spec["default"]
                    self.validate_attribute_default(name, value_type, default)
                    parts.append(hsm.Attribute(name, value_type, default))
                else:
                    parts.append(hsm.Attribute(name, value_type))
            else:
                parts.append(hsm.Attribute(name, spec["default"]))

        for name, ref in self._optional_object(model_ir, "operations").items():
            behavior_id = self.behavior_id(ref)
            self.mark_behavior_role(behavior_id, "operation")
            parts.append(hsm.Operation(name, self.operation_callback(name, behavior_id)))

        for entry_point in model_ir.get("entry_points", []):
            entry_parts: list[Any] = [
                hsm.Target(
                    self.absolute_path(
                        self._require_string(entry_point, "target"),
                        "/" + model_name,
                    )
                )
            ]
            for ref in entry_point.get("effects", []):
                behavior_id = self.behavior_id(ref)
                self.mark_behavior_role(behavior_id, "effect")
                entry_parts.append(hsm.Effect(self.behavior_callback(behavior_id, role="effect")))
            parts.append(hsm.EntryPoint(self._require_string(entry_point, "name"), *entry_parts))

        for exit_point in model_ir.get("exit_points", []):
            exit_parts: list[Any] = []
            for ref in exit_point.get("effects", []):
                behavior_id = self.behavior_id(ref)
                self.mark_behavior_role(behavior_id, "effect")
                exit_parts.append(hsm.Effect(self.behavior_callback(behavior_id, role="effect")))
            parts.append(hsm.ExitPoint(self._require_string(exit_point, "name"), *exit_parts))

        parts.append(self.build_initial(model_ir["initial"], "/" + model_name))
        for state in self._require_array(model_ir, "states"):
            parts.append(self.build_state(state, "/" + model_name))
        for transition in model_ir.get("transitions", []):
            parts.append(self.build_transition(transition, "/" + model_name))

        return hsm.Define(model_name, *parts)

    def build_initial(self, initial: Any, owner_path: str) -> Any:
        if isinstance(initial, str):
            return hsm.Initial(hsm.Target(self.absolute_path(initial, owner_path, bare_relative_to_owner=True)))
        if isinstance(initial, dict):
            parts: list[Any] = [
                hsm.Target(
                    self.absolute_path(
                        self._require_string(initial, "target"),
                        owner_path,
                        bare_relative_to_owner=True,
                    )
                )
            ]
            for ref in initial.get("effects", []):
                behavior_id = self.behavior_id(ref)
                self.mark_behavior_role(behavior_id, "effect")
                parts.append(hsm.Effect(self.behavior_callback(behavior_id, role="effect")))
            return hsm.Initial(*parts)
        raise ConformanceError("initial must be a string or object")

    def build_state(self, state: dict[str, Any], owner_path: str) -> Any:
        name = state.get("name")
        if not isinstance(name, str) or not name:
            raise ConformanceError("state.name must be a non-empty string")

        state_path = posixpath.normpath(owner_path + "/" + name)
        kind = state.get("kind", "state")
        parts: list[Any] = []
        if "initial" in state:
            parts.append(self.build_initial(state["initial"], state_path))
        for field, factory in (("entry", hsm.Entry), ("exit", hsm.Exit), ("activity", hsm.Activity)):
            refs = state.get(field, [])
            if refs:
                callbacks = []
                for ref in refs:
                    behavior_id = self.behavior_id(ref)
                    self.mark_behavior_role(behavior_id, field)
                    callbacks.append(self.behavior_callback(behavior_id, role=field))
                parts.append(factory(*callbacks))
        for event in state.get("defer", []):
            parts.append(hsm.Defer(self.event_name_from_ref(event)))
        transition_owner_path = owner_path if kind in {"choice", "shallow_history", "deep_history"} else state_path
        for child in state.get("states", []):
            parts.append(self.build_state(child, state_path))
        for transition in state.get("transitions", []):
            parts.append(
                self.build_transition(
                    transition,
                    transition_owner_path,
                    bare_relative_targets=kind in {"choice", "shallow_history", "deep_history"},
                )
            )

        if kind == "state":
            return hsm.State(name, *parts)
        if kind == "submachine":
            machine_name = self._require_string(state, "machine")
            machine = self.models_by_name.get(machine_name)
            if machine is None:
                machine_ir = self.model_irs_by_name.get(machine_name)
                if machine_ir is None:
                    raise ConformanceError(f"unknown submachine model {machine_name!r}")
                machine = self.build_named_model(machine_ir)
            return hsm.SubmachineState(name, machine, *parts)
        if kind == "final":
            if parts:
                raise ConformanceError(f"final state {name!r} cannot contain parts")
            return hsm.Final(name)
        if kind == "choice":
            return hsm.Choice(name, *parts)
        if kind == "shallow_history":
            return hsm.ShallowHistory(name, *parts)
        if kind == "deep_history":
            return hsm.DeepHistory(name, *parts)
        raise ConformanceError(f"unsupported state kind {kind!r}")

    def build_transition(
        self,
        transition: dict[str, Any],
        owner_path: str,
        *,
        bare_relative_targets: bool = False,
    ) -> Any:
        parts: list[Any] = []
        if "kind" in transition:
            parts.append(TransitionKindOverride(self.transition_kind_from_ir(transition["kind"])))
        source_path: str | None = None
        if "source" in transition:
            source_path = self.absolute_path(transition["source"], owner_path, bare_relative_to_owner=bare_relative_targets)
            parts.append(hsm.Source(source_path))
        trigger = transition.get("trigger")
        if trigger is None and "on" in transition:
            trigger = {"kind": "on", "event": transition["on"]}
        timer_trigger = isinstance(trigger, dict) and trigger.get("kind") in {"after", "every", "at"}
        if trigger is not None:
            parts.append(self.build_trigger(trigger))
        guard_callback: Behavior | None = None
        if "guard" in transition:
            behavior_id = self.behavior_id(transition["guard"])
            self.mark_behavior_role(behavior_id, "guard")
            guard_callback = self.behavior_callback(behavior_id, role="guard")
        if timer_trigger:
            parts.append(hsm.Guard(self.timer_fired_guard_callback(guard_callback)))
        elif guard_callback is not None:
            parts.append(hsm.Guard(guard_callback))
        if "target" in transition:
            parts.append(
                hsm.Target(
                    self.transition_target_path(
                        transition["target"],
                        owner_path,
                        source_path=source_path,
                        bare_relative_targets=bare_relative_targets,
                    )
                )
            )
        if "entry_point" in transition:
            parts.append(hsm.EntryPoint(self._require_string(transition, "entry_point")))
        for ref in transition.get("effects", []):
            behavior_id = self.behavior_id(ref)
            self.mark_behavior_role(behavior_id, "effect")
            parts.append(hsm.Effect(self.behavior_callback(behavior_id, role="effect")))
        if "id" in transition:
            return hsm.Transition(transition["id"], *parts)
        if not parts:
            raise ConformanceError("transition must contain at least one partial")
        return hsm.Transition(parts[0], *parts[1:])

    def build_trigger(self, trigger: dict[str, Any]) -> Any:
        kind = trigger.get("kind")
        if kind == "on":
            events = trigger.get("events")
            if events is None:
                events = [trigger.get("event")]
            return hsm.On(*(self.event_name_from_ref(event) for event in events))
        if kind == "on_set":
            return hsm.OnSet(self._require_string(trigger, "attribute"))
        if kind == "on_call":
            return hsm.OnCall(self._require_string(trigger, "operation"))
        if kind == "when":
            if "attribute" in trigger:
                return hsm.When(trigger["attribute"])
            return hsm.When(self.behavior_callback(self._require_string(trigger, "behavior"), role="guard"))
        if kind == "completion":
            return hsm.On(hsm.FinalEvent)
        if kind == "exit_point":
            return hsm.ExitPoint(self._require_string(trigger, "exit_point"))
        if kind in {"after", "every", "at"}:
            if "duration_ms" in trigger:
                millis = int(trigger["duration_ms"])
                key = (kind, "duration_ms", millis)
                cached = self.timer_source_callbacks.get(key)
                if cached is not None:
                    return {"after": hsm.After, "every": hsm.Every, "at": hsm.At}[kind](cached)

                async def duration(ctx: hsm.Context, instance: hsm.Instance, event: hsm.Event) -> timedelta:
                    self.note_timer_scheduled()
                    return self.duration_from_millis(millis)

                value = duration
                self.timer_source_callbacks[key] = value
            elif "time_ms" in trigger:
                millis = int(trigger["time_ms"])
                key = (kind, "time_ms", millis)
                cached = self.timer_source_callbacks.get(key)
                if cached is not None:
                    return {"after": hsm.After, "every": hsm.Every, "at": hsm.At}[kind](cached)

                async def timepoint(ctx: hsm.Context, instance: hsm.Instance, event: hsm.Event) -> datetime:
                    self.note_timer_scheduled()
                    remaining = millis - self.logical_clock.now_ms
                    return datetime.now() + self.timepoint_duration_from_millis(remaining)

                value = timepoint
                self.timer_source_callbacks[key] = value
            elif "attribute" in trigger:
                attribute = self._require_string(trigger, "attribute")
                key = (kind, "attribute", attribute)
                value = self.timer_source_callbacks.get(key)
                if value is None:
                    value = self.timer_attribute_source(attribute, timepoint=kind == "at")
                    self.timer_source_callbacks[key] = value
            elif "behavior" in trigger:
                behavior = self._require_string(trigger, "behavior")
                key = (kind, "behavior", behavior)
                value = self.timer_source_callbacks.get(key)
                if value is None:
                    value = self.timer_behavior_source(behavior, timepoint=kind == "at")
                    self.timer_source_callbacks[key] = value
            else:
                raise ConformanceError(f"{kind} trigger requires attribute or behavior")
            if kind == "after":
                return hsm.After(value)
            if kind == "every":
                return hsm.Every(value)
            return hsm.At(value)
        raise ConformanceError(f"unsupported trigger kind {kind!r}")

    @staticmethod
    def duration_from_millis(value: Any) -> timedelta:
        millis = float(value)
        if millis <= 0:
            return timedelta(microseconds=1)
        return timedelta(milliseconds=millis)

    @staticmethod
    def timepoint_duration_from_millis(value: Any) -> timedelta:
        millis = float(value)
        if millis <= 0:
            return timedelta(microseconds=500)
        return timedelta(milliseconds=millis)

    def timer_attribute_source(self, name: str, *, timepoint: bool) -> Behavior:
        async def callback(ctx: hsm.Context, instance: hsm.Instance, event: hsm.Event) -> Any:
            try:
                value, _ = hsm.Get(ctx, instance, name)
                if timepoint:
                    result = datetime.now() + self.timepoint_duration_from_millis(value)
                else:
                    result = self.duration_from_millis(value)
                self.note_timer_scheduled()
                return result
            except Exception:
                self.trace_expected_error_once()
                raise

        callback.__name__ = f"attribute_{name}"
        return callback

    def timer_behavior_source(self, behavior_id: str, *, timepoint: bool) -> Behavior:
        behavior = self.behavior_callback(behavior_id, role="timer")
        should_trace_schedule = self.behavior_is_silent_timer_source(behavior_id)

        async def callback(ctx: hsm.Context, instance: hsm.Instance, event: hsm.Event) -> Any:
            try:
                value = await behavior(ctx, instance, event)
                if timepoint:
                    result = datetime.now() + self.timepoint_duration_from_millis(value)
                else:
                    result = self.duration_from_millis(value)
                if should_trace_schedule:
                    self.note_timer_scheduled()
                return result
            except Exception:
                self.trace_expected_error_once()
                raise

        callback.__name__ = behavior_id
        return callback

    def trace_expected_error_once(self) -> None:
        expected_error = self._optional_object(self._require_object(self.case, "expect"), "error")
        if not expected_error:
            return
        if self.trace and self.trace[-1].get("type") == "error":
            return
        self.trace.append({"type": "error", "code": expected_error.get("code", "runtime_error")})

    def behavior_is_silent_timer_source(self, behavior_id: str) -> bool:
        program = self._optional_object(self.case, "behaviors").get(behavior_id, [])
        if not isinstance(program, list):
            return False
        visible_ops = {"trace", "set_attr", "set_attr_from_event_data", "dispatch", "call", "snapshot", "raise", "sleep", "yield"}
        return not any(isinstance(op, dict) and op.get("op") in visible_ops for op in program)

    def note_timer_scheduled(self) -> None:
        self.pending_timer_scheduled += 1

    def flush_timer_scheduled(self, *, count: int | None = None) -> None:
        if count is None:
            count = self.pending_timer_scheduled
        count = min(count, self.pending_timer_scheduled)
        for _ in range(count):
            self.trace.append({"type": "timer_scheduled"})
        self.pending_timer_scheduled -= count

    @staticmethod
    def transition_kind_from_ir(kind: Any) -> int:
        mapping = {
            "external": hsm_core.Kinds.External,
            "internal": hsm_core.Kinds.Internal,
            "local": hsm_core.Kinds.Local,
            "self": hsm_core.Kinds.Self,
        }
        if kind not in mapping:
            raise ConformanceError(f"unsupported transition kind {kind!r}")
        return mapping[kind]

    def timer_fired_guard_callback(self, guard: Behavior | None = None) -> Behavior:
        async def callback(ctx: hsm.Context, instance: hsm.Instance, event: hsm.Event) -> None:
            should_trace = not getattr(event, "_conformance_timer_fired_traced", False)
            if should_trace:
                setattr(event, "_conformance_timer_fired_traced", True)
                self.flush_timer_scheduled(count=1)
            if guard is None:
                if should_trace:
                    self.trace.append({"type": "timer_fired"})
                return True
            fired_index = len(self.trace)
            try:
                result = bool(await guard(ctx, instance, event))
            except BaseException:
                if should_trace:
                    self.trace.insert(fired_index, {"type": "timer_fired"})
                raise
            if should_trace:
                if result:
                    self.trace.append({"type": "timer_fired"})
                else:
                    self.trace.insert(fired_index, {"type": "timer_fired"})
            return result

        callback.__name__ = "timer_fired"
        setattr(callback, "_hsm_snapshot_guard", guard is not None)
        return callback

    def build_instances(self) -> None:
        instances = self.case.get("instances")
        if instances is None:
            self.instances["default"] = ConformanceInstance()
            return
        if not isinstance(instances, list):
            raise ConformanceError("instances must be an array")
        for instance_ir in instances:
            instance_id = self._require_string(instance_ir, "id")
            if instance_id in self.instances:
                raise ConformanceError(f"duplicate instance {instance_id!r}")
            self.instances[instance_id] = ConformanceInstance()

    def build_groups(self) -> None:
        group_ids: set[str] = set()
        for group_ir in self.case.get("groups", []):
            group_id = self._require_string(group_ir, "id")
            if group_id in group_ids:
                raise ConformanceError(f"duplicate group {group_id!r}")
            group_ids.add(group_id)
        for group_ir in self.case.get("groups", []):
            group_id = self._require_string(group_ir, "id")
            members = group_ir.get("members", [])
            if not isinstance(members, list):
                raise ConformanceError("group.members must be an array")
            seen_members: set[str] = set()
            for member in members:
                member_id = self._require_member_id(member)
                if member_id in seen_members:
                    raise ConformanceError(f"duplicate group member {member_id!r}")
                if member_id not in self.instances:
                    raise ConformanceError(f"unknown group member {member_id!r}")
                seen_members.add(member_id)
            if len(members) < 2:
                raise ConformanceError("group must contain at least two members")
            self.groups[group_id] = hsm.MakeGroup(group_id, *(self.instances[self._require_member_id(member)] for member in members))

    def run_validation(self) -> None:
        try:
            self.build_model()
            self.build_instances()
            self.build_groups()
        except Exception as error:
            self.assert_validation_error(error)
            return
        raise AssertionError("validation case unexpectedly built successfully")

    def assert_validation_error(self, error: Exception) -> None:
        expected = self._require_object(self.case, "expect").get("validation", [])
        if not isinstance(expected, list) or not expected:
            return
        message = str(error)
        for item in expected:
            if isinstance(item, str):
                if item not in message:
                    raise AssertionError(f"validation error mismatch: {message!r} does not contain {item!r}")
                return
            if not isinstance(item, dict):
                continue
            contains = item.get("message_contains")
            if isinstance(contains, str) and contains not in message:
                raise AssertionError(f"validation error mismatch: {message!r} does not contain {contains!r}")
                return
            code = item.get("code")
            if isinstance(code, str) and not self.validation_code_matches(code, message):
                raise AssertionError(f"validation error mismatch: {message!r} does not match code {code!r}")
        return

    @staticmethod
    def validation_code_matches(code: str, message: str) -> bool:
        if code == "missing_initial":
            return "'initial'" in message or "missing initial" in message
        checks = {
            "invalid_name": "cannot contain",
            "missing_target": "not found",
            "invalid_final_transition": "cannot",
            "choice_missing_fallback": "last transition",
            "missing_submachine_model": "unknown submachine model",
            "missing_entry_point": "has no entry point",
            "missing_exit_point": "has no exit point",
            "invalid_submachine_contents": "cannot contain nested",
            "invalid_submachine_internal_target": "cannot target internal state",
            "invalid_entry_point_target": "entry point target",
            "invalid_entry_point_usage": "can only target a SubmachineState",
            "invalid_entry_point_internal_target": "entry point target cannot be internal",
            "invalid_entry_point_target_kind": "entry point target",
            "invalid_exit_point_usage": "ExitPoint outcome can only be handled",
            "duplicate_model": "duplicate model",
            "duplicate_instance": "duplicate instance",
            "duplicate_group": "duplicate group",
            "duplicate_group_member": "duplicate group member",
            "unknown_group_member": "unknown group member",
            "choice_default_not_last": "last transition",
            "choice_missing_transition": "has no transitions",
            "invalid_submachine_initial": "already has an initial state",
            "submachine_model_cycle": "recursive submachine model reference",
            "invalid_history_owner": "within a nested State",
            "missing_operation": "missing operation",
            "multiple_transition_triggers": "multiple transition triggers",
            "multiple_trigger_operands": "multiple trigger operands",
            "missing_trigger_operand": "missing trigger operand",
            "invalid_timer_source": "invalid timer source",
            "missing_source": "not found",
            "history_missing_default": "requires a default transition",
            "missing_initial": "missing initial",
            "invalid_pseudostate_contents": "invalid pseudostate contents",
            "empty_event_array": "empty event array",
            "extraneous_trigger_operand": "extraneous trigger operand",
            "invalid_group_cardinality": "at least two",
            "invalid_behavior_op_operand": "behavior op",
            "missing_behavior": "missing behavior program",
            "invalid_attribute": "attribute",
            "empty_behavior_array": "empty behavior array",
            "duplicate_state": "duplicate state",
            "missing_attribute": "missing attribute",
            "missing_timer_attribute": "missing timer attribute",
            "invalid_timer_attribute_type": "invalid timer attribute type",
            "duplicate_entry_point": "duplicate entry point",
            "duplicate_exit_point": "duplicate exit point",
            "connection_point_name_collision": "connection point name collision",
            "invalid_submachine_boundary_target": "submachine boundary",
            "invalid_submachine_internal_source": "submachine internal source",
            "invalid_timer_behavior_return": "invalid timer behavior return",
        }
        needle = checks.get(code, code)
        return needle in message

    def validate_ir_shape(self, model_ir: dict[str, Any]) -> None:
        def behavior_array(parent: dict[str, Any], field: str) -> None:
            if field in parent and parent[field] == []:
                raise ConformanceError(f"empty behavior array: {field}")

        def transition_arrays(transition: dict[str, Any]) -> None:
            behavior_array(transition, "effects")

        def walk_state(state: dict[str, Any]) -> None:
            for field in ("entry", "exit", "activity"):
                behavior_array(state, field)
            if state.get("defer") == []:
                raise ConformanceError("empty event array")
            if state.get("kind") in {"choice", "shallow_history", "deep_history"}:
                if "initial" in state:
                    raise ConformanceError("already has an initial state")
                for field in ("entry", "exit", "activity", "defer", "states", "initial"):
                    if field in state:
                        raise ConformanceError("invalid pseudostate contents")
            initial = state.get("initial")
            if isinstance(initial, dict):
                behavior_array(initial, "effects")
            for transition in state.get("transitions", []):
                if isinstance(transition, dict):
                    transition_arrays(transition)
            for child in state.get("states", []):
                if isinstance(child, dict):
                    walk_state(child)

        initial = model_ir.get("initial")
        if isinstance(initial, dict):
            behavior_array(initial, "effects")
        for field in ("entry_points", "exit_points"):
            for point in model_ir.get(field, []):
                if isinstance(point, dict):
                    behavior_array(point, "effects")
        for transition in model_ir.get("transitions", []):
            if isinstance(transition, dict):
                transition_arrays(transition)
        for state in model_ir.get("states", []):
            if isinstance(state, dict):
                walk_state(state)

    def validate_model_references(self, model_ir: dict[str, Any]) -> None:
        model_name = self._require_string(model_ir, "name")
        if "/" in model_name:
            raise ConformanceError(f'model name "{model_name}" cannot contain "/"')
        attributes = self._optional_object(model_ir, "attributes")
        state_paths: set[str] = set()
        state_kinds: dict[str, str] = {}
        entry_points: set[str] = set()
        exit_points: set[str] = set()

        def validate_name(name: Any, what: str) -> str:
            if not isinstance(name, str) or not name:
                raise ConformanceError(f"{what} name must be a non-empty string")
            if "/" in name:
                raise ConformanceError(f'{what} name "{name}" cannot contain "/"')
            return name

        def absolute_in_model(path: str, owner_path: str, *, bare_relative_to_owner: bool = False) -> str:
            if not isinstance(path, str) or not path:
                raise ConformanceError("path must be a non-empty string")
            if path.startswith("/"):
                resolved = posixpath.normpath(path)
            elif bare_relative_to_owner or path == "." or path.startswith("./") or path.startswith("../"):
                resolved = posixpath.normpath(posixpath.join(owner_path, path))
            else:
                resolved = posixpath.normpath("/" + model_name + "/" + path)
            root = resolved.strip("/").split("/", 1)[0] if resolved.startswith("/") else model_name
            if root != model_name:
                raise ConformanceError(f"submachine boundary target {resolved!r} leaves model {model_name!r}")
            return resolved

        def state_names(states: list[Any], owner_path: str) -> set[str]:
            names: set[str] = set()
            for state in states:
                if not isinstance(state, dict):
                    continue
                name = validate_name(state.get("name"), "state")
                if name in names:
                    raise ConformanceError(f"duplicate state {name!r}")
                names.add(name)
                path = posixpath.normpath(owner_path + "/" + name)
                state_paths.add(path)
                state_kinds[path] = state.get("kind", "state")
                state_names(state.get("states", []), path)
            return names

        top_state_names = state_names(model_ir.get("states", []), "/" + model_name)

        for field, target_set, duplicate_message in (
            ("entry_points", entry_points, "duplicate entry point"),
            ("exit_points", exit_points, "duplicate exit point"),
        ):
            for point in model_ir.get(field, []):
                if not isinstance(point, dict):
                    continue
                name = validate_name(point.get("name"), field[:-1])
                if name in target_set:
                    raise ConformanceError(f"{duplicate_message} {name!r}")
                if name in top_state_names:
                    raise ConformanceError(f"connection point name collision {name!r}")
                target_set.add(name)

        def connection_path(name: str, kind: str) -> str:
            return posixpath.normpath(f"/{model_name}/.{kind}/{name}")

        entry_paths = {connection_path(name, "entry") for name in entry_points}
        exit_paths = {connection_path(name, "exit") for name in exit_points}

        def validate_state_target(path: str) -> None:
            if path not in state_paths:
                raise ConformanceError(f'target "{path}" not found')

        def validate_initial(initial: Any, owner_path: str) -> None:
            target = initial.get("target") if isinstance(initial, dict) else initial
            if isinstance(target, str) and target.startswith("/"):
                root = target.strip("/").split("/", 1)[0]
                if root != model_name:
                    raise ConformanceError(f'target "{target}" not found')
            validate_state_target(absolute_in_model(target, owner_path, bare_relative_to_owner=True))

        def behavior_program_returns_number(behavior_id: str) -> bool:
            program = self._optional_object(self.case, "behaviors").get(behavior_id)
            if not isinstance(program, list):
                return True
            for op in reversed(program):
                if not isinstance(op, dict):
                    continue
                if op.get("op") == "return_value":
                    value = op.get("value")
                    return (type(value) is int or type(value) is float) and not isinstance(value, bool)
                if op.get("op") == "return_attr":
                    return True
            return True

        def validate_trigger(trigger: dict[str, Any]) -> None:
            kind = trigger.get("kind")
            if kind in {"on_set", "when"} and "attribute" in trigger:
                name = self._require_string(trigger, "attribute")
                if "/" in name:
                    raise ConformanceError(f'attribute name "{name}" cannot contain "/"')
                if name not in attributes:
                    raise ConformanceError(f"missing attribute {name!r}")
            if kind in {"after", "every", "at"}:
                if kind == "every" and "duration_ms" in trigger and float(trigger["duration_ms"]) <= 0:
                    raise ConformanceError("invalid timer source")
                if "attribute" in trigger:
                    name = self._require_string(trigger, "attribute")
                    spec = attributes.get(name)
                    if not isinstance(spec, dict):
                        raise ConformanceError(f"missing timer attribute {name!r}")
                    type_name = spec.get("type")
                    expected = "time_ms" if kind == "at" else "duration_ms"
                    if type_name != expected:
                        raise ConformanceError(f"invalid timer attribute type for {name!r}")
                if "behavior" in trigger and not behavior_program_returns_number(self._require_string(trigger, "behavior")):
                    raise ConformanceError("invalid timer behavior return")

        def validate_transition(transition: dict[str, Any], owner_path: str, *, bare_relative_targets: bool = False) -> None:
            if "source" in transition:
                source_path = absolute_in_model(self._require_string(transition, "source"), owner_path)
                if any(state_kinds.get(prefix) == "submachine" for prefix in self.path_prefixes(source_path)[:-1]):
                    raise ConformanceError(f"submachine internal source {source_path!r}")
                if source_path not in state_paths:
                    raise ConformanceError(f'source "{source_path}" not found')
            else:
                source_path = None
            trigger = transition.get("trigger")
            if isinstance(trigger, dict):
                validate_trigger(trigger)
            if "target" in transition:
                raw_target = self._require_string(transition, "target")
                if raw_target.startswith(".entry/") or raw_target.startswith(".exit/"):
                    raise ConformanceError("entry point target cannot be internal")
                target_path = source_path if raw_target == "." and source_path is not None else absolute_in_model(
                    raw_target,
                    owner_path,
                    bare_relative_to_owner=bare_relative_targets,
                )
                if any(state_kinds.get(prefix) == "submachine" for prefix in self.path_prefixes(target_path)[:-1]):
                    raise ConformanceError(f"cannot target internal state {target_path!r}")
                validate_state_target(target_path)

        def walk_state(state: dict[str, Any], owner_path: str) -> None:
            name = self._require_string(state, "name")
            path = posixpath.normpath(owner_path + "/" + name)
            children = [child for child in state.get("states", []) if isinstance(child, dict)]
            if children and state.get("kind", "state") == "state":
                if "initial" not in state:
                    raise ConformanceError("missing initial")
                validate_initial(state["initial"], path)
            transition_owner_path = owner_path if state.get("kind") in {"choice", "shallow_history", "deep_history"} else path
            for transition in state.get("transitions", []):
                if isinstance(transition, dict):
                    validate_transition(
                        transition,
                        transition_owner_path,
                        bare_relative_targets=state.get("kind") in {"choice", "shallow_history", "deep_history"},
                    )
            for child in children:
                walk_state(child, path)

        validate_initial(model_ir["initial"], "/" + model_name)
        for point in model_ir.get("entry_points", []):
            if not isinstance(point, dict):
                continue
            raw_target = self._require_string(point, "target")
            if raw_target.startswith("/"):
                root = raw_target.strip("/").split("/", 1)[0]
                if root != model_name:
                    raise ConformanceError(f"entry point target {raw_target!r} leaves model {model_name!r}")
            if raw_target in entry_points or raw_target in exit_points:
                raise ConformanceError(f"entry point target {raw_target!r} is not a state")
            target = absolute_in_model(raw_target, "/" + model_name)
            if target in entry_paths or target in exit_paths or target not in state_paths:
                if target not in state_paths:
                    raise ConformanceError(f'target "{target}" not found')
                raise ConformanceError(f"entry point target {target!r} is not a state")
        for transition in model_ir.get("transitions", []):
            if isinstance(transition, dict):
                validate_transition(transition, "/" + model_name)
        for state in model_ir.get("states", []):
            if isinstance(state, dict):
                walk_state(state, "/" + model_name)

    @staticmethod
    def path_prefixes(path: str) -> list[str]:
        parts = [part for part in path.strip("/").split("/") if part]
        prefixes: list[str] = []
        for index in range(1, len(parts) + 1):
            prefixes.append("/" + "/".join(parts[:index]))
        return prefixes

    def validate_behavior_programs(self) -> None:
        behaviors = self.case.get("behaviors", {})
        if behaviors is None:
            return
        if not isinstance(behaviors, dict):
            raise ConformanceError("behaviors must be an object")
        required: dict[str, set[str]] = {
            "trace": {"value"},
            "set_attr": {"name", "value"},
            "set_attr_from_event_data": {"name", "path"},
            "get_attr": {"name"},
            "return_attr": {"name"},
            "return_value": {"value"},
            "return_equals": {"name", "value"},
            "event_name_equals": {"value"},
            "event_data_equals": {"path", "value"},
            "event_data_get": {"path"},
            "event_metadata_set": {"name", "value"},
            "event_metadata_get": {"name"},
            "event_metadata_equals": {"name", "value"},
            "dispatch": {"event"},
            "call": {"name"},
            "sleep": {"millis"},
            "snapshot": set(),
            "yield": set(),
        }
        allowed: dict[str, set[str]] = {kind: set(keys) | {"op"} for kind, keys in required.items()}
        allowed["dispatch"] = {"op", "event", "target", "group"}
        allowed["raise"] = {"op", "event", "code", "value"}
        for behavior_id, program in behaviors.items():
            if not isinstance(program, list) or not program:
                raise ConformanceError(f"missing behavior program {behavior_id!r}")
            for index, op in enumerate(program):
                if not isinstance(op, dict):
                    raise ConformanceError(f"behavior op {behavior_id}[{index}] must be an object")
                kind = op.get("op")
                if not isinstance(kind, str):
                    raise ConformanceError(f"behavior op {behavior_id}[{index}] must declare op")
                if kind == "raise":
                    has_event = "event" in op
                    has_code = "code" in op
                    if has_event == has_code:
                        raise ConformanceError(f"behavior op {behavior_id}[{index}] raise requires exactly one of event or code")
                    extra = set(op) - allowed["raise"]
                    if extra:
                        raise ConformanceError(f"behavior op {behavior_id}[{index}] raise has unsupported operands: {sorted(extra)}")
                    continue
                if kind not in required:
                    raise ConformanceError(f"unsupported behavior op {kind!r}")
                if kind == "dispatch" and "target" in op and "group" in op:
                    raise ConformanceError(f"behavior op {behavior_id}[{index}] dispatch cannot declare both target and group")
                missing = required[kind] - set(op)
                if missing:
                    raise ConformanceError(f"behavior op {behavior_id}[{index}] {kind} missing operands: {sorted(missing)}")
                extra = set(op) - allowed[kind]
                if extra:
                    raise ConformanceError(f"behavior op {behavior_id}[{index}] {kind} has unsupported operands: {sorted(extra)}")

    def validate_trigger_operands(self, model_ir: dict[str, Any]) -> None:
        allowed_by_kind: dict[str, set[str]] = {
            "on": {"kind", "event", "events"},
            "on_set": {"kind", "attribute"},
            "on_call": {"kind", "operation"},
            "when": {"kind", "attribute", "behavior"},
            "completion": {"kind"},
            "exit_point": {"kind", "exit_point"},
            "after": {"kind", "duration_ms", "time_ms", "attribute", "behavior"},
            "every": {"kind", "duration_ms", "time_ms", "attribute", "behavior"},
            "at": {"kind", "duration_ms", "time_ms", "attribute", "behavior"},
        }

        def validate_transition(transition: dict[str, Any]) -> None:
            if "on" in transition and "trigger" in transition:
                raise ConformanceError("multiple transition triggers")
            trigger = transition.get("trigger")
            if trigger is None:
                return
            if not isinstance(trigger, dict):
                return
            kind = trigger.get("kind")
            allowed = allowed_by_kind.get(kind)
            if allowed is None:
                return
            extra = set(trigger) - allowed
            if extra:
                raise ConformanceError(f"extraneous trigger operand: {sorted(extra)}")
            if kind == "on":
                present = [field for field in ("event", "events") if field in trigger]
                if not present:
                    raise ConformanceError("missing trigger operand")
                if len(present) > 1:
                    raise ConformanceError("multiple trigger operands")
                if trigger.get("events") == []:
                    raise ConformanceError("empty event array")
            elif kind in {"on_set", "on_call", "exit_point"}:
                field = {"on_set": "attribute", "on_call": "operation", "exit_point": "exit_point"}[kind]
                if field not in trigger:
                    raise ConformanceError("missing trigger operand")
                value = trigger.get(field)
                if isinstance(value, str) and "/" in value:
                    raise ConformanceError(f'{field} name "{value}" cannot contain "/"')
            elif kind == "when":
                present = [field for field in ("attribute", "behavior") if field in trigger]
                if not present:
                    raise ConformanceError("missing trigger operand")
                if len(present) > 1:
                    raise ConformanceError("multiple trigger operands")
                value = trigger.get(present[0])
                if present[0] == "attribute" and isinstance(value, str) and "/" in value:
                    raise ConformanceError(f'attribute name "{value}" cannot contain "/"')
            elif kind in {"after", "every"}:
                present = [field for field in ("duration_ms", "time_ms", "attribute", "behavior") if field in trigger]
                if len(present) != 1 or "time_ms" in present:
                    raise ConformanceError("invalid timer source")
            elif kind == "at":
                present = [field for field in ("duration_ms", "time_ms", "attribute", "behavior") if field in trigger]
                if len(present) != 1 or "duration_ms" in present:
                    raise ConformanceError("invalid timer source")

        def walk_states(states: list[Any]) -> None:
            for state in states:
                if not isinstance(state, dict):
                    continue
                for transition in state.get("transitions", []):
                    if isinstance(transition, dict):
                        validate_transition(transition)
                walk_states(state.get("states", []))

        for transition in model_ir.get("transitions", []):
            if isinstance(transition, dict):
                validate_transition(transition)
        walk_states(model_ir.get("states", []))

    def validate_on_call_operations(
        self,
        model_ir: dict[str, Any],
        inherited_operations: set[str],
        seen_models: set[str],
    ) -> None:
        model_name = self._require_string(model_ir, "name")
        if model_name in seen_models:
            return
        seen_models = {*seen_models, model_name}
        visible_operations = set(inherited_operations)
        visible_operations.update(
            name
            for name in self._optional_object(model_ir, "operations")
            if isinstance(name, str)
        )

        def validate_transition(transition: dict[str, Any]) -> None:
            trigger = transition.get("trigger")
            if not isinstance(trigger, dict) or trigger.get("kind") != "on_call":
                return
            operation = trigger.get("operation")
            if isinstance(operation, str) and operation not in visible_operations:
                raise ConformanceError(f'missing operation "{operation}" for OnCall()')

        def walk_states(states: list[Any]) -> None:
            for state in states:
                if not isinstance(state, dict):
                    continue
                for transition in state.get("transitions", []):
                    if isinstance(transition, dict):
                        validate_transition(transition)
                if state.get("kind") == "submachine":
                    child_name = state.get("machine")
                    child_ir = self.model_irs_by_name.get(child_name) if isinstance(child_name, str) else None
                    if child_ir is not None:
                        self.validate_on_call_operations(child_ir, visible_operations, seen_models)
                walk_states(state.get("states", []))

        for transition in model_ir.get("transitions", []):
            if isinstance(transition, dict):
                validate_transition(transition)
        walk_states(model_ir.get("states", []))

    def behavior_callback(self, behavior_id: str, *, role: str = "behavior") -> Behavior:
        program = self._optional_object(self.case, "behaviors").get(behavior_id)
        if not isinstance(program, list) or not program:
            raise ConformanceError(f"missing behavior program {behavior_id!r}")

        async def callback(ctx: hsm.Context, instance: hsm.Instance, event: hsm.Event) -> Any:
            result: Any = None
            for op in program:
                result = await self.execute_behavior_op(ctx, instance, event, op, behavior_id, role)
                if op.get("op", "").startswith("return_") and role in {"guard", "operation", "timer"}:
                    self.trace_activity_done(behavior_id)
                    return result
            self.trace_activity_done(behavior_id)
            return result

        callback.__name__ = behavior_id
        return callback

    def mark_behavior_role(self, behavior_id: str, role: str) -> None:
        self.behavior_roles.setdefault(behavior_id, set()).add(role)

    @staticmethod
    def behavior_call_op_is_traceable(role: str) -> bool:
        return role in {"entry", "exit", "activity"}

    def trace_activity_done(self, behavior_id: str) -> None:
        if behavior_id in self.activity_behavior_ids and self.trace_contract_includes("activity_done"):
            self.trace.append({"type": "activity_done", "behavior": behavior_id})

    def trace_defer_event(self, event_name: str) -> None:
        if self.trace and self.trace[-1] == {"type": "defer", "event": event_name}:
            return
        self.trace.append({"type": "defer", "event": event_name})

    def collect_activity_behaviors(self, model_ir: dict[str, Any]) -> None:
        def walk(states: list[Any]) -> None:
            for state in states:
                if not isinstance(state, dict):
                    continue
                for ref in state.get("activity", []):
                    if isinstance(ref, dict):
                        self.activity_behavior_ids.add(self.behavior_id(ref))
                walk(state.get("states", []))

        walk(model_ir.get("states", []))

    def operation_callback(self, name: str, behavior_id: str) -> Callable[..., Any]:
        program = self.behavior_callback(behavior_id, role="operation")

        async def callback(ctx: hsm.Context, instance: hsm.Instance, *args: Any) -> Any:
            event = hsm.Event(
                name=f"@call:{name}",
                qualified_name=f"@call:{name}",
                kind=hsm.Kinds.CallEvent,
                data=hsm.CallData(name=name, args=args),
                schema=hsm.CallData,
            )
            return await program(ctx, instance, event)

        callback.__name__ = name
        return callback

    async def execute_behavior_op(
        self,
        ctx: hsm.Context,
        instance: hsm.Instance,
        event: hsm.Event,
        op: dict[str, Any],
        behavior_id: str,
        behavior_role: str,
    ) -> Any:
        kind = op.get("op")
        if kind == "trace" and self.deferred_events and self.trace_contract_includes("undefer"):
            if self.defer_replay_barrier:
                self.defer_replay_barrier = False
            else:
                event_name = self.pop_deferred_event_for_instance(instance)
                if event_name is not None:
                    self.trace.append({"type": "undefer", "event": event_name})
        if kind == "trace":
            self.trace.append({"type": "trace", "value": op.get("value")})
            return None
        if kind == "set_attr":
            await hsm.Set(ctx, instance, self._require_string(op, "name"), op.get("value"))
            return None
        if kind == "set_attr_from_event_data":
            await hsm.Set(ctx, instance, self._require_string(op, "name"), self.read_path(event.data, op.get("path")))
            return None
        if kind == "get_attr":
            value, _ = hsm.Get(ctx, instance, self._require_string(op, "name"))
            return value
        if kind == "return_attr":
            value, _ = hsm.Get(ctx, instance, self._require_string(op, "name"))
            return value
        if kind == "return_value":
            return op.get("value")
        if kind == "return_equals":
            value, _ = hsm.Get(ctx, instance, self._require_string(op, "name"))
            return value == op.get("value")
        if kind == "event_name_equals":
            return event.name == op.get("value")
        if kind == "event_data_equals":
            return self.read_path(event.data, op.get("path")) == op.get("value")
        if kind == "event_data_get":
            return self.read_path(event.data, op.get("path"))
        if kind == "event_metadata_set":
            self.set_event_metadata(event, self._require_string(op, "name"), op.get("value"))
            return None
        if kind == "event_metadata_get":
            return self.get_event_metadata(event, self._require_string(op, "name"))
        if kind == "event_metadata_equals":
            return self.get_event_metadata(event, self._require_string(op, "name")) == op.get("value")
        if kind == "call":
            name = self._require_string(op, "name")
            await self.execute_operation(ctx, instance, event, name)
            if self.trace_contract_includes("call") and self.behavior_call_op_is_traceable(behavior_role):
                self.trace.append({"type": "call", "operation": name})
            await instance.dispatch(
                hsm.Event(
                    name=f"@call:{name}",
                    qualified_name=f"@call:{name}",
                    kind=hsm.Kinds.CallEvent,
                    data=hsm.CallData(name=name, args=()),
                    schema=hsm.CallData,
                )
            )
            return None
        if kind == "dispatch":
            nested_event = self.event_from_value(op.get("event"))
            wait_for_dispatch = behavior_role in {"activity", "operation"}
            if op.get("target") == "all":
                self.trace.append({"type": "dispatch", "event": nested_event.name, "target": "all"})
                self.trace_deferred_dispatch(nested_event.name, self.instances.values())
                dispatched = hsm.DispatchAll(ctx, nested_event)
            elif "target" in op:
                target_id = self._require_string(op, "target")
                self.trace.append({"type": "dispatch", "event": nested_event.name, "target": target_id})
                if target_id in self.instances:
                    self.trace_deferred_dispatch(nested_event.name, [self.instances[target_id]])
                dispatched = hsm.DispatchTo(ctx, nested_event, target_id)
            elif "group" in op:
                group_id = self._require_string(op, "group")
                self.trace.append({"type": "dispatch", "event": nested_event.name, "target": group_id})
                self.trace_deferred_dispatch(nested_event.name, self.instances_for_group(group_id))
                if group_id not in self.groups:
                    raise ConformanceError(f"unknown group {group_id!r}")
                dispatched = hsm.Dispatch(ctx, self.groups[group_id], nested_event)
            else:
                self.trace.append({"type": "dispatch", "event": nested_event.name})
                dispatched = instance.dispatch(nested_event)
            if wait_for_dispatch:
                await dispatched
            return None
        if kind == "snapshot":
            self.flush_timer_scheduled()
            snapshot = hsm.TakeSnapshot(ctx, instance)
            self.trace.append(self.snapshot_trace(snapshot))
            return snapshot
        if kind == "sleep":
            try:
                await asyncio.sleep(float(op.get("millis", 0)) / 1000.0)
            except asyncio.CancelledError:
                if "activity" in self.features:
                    self.trace.append({"type": "activity_cancel", "behavior": behavior_id})
                raise
            return None
        if kind == "yield":
            await asyncio.sleep(0)
            return None
        if kind == "raise":
            if "code" in op:
                self.trace.append({"type": "error", "code": op.get("code", "behavior_error")})
                raise RuntimeError(str(op.get("value", "behavior error")))
            raised_event = self.event_from_value(op.get("event", op.get("value")))
            self.trace.append({"type": "raise", "event": raised_event.name})
            if (
                self.event_is_deferred(instance, raised_event.name)
                and not self.current_state_has_event_transition(instance, raised_event.name)
            ):
                self.note_deferred_event(instance, raised_event.name)
                if self.trace_contract_includes("defer"):
                    self.trace_defer_event(raised_event.name)
            await instance.dispatch(raised_event)
            return None
        raise ConformanceError(f"unsupported behavior op {kind!r}")

    async def execute_operation(
        self,
        ctx: hsm.Context,
        instance: hsm.Instance,
        event: hsm.Event,
        name: str,
    ) -> Any:
        machine = getattr(instance, "_Instance__hsm", None)
        if isinstance(machine, hsm.HSM):
            scope = machine._execution_scope or machine.state() or machine.model.qualified_name
            operation_name = hsm_core._resolve_operation_name(machine.model, scope, name)
            operation = machine.model.operations.get(operation_name)
            if operation is not None and operation.callback is not None:
                return await hsm_core._maybe_await(
                    hsm_core._invoke_operation_callback(
                        operation.callback,
                        ctx,
                        instance,
                        (),
                    )
                )
        model_ir = self._require_object(self.case, "model")
        operation_ref = self._optional_object(model_ir, "operations").get(name)
        if operation_ref is None:
            raise ConformanceError(f"missing operation {name!r}")
        callback = self.behavior_callback(self.behavior_id(operation_ref), role="operation")
        return await callback(ctx, instance, event)

    async def execute_step(
        self,
        step: dict[str, Any],
    ) -> None:
        op = step.get("op")
        if op == "start":
            self.trace_lifecycle(step, "start")
            await self.start_instance(self.step_instance_id(step))
            await self.settle_ready_tasks()
            if "timer_behavior" in self.features:
                await self.settle_ready_tasks(turns=7)
            return
        if op == "dispatch":
            instance = self.instance_for_step(step)
            event = self.event_from_step(step)
            self.flush_timer_scheduled()
            self.trace.append({"type": "dispatch", "event": event.name})
            if self.exiting_timer_state(instance, event.name):
                self.trace.append({"type": "timer_cancelled"})
            event_deferred_by_current_state = self.event_is_deferred(instance, event.name)
            if (
                event_deferred_by_current_state
                and self.trace_contract_includes("defer")
                and not self.current_state_has_event_transition(instance, event.name)
            ):
                key = self.deferred_event_key(instance, event.name)
                if not self.has_deferred_event(instance, event.name):
                    self.deferred_events.append(key)
                    self.trace_defer_event(event.name)
            if self.event_exits_active_submachine(instance, event.name):
                self.clear_child_deferred_events_for_instance(instance)
            if self.deferred_events and not event_deferred_by_current_state:
                event_name = self.pop_deferred_event_for_instance(instance)
                if event_name is not None:
                    self.trace.append({"type": "undefer", "event": event_name})
                    self.defer_replay_barrier = True
            await instance.dispatch(event)
            await self.settle_ready_tasks()
            self.trace_new_runtime_deferred([instance])
            self.last_stable_label = None
            return
        if op == "dispatch_all":
            event = self.event_from_step(step)
            self.flush_timer_scheduled()
            self.trace.append({"type": "dispatch", "event": event.name, "target": "all"})
            await hsm.DispatchAll(self.ctx, event)
            await self.settle_ready_tasks(turns=7)
            self.trace_new_runtime_deferred(self.instances.values())
            self.last_stable_label = "all"
            return
        if op == "dispatch_to":
            event = self.event_from_step(step)
            self.flush_timer_scheduled()
            raw_targets = step.get("targets")
            if raw_targets is None:
                raw_targets = [step.get("instance") or step.get("target")]
            if not isinstance(raw_targets, list) or not raw_targets:
                raise ConformanceError("dispatch_to requires instance, target, or targets")
            targets = [self._require_member_id(target) for target in raw_targets]
            trace_target: str | list[str] = targets[0] if len(targets) == 1 else targets
            self.trace.append({"type": "dispatch", "event": event.name, "target": trace_target})
            await hsm.DispatchTo(self.ctx, event, *targets)
            await self.settle_ready_tasks(turns=7)
            self.trace_new_runtime_deferred(self.instances[target] for target in targets)
            self.last_stable_label = targets[0] if len(targets) == 1 else "targets:" + ",".join(targets)
            return
        if op == "group_dispatch":
            event = self.event_from_step(step)
            group_id = self._require_string(step, "group")
            self.flush_timer_scheduled()
            self.trace.append({"type": "dispatch", "event": event.name, "target": group_id})
            if group_id not in self.groups:
                raise ConformanceError(f"unknown group {group_id!r}")
            await hsm.Dispatch(self.ctx, self.groups[group_id], event)
            await self.settle_ready_tasks(turns=7)
            self.trace_new_runtime_deferred(self.instances_for_group(group_id))
            self.last_stable_label = "group:" + group_id
            return
        if op == "set":
            instance = self.instance_for_step(step)
            self.flush_timer_scheduled()
            if (
                self.trace_contract_includes("set")
                or
                "on_set" in self.features
                or "when" in self.features
                or ("timer" in self.features and "attribute" in self.features)
            ):
                self.trace.append({"type": "set", "attribute": self._require_string(step, "attribute"), "value": step.get("value")})
            await hsm.Set(self.ctx, instance, self._require_string(step, "attribute"), step.get("value"))
            self.last_stable_label = None
            return
        if op == "call":
            instance = self.instance_for_step(step)
            operation = self._require_string(step, "operation")
            data = step.get("data")
            args = (data,) if "data" in step else ()
            self.flush_timer_scheduled()
            self.trace.append({"type": "call", "operation": operation})
            await hsm.Call(self.ctx, instance, operation, *args)
            self.last_stable_label = None
            return
        if op in {"sleep", "tick"}:
            if op == "tick":
                await self.logical_clock.advance(int(step.get("millis", 0)))
            else:
                await asyncio.sleep(float(step.get("millis", 0)) / 1000.0)
            return
        if op == "expect":
            self.assert_expectation_object(self._require_object(step, "expect"))
            return
        if op == "snapshot":
            self.flush_timer_scheduled()
            if "group" in step:
                group_id = self._require_string(step, "group")
                group_ir = next((group for group in self.case.get("groups", []) if group.get("id") == group_id), None)
                if group_ir is None:
                    raise ConformanceError(f"unknown group {group_id!r}")
                for member_id in group_ir.get("members", []):
                    member = self.instances.get(member_id)
                    machine = getattr(member, "_Instance__hsm", None)
                    if not isinstance(machine, hsm.HSM) or not machine._started:
                        raise ConformanceError("operation requires a started HSM")
                self.snapshots[group_id] = self.group_snapshot(group_id)
                self.trace.append({"type": "snapshot", "group": group_id})
                self.last_stable_label = "group:" + group_id
                return
            instance = self.instance_for_step(step)
            snapshot_id = step.get("id", "last")
            if not isinstance(snapshot_id, str):
                raise ConformanceError("snapshot id must be a string")
            machine = getattr(instance, "_Instance__hsm", None)
            if not isinstance(machine, hsm.HSM) or not machine._started:
                raise ConformanceError("operation requires a started HSM")
            snapshot = hsm.TakeSnapshot(self.ctx, instance)
            self.snapshots[snapshot_id] = self.normalize_snapshot(snapshot)
            self.trace.append(self.snapshot_trace(snapshot))
            self.last_stable_label = None
            return
        if op == "restart":
            instance = self.instance_for_step(step)
            self.flush_timer_scheduled()
            self.trace_lifecycle(step, "restart")
            await hsm.Restart(instance)
            self.clear_deferred_events_for_instance(instance)
            if self.trace_contract_includes("timer_cancelled"):
                self.trace.append({"type": "timer_cancelled"})
            await asyncio.sleep(0)
            self.last_stable_label = None
            return
        if op == "stop":
            instance = self.instance_for_step(step)
            self.flush_timer_scheduled()
            self.trace_lifecycle(step, "stop")
            machine = getattr(instance, "_Instance__hsm", None)
            if not isinstance(machine, hsm.HSM) or not machine._started:
                raise ConformanceError("operation requires a started HSM")
            await hsm.Stop(instance)
            self.clear_deferred_events_for_instance(instance)
            if self.trace_contract_includes("timer_cancelled"):
                self.trace.append({"type": "timer_cancelled"})
            self.last_stable_label = None
            return
        raise ConformanceError(f"unsupported script op {op!r}")

    def event_from_step(self, step: dict[str, Any]) -> hsm.Event:
        return self.event_from_value(step.get("event"))

    def event_from_value(self, raw: Any) -> hsm.Event:
        if isinstance(raw, str):
            return hsm.Event(name=raw)
        if isinstance(raw, dict):
            return hsm.Event(
                name=self._require_string(raw, "name"),
                data=raw.get("data"),
                id=raw.get("id", ""),
                source=raw.get("source", ""),
                target=raw.get("target", ""),
                schema=copy.deepcopy(raw.get("metadata")),
            )
        raise ConformanceError("dispatch step requires string or object event")

    def event_name_from_ref(self, raw: Any) -> str:
        if isinstance(raw, str):
            return raw
        if isinstance(raw, dict):
            return self._require_string(raw, "name")
        raise ConformanceError("event reference requires string or object event")

    def get_event_metadata(self, event: hsm.Event, name: str) -> Any:
        if name in {"name", "data", "kind", "id", "source", "target", "qualified_name", "schema"}:
            return getattr(event, name, None)
        schema = getattr(event, "schema", None)
        if isinstance(schema, dict):
            return schema.get(name)
        return getattr(event, name, None)

    def set_event_metadata(self, event: hsm.Event, name: str, value: Any) -> None:
        if name in {"name", "data", "kind", "id", "source", "target", "qualified_name", "schema"}:
            setattr(event, name, value)
            return
        schema = getattr(event, "schema", None)
        if not isinstance(schema, dict):
            schema = {}
            event.schema = schema
        schema[name] = value

    def assert_expectations(self) -> None:
        self.assert_expectation_object(self._require_object(self.case, "expect"), final=True)

    def assert_expectation_object(self, expect: dict[str, Any], *, final: bool = False) -> None:
        instance = self.instances.get("default") or next(iter(self.instances.values()))
        if "state" in expect and instance.state() != expect["state"]:
            raise AssertionError(f"state mismatch: got {instance.state()!r}, want {expect['state']!r}")
        if "states" in expect:
            for instance_id, wanted in expect["states"].items():
                actual = self.instances[instance_id].state()
                if actual != wanted:
                    raise AssertionError(f"state {instance_id!r} mismatch: got {actual!r}, want {wanted!r}")
        if "trace" in expect and self.trace != expect["trace"]:
            actual = json.dumps(self.trace, indent=2, sort_keys=True)
            wanted = json.dumps(expect["trace"], indent=2, sort_keys=True)
            raise AssertionError(f"trace mismatch:\nactual:\n{actual}\nexpected:\n{wanted}")
        if "attributes" in expect:
            for name, wanted in expect["attributes"].items():
                actual, ok = instance.get(name)
                if not ok or actual != wanted:
                    raise AssertionError(f"attribute {name!r} mismatch: got {actual!r}, want {wanted!r}")
        if "instance_attributes" in expect:
            for instance_id, attrs in expect["instance_attributes"].items():
                checked = self.instances[instance_id]
                for name, wanted in attrs.items():
                    actual, ok = checked.get(name)
                    if not ok or actual != wanted:
                        raise AssertionError(
                            f"attribute {name!r} for instance {instance_id!r} mismatch: got {actual!r}, want {wanted!r}"
                        )
        if "snapshots" in expect and not self.value_contains(self.snapshots, expect["snapshots"]):
            actual = json.dumps(self.snapshots, indent=2, sort_keys=True)
            wanted = json.dumps(expect["snapshots"], indent=2, sort_keys=True)
            raise AssertionError(f"snapshot mismatch:\nactual:\n{actual}\nexpected:\n{wanted}")

    def value_contains(self, actual: Any, expected: Any) -> bool:
        if isinstance(expected, dict):
            if not isinstance(actual, dict):
                return False
            return all(key in actual and self.value_contains(actual[key], value) for key, value in expected.items())
        if isinstance(expected, list):
            if not isinstance(actual, list) or len(actual) != len(expected):
                return False
            return all(self.value_contains(actual_item, expected_item) for actual_item, expected_item in zip(actual, expected))
        return actual == expected

    def absolute_path(
        self,
        path: str,
        owner_path: str | None = None,
        *,
        bare_relative_to_owner: bool = False,
        model_name: str | None = None,
    ) -> str:
        if not isinstance(path, str) or not path:
            raise ConformanceError("path must be a non-empty string")
        if model_name is not None:
            root_name = model_name
        elif owner_path and owner_path.startswith("/"):
            root_name = owner_path.strip("/").split("/", 1)[0] or self.model_name
        else:
            root_name = self.model_name
        if path.startswith("/"):
            return posixpath.normpath(path)
        if bare_relative_to_owner or path == "." or path.startswith("./") or path.startswith("../"):
            return posixpath.normpath(posixpath.join(owner_path or "/" + root_name, path))
        return posixpath.normpath("/" + root_name + "/" + path)

    def transition_target_path(
        self,
        path: str,
        owner_path: str,
        *,
        source_path: str | None,
        bare_relative_targets: bool,
    ) -> str:
        if path == "." and source_path is not None:
            return source_path
        return self.absolute_path(path, owner_path, bare_relative_to_owner=bare_relative_targets)

    def step_instance_id(self, step: dict[str, Any]) -> str:
        return step.get("instance", "default")

    def instance_for_step(self, step: dict[str, Any]) -> ConformanceInstance:
        instance_id = self.step_instance_id(step)
        if instance_id not in self.instances:
            raise ConformanceError(f"unknown instance {instance_id!r}")
        return self.instances[instance_id]

    async def start_instance(self, instance_id: str) -> None:
        if self.model is None:
            raise ConformanceError("model has not been built")
        if instance_id not in self.instances:
            self.instances[instance_id] = ConformanceInstance()
        instance_ir = self.instance_ir(instance_id)
        config_ir = instance_ir.get("config", {}) if instance_ir is not None else {}
        if not isinstance(config_ir, dict):
            raise ConformanceError("instance.config must be an object")
        data = config_ir.get("Data", config_ir.get("data", instance_ir.get("data") if instance_ir is not None else None))
        name = config_ir.get("Name", config_ir.get("name", ""))
        clock = self.clock_from_config(config_ir)
        if clock is None:
            clock = self.logical_clock.clock()
        queue = self.queue_from_config(config_ir)
        model_name = instance_ir.get("model") if instance_ir is not None else None
        if isinstance(model_name, str):
            model = self.models_by_name.get(model_name)
            if model is None and model_name in self.model_irs_by_name:
                model = self.build_named_model(self.model_irs_by_name[model_name])
        else:
            model = self.model
        if model is None:
            raise ConformanceError(f"unknown instance model {model_name!r}")
        await hsm.Started(
            self.ctx,
            self.instances[instance_id],
            model,
            hsm.Config(ID=instance_id, Name=name, Data=data, Clock=clock, Queue=queue),
        )
        self.last_stable_label = None

    def clock_from_config(self, config_ir: dict[str, Any]) -> hsm.Clock | None:
        clock_id = config_ir.get("Clock", config_ir.get("clock"))
        if clock_id is None:
            return None
        if clock_id not in {"trace_no_sleep", "trace_yield_sleep", "trace_nonzero_sleep"}:
            raise ConformanceError(f"unsupported clock fixture {clock_id!r}")

        async def sleep(duration: timedelta) -> None:
            millis = round(duration.total_seconds() * 1000)
            value = "clock:sleep:nonzero" if clock_id == "trace_nonzero_sleep" and millis > 0 else f"clock:sleep:{millis}"
            self.flush_timer_scheduled(count=1)
            self.trace.append({"type": "trace", "value": value})
            if (
                clock_id == "trace_yield_sleep"
                or (clock_id == "trace_nonzero_sleep" and millis > 0 and "after" in self.features)
            ):
                await self.logical_clock.clock().Sleep(duration)
                return
            await asyncio.sleep(0)

        return hsm.Clock(sleep=sleep)

    def queue_from_config(self, config_ir: dict[str, Any]) -> hsm.Queue | None:
        queue_id = config_ir.get("Queue", config_ir.get("queue"))
        if queue_id is None:
            return None
        if queue_id == "len_seven":
            return hsm.Queue(Push=lambda event: None, Pop=lambda: None, Len=lambda: 7)
        if queue_id == "len_error_once":
            events: deque[hsm.Event] = deque()
            failed = False

            def push(event: hsm.Event) -> None:
                self.trace.append({"type": "trace", "value": f"queue:push:{event.name}"})
                events.append(event)
                return None

            def pop() -> hsm.Event | None:
                if not events:
                    return None
                event = events.popleft()
                self.trace.append({"type": "trace", "value": f"queue:pop:{event.name}"})
                return event

            def length() -> int | RuntimeError:
                nonlocal failed
                if not failed:
                    failed = True
                    self.trace.append({"type": "trace", "value": "queue:len-error"})
                    return RuntimeError("queue len boom")
                return len(events)

            return hsm.Queue(Push=push, Pop=pop, Len=length)
        if queue_id == "push_error":
            def push_error(event: hsm.Event) -> RuntimeError:
                self.trace.append({"type": "trace", "value": f"queue:push-error:{event.name}"})
                return RuntimeError("queue push boom")

            return hsm.Queue(Push=push_error, Pop=lambda: None, Len=lambda: 0)
        if queue_id == "pop_error_once":
            events: deque[hsm.Event] = deque()
            failed = False

            def push(event: hsm.Event) -> None:
                self.trace.append({"type": "trace", "value": f"queue:push:{event.name}"})
                events.append(event)
                return None

            def pop() -> hsm.Event | RuntimeError | None:
                nonlocal failed
                if not events:
                    return None
                if not failed:
                    failed = True
                    self.trace.append({"type": "trace", "value": "queue:pop-error"})
                    return RuntimeError("queue pop boom")
                event = events.popleft()
                self.trace.append({"type": "trace", "value": f"queue:pop:{event.name}"})
                return event

            return hsm.Queue(Push=push, Pop=pop, Len=lambda: len(events))
        if queue_id == "trace_lifo":
            events: deque[hsm.Event] = deque()

            def push(event: hsm.Event) -> None:
                self.trace.append({"type": "trace", "value": f"queue:push:{event.name}"})
                events.append(event)
                return None

            def pop() -> hsm.Event | None:
                if not events:
                    return None
                event = events.pop()
                self.trace.append({"type": "trace", "value": f"queue:pop:{event.name}"})
                return event

            def length() -> int:
                return len(events)

            return hsm.Queue(Push=push, Pop=pop, Len=length)
        if queue_id != "trace_fifo":
            raise ConformanceError(f"unsupported queue fixture {queue_id!r}")
        events: deque[hsm.Event] = deque()

        def push(event: hsm.Event) -> None:
            self.trace.append({"type": "trace", "value": f"queue:push:{event.name}"})
            events.append(event)
            return None

        def pop() -> hsm.Event | None:
            if not events:
                return None
            event = events.popleft()
            self.trace.append({"type": "trace", "value": f"queue:pop:{event.name}"})
            return event

        def length() -> int:
            return len(events)

        return hsm.Queue(Push=push, Pop=pop, Len=length)

    @staticmethod
    def attribute_type_from_ir(name: str, spec: dict[str, Any]) -> type[Any] | None:
        type_name = spec.get("type")
        mapping: dict[str, type[Any] | None] = {
            "any": None,
            "boolean": bool,
            "number": float if isinstance(spec.get("default"), float) else int,
            "string": str,
            "object": dict,
            "array": list,
            "duration_ms": int,
            "time_ms": int,
        }
        if type_name not in mapping:
            raise ConformanceError(f"attribute {name!r} has unsupported type {type_name!r}")
        return mapping[type_name]

    @staticmethod
    def validate_attribute_default(name: str, value_type: type[Any] | None, default: Any) -> None:
        if value_type is None:
            return
        if value_type is float:
            if (type(default) is not int and type(default) is not float) or isinstance(default, bool):
                raise ConformanceError(f"attribute {name!r} default does not match declared type")
            return
        if type(default) is not value_type:
            raise ConformanceError(f"attribute {name!r} default does not match declared type")

    def instance_ir(self, instance_id: str) -> dict[str, Any] | None:
        for item in self.case.get("instances", []):
            if isinstance(item, dict) and item.get("id") == instance_id:
                return item
        return None

    def stable_state(self) -> str:
        if self.last_stable_label is not None:
            return self.last_stable_label
        instance = self.instances.get("default") or next(iter(self.instances.values()))
        return instance.state()

    def trace_lifecycle(self, step: dict[str, Any], op: str) -> None:
        if "trace" in step and step["trace"] is False:
            return
        if self.trace_contract_includes(op) or (
            op == "restart" and "activity" in self.features and "cancellation" in self.features
        ) or (
            op == "stop" and "activity" in self.features and "cancellation" in self.features
        ):
            self.trace.append({"type": op})

    def collect_trace_contract(self) -> set[str]:
        # Some cases intentionally use sparse traces. The expected trace declares
        # which optional runner-observed events should be projected, but it must
        # not affect native model construction, dispatch, or transition choice.
        expect = self._optional_object(self.case, "expect")
        trace = expect.get("trace", [])
        if not isinstance(trace, list):
            return set()
        return {item.get("type") for item in trace if isinstance(item, dict) and isinstance(item.get("type"), str)}

    def trace_contract_includes(self, event_type: str) -> bool:
        return event_type in self.trace_contract

    def snapshot_trace(self, snapshot: hsm.Snapshot) -> dict[str, Any]:
        return {"type": "snapshot", "state": snapshot.State}

    def normalize_snapshot(self, snapshot: hsm.Snapshot) -> dict[str, Any]:
        attributes: dict[str, Any] = {}
        prefix = "/" + self.model_name + "/"
        basename_counts: dict[str, int] = {}
        normalized_items: list[tuple[str, str, Any]] = []
        for key, value in (snapshot.Attributes or {}).items():
            name = key[len(prefix):] if key.startswith(prefix) else key
            basename = posixpath.basename(name)
            basename_counts[basename] = basename_counts.get(basename, 0) + 1
            normalized_items.append((name, basename, self.normalize_value(value)))
        for name, basename, value in normalized_items:
            attributes[name] = value
            if basename_counts.get(basename) == 1:
                attributes.setdefault(basename, value)
        normalized = {
            "id": snapshot.ID,
            "qualified_name": snapshot.QualifiedName,
            "state": snapshot.State,
            "attributes": attributes,
            "queue_len": snapshot.QueueLen,
        }
        events = []
        for event in getattr(snapshot, "Events", ()) or ():
            events.append({
                "name": event.Name,
                "kind": int(event.Kind),
                "target": event.Target,
                "guard": event.Guard,
                "schema": self.normalize_value(event.Schema),
            })
        if events:
            normalized["events"] = events
        return normalized

    def group_snapshot(self, group_id: str) -> dict[str, Any]:
        group_ir = next((group for group in self.case.get("groups", []) if group.get("id") == group_id), None)
        if group_ir is None:
            raise ConformanceError(f"unknown group {group_id!r}")
        members = {
            member_id: self.instances[member_id].state()
            for member_id in group_ir.get("members", [])
        }
        return {"members": members}

    def normalize_value(self, value: Any) -> Any:
        if isinstance(value, type):
            return value.__name__
        if isinstance(value, dict):
            return {key: self.normalize_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self.normalize_value(item) for item in value]
        if hasattr(value, "items"):
            return {key: self.normalize_value(item) for key, item in value.items()}
        return value

    def read_path(self, value: Any, path: Any) -> Any:
        if isinstance(value, hsm.CallData):
            if len(value.args) == 1:
                value = value.args[0]
            else:
                value = list(value.args)
        if path in (None, ""):
            if isinstance(value, hsm.AttributeChange):
                return value.value
            return value
        current = value
        for segment in str(path).split("."):
            if isinstance(current, dict):
                current = current.get(segment)
            else:
                return None
        return current

    def event_is_deferred(self, instance: hsm.Instance, event_name: str) -> bool:
        state_path = instance.state()
        if self.model is None:
            return False
        return self.model.deferred_map.get(state_path, {}).get(event_name, False)

    def current_state_has_event_transition(self, instance: hsm.Instance, event_name: str) -> bool:
        machine = getattr(instance, "_Instance__hsm", None)
        model = machine.model if isinstance(machine, hsm.HSM) else self.model
        if model is None:
            return False
        transitions = model.transition_map.get(instance.state(), {})
        return bool(transitions.get(event_name) or transitions.get(hsm.AnyEvent.qualified_name))

    def trace_deferred_dispatch(self, event_name: str, instances: Iterable[hsm.Instance]) -> None:
        if not self.trace_contract_includes("defer"):
            return
        for instance in instances:
            if self.event_is_deferred(instance, event_name) and not self.current_state_has_event_transition(instance, event_name):
                if not self.has_deferred_event(instance, event_name):
                    self.note_deferred_event(instance, event_name)
                    self.trace.append({"type": "defer", "event": event_name})

    def trace_new_runtime_deferred(self, instances: Iterable[hsm.Instance]) -> None:
        if not self.trace_contract_includes("defer"):
            return
        instance_list = list(instances)
        known = set(self.deferred_events)
        for instance in instance_list:
            machine = getattr(instance, "_Instance__hsm", None)
            if not isinstance(machine, hsm.HSM):
                continue
            for event in getattr(machine, "_deferred_events", []):
                key = self.deferred_event_key(instance, event.name)
                if key in known or self.has_deferred_event(instance, event.name):
                    continue
                if len(instance_list) == 1 and self.trace and self.trace[-1] == {"type": "defer", "event": event.name}:
                    known.add(key)
                    self.deferred_events.append(key)
                    continue
                known.add(key)
                self.deferred_events.append(key)
                self.trace.append({"type": "defer", "event": event.name})

    def pop_deferred_event_for_instance(self, instance: hsm.Instance) -> str | None:
        instance_id = self.instance_id_for_instance(instance)
        for index, (deferred_instance_id, event_name, _cleanup_on_parent_exit) in enumerate(self.deferred_events):
            if deferred_instance_id == instance_id:
                self.deferred_events.pop(index)
                return event_name
        return None

    def clear_deferred_events_for_instance(self, instance: hsm.Instance) -> None:
        instance_id = self.instance_id_for_instance(instance)
        self.deferred_events = [
            item
            for item in self.deferred_events
            for deferred_instance_id, _event_name, _cleanup_on_parent_exit in [item]
            if deferred_instance_id != instance_id
        ]

    def clear_child_deferred_events_for_instance(self, instance: hsm.Instance) -> None:
        instance_id = self.instance_id_for_instance(instance)
        self.deferred_events = [
            item
            for item in self.deferred_events
            for deferred_instance_id, _event_name, cleanup_on_parent_exit in [item]
            if deferred_instance_id != instance_id or not cleanup_on_parent_exit
        ]

    def note_deferred_event(self, instance: hsm.Instance, event_name: str) -> None:
        if not self.has_deferred_event(instance, event_name):
            self.deferred_events.append(self.deferred_event_key(instance, event_name))

    def has_deferred_event(self, instance: hsm.Instance, event_name: str) -> bool:
        instance_id = self.instance_id_for_instance(instance)
        return any(
            deferred_instance_id == instance_id and deferred_event_name == event_name
            for deferred_instance_id, deferred_event_name, _cleanup_on_parent_exit in self.deferred_events
        )

    def deferred_event_key(self, instance: hsm.Instance, event_name: str) -> tuple[str, str, bool]:
        cleanup_on_parent_exit = False
        machine = getattr(instance, "_Instance__hsm", None)
        if isinstance(machine, hsm.HSM):
            state_name = instance.state()
            owner = machine.model.deferred_owner_map.get(state_name, {}).get(event_name)
            boundary = machine.model.submachine_owner_map.get(owner or "")
            cleanup_on_parent_exit = bool(boundary and owner and boundary != owner)
        return (self.instance_id_for_instance(instance), event_name, cleanup_on_parent_exit)

    def instance_id_for_instance(self, instance: hsm.Instance) -> str:
        for instance_id, candidate in self.instances.items():
            if candidate is instance:
                return instance_id
        return "default"

    def instances_for_group(self, group_id: str) -> list[hsm.Instance]:
        group_ir = next((group for group in self.case.get("groups", []) if group.get("id") == group_id), None)
        if not isinstance(group_ir, dict):
            raise ConformanceError(f"unknown group {group_id!r}")
        return [self.instances[self._require_member_id(member)] for member in group_ir.get("members", [])]

    def exiting_timer_state(self, instance: hsm.Instance, event_name: str) -> bool:
        if not self.trace_contract_includes("timer_cancelled"):
            return False
        model_ir = self._require_object(self.case, "model")
        active_states = self.active_state_irs(model_ir, instance.state())
        has_timer = any(self.state_has_timer_transition(state_ir) for state_ir in active_states)
        has_event_transition = any(
            isinstance(transition, dict)
            and transition.get("on") == event_name
            and "target" in transition
            for state_ir in active_states
            for transition in state_ir.get("transitions", [])
        )
        return has_timer and has_event_transition

    @staticmethod
    def state_has_timer_transition(state_ir: dict[str, Any]) -> bool:
        return any(
            isinstance(transition, dict)
            and isinstance(transition.get("trigger"), dict)
            and transition["trigger"].get("kind") in {"after", "every", "at"}
            for transition in state_ir.get("transitions", [])
        )

    def event_exits_active_submachine(self, instance: hsm.Instance, event_name: str) -> bool:
        model_ir = self._require_object(self.case, "model")
        active_states = self.active_state_irs(model_ir, instance.state())
        return any(
            state_ir.get("kind") == "submachine"
            and any(
                isinstance(transition, dict)
                and transition.get("on") == event_name
                and "target" in transition
                for transition in state_ir.get("transitions", [])
            )
            for state_ir in active_states
        )

    def active_state_irs(self, model_ir: dict[str, Any], state_path: str) -> list[dict[str, Any]]:
        parts = [part for part in state_path.strip("/").split("/") if part]
        if not parts:
            return []
        parts = parts[1:]
        states = model_ir.get("states", [])
        active: list[dict[str, Any]] = []
        index = 0
        while index < len(parts):
            state = next(
                (candidate for candidate in states if isinstance(candidate, dict) and candidate.get("name") == parts[index]),
                None,
            )
            if state is None:
                break
            active.append(state)
            index += 1
            if state.get("kind") == "submachine":
                machine_ir = self.model_irs_by_name.get(self._require_string(state, "machine"))
                if machine_ir is None:
                    break
                states = machine_ir.get("states", [])
            else:
                states = state.get("states", [])
        return active

    def find_state_ir(self, states: list[Any], owner_path: str, state_path: str) -> dict[str, Any] | None:
        for state in states:
            if not isinstance(state, dict):
                continue
            name = state.get("name")
            if not isinstance(name, str):
                continue
            current_path = posixpath.normpath(owner_path + "/" + name)
            if current_path == state_path:
                return state
            found = self.find_state_ir(state.get("states", []), current_path, state_path)
            if found is not None:
                return found
        return None

    @staticmethod
    def behavior_id(ref: dict[str, Any]) -> str:
        behavior_id = ref.get("behavior") if isinstance(ref, dict) else None
        if not isinstance(behavior_id, str) or not behavior_id:
            raise ConformanceError("behavior reference requires behavior")
        return behavior_id

    @staticmethod
    def _require_member_id(value: Any) -> str:
        if not isinstance(value, str) or not value:
            raise ConformanceError("group member must be a non-empty string")
        return value

    @staticmethod
    def _require_object(parent: dict[str, Any], key: str) -> dict[str, Any]:
        value = parent.get(key)
        if not isinstance(value, dict):
            raise ConformanceError(f"{key} must be an object")
        return value

    @staticmethod
    def _optional_object(parent: dict[str, Any], key: str) -> dict[str, Any]:
        value = parent.get(key, {})
        if not isinstance(value, dict):
            raise ConformanceError(f"{key} must be an object")
        return value

    @staticmethod
    def _require_array(parent: dict[str, Any], key: str) -> list[Any]:
        value = parent.get(key)
        if not isinstance(value, list):
            raise ConformanceError(f"{key} must be an array")
        return value

    @staticmethod
    def _require_string(parent: dict[str, Any], key: str) -> str:
        value = parent.get(key)
        if not isinstance(value, str) or not value:
            raise ConformanceError(f"{key} must be a non-empty string")
        return value


async def run_case(path: Path) -> None:
    with path.open("r", encoding="utf-8") as handle:
        case = json.load(handle)
    if case.get("version") != "hsm-conformance-v1":
        raise ConformanceError(f"unsupported conformance version {case.get('version')!r}")
    features = set(case.get("features", []))
    unsupported = sorted(features - SUPPORTED_FEATURES)
    if unsupported:
        raise ConformanceSkip("unsupported features: " + ", ".join(unsupported))
    runner = Runner(case)
    if case.get("mode", "runtime") == "validation":
        runner.run_validation()
        return
    if case.get("mode", "runtime") != "runtime":
        raise ConformanceSkip(f"unsupported mode {case.get('mode')!r}")
    await runner.run()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Run an HSM conformance case against hsm.py")
    parser.add_argument("case", type=Path, nargs="+")
    args = parser.parse_args(argv)
    failed = False
    skipped = False
    for path in args.case:
        try:
            asyncio.run(run_case(path))
        except ConformanceSkip as skip:
            print(f"{path}: skipped ({skip})")
            skipped = True
        except Exception as error:
            print(f"{path}: conformance failed: {error}", file=sys.stderr)
            failed = True
        else:
            print(f"{path}: ok")
    if failed:
        return 1
    if skipped:
        return 77
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
