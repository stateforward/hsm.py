import datetime

import pytest

import hsm


def test_only_base_redefinable_element_is_publicly_exported() -> None:
    redefinable_exports = {
        name for name in hsm.__all__ if name.startswith("Redefinable")
    }

    assert redefinable_exports == {"RedefinableElement"}


class RecordingValidator(hsm.ModelValidator):
    def __init__(self) -> None:
        super().__init__()
        self.seen: list[str] = []

    def validate(self, model: hsm.Model) -> None:
        self.seen.append(model.qualified_name)
        super().validate(model)


class RecordingFinalizer:
    def __init__(self) -> None:
        self.seen: list[str] = []

    def finalize(self, model: hsm.Model) -> hsm.Model:
        self.seen.append(model.qualified_name)
        return hsm.DefaultModelFinalizer().finalize(model)


def test_define_accepts_validator_element_override() -> None:
    validator = RecordingValidator()

    model = hsm.Define(
        "CustomValidator",
        hsm.Validator(validator),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )

    assert validator.seen == ["/CustomValidator"]
    assert isinstance(model, hsm.FinalizedModel)


def test_redefine_replays_validator_and_accepts_override() -> None:
    original = RecordingValidator()
    override = RecordingValidator()

    model = hsm.Define(
        "LateValidator",
        hsm.Validator(original),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )
    replayed = model.redefine(model, ())
    replaced = model.redefine(model, (hsm.Validator(override),))

    assert isinstance(replayed, hsm.FinalizedModel)
    assert isinstance(replaced, hsm.FinalizedModel)
    assert original.seen == ["/LateValidator", "/LateValidator"]
    assert override.seen == ["/LateValidator"]


def test_define_accepts_finalizer_element_override() -> None:
    finalizer = RecordingFinalizer()

    model = hsm.Define(
        "CustomFinalizer",
        hsm.Finalizer(finalizer),
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )

    assert finalizer.seen == ["/CustomFinalizer"]
    assert isinstance(model, hsm.FinalizedModel)


def test_transition_kind_is_resolved_before_finalizer() -> None:
    seen: list[int] = []

    class Finalizer:
        def finalize(self, model: hsm.Model) -> hsm.Model:
            transition = next(
                member
                for member in model.members.values()
                if isinstance(member, hsm.TransitionElement)
                and "go" in member.events
            )
            seen.append(transition.kind)
            return hsm.DefaultModelFinalizer().finalize(model)

    model = hsm.Define(
        "KindBeforeFinalizer",
        hsm.Finalizer(Finalizer()),
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )

    assert isinstance(model, hsm.FinalizedModel)
    assert seen == [hsm.ExternalKind]


def test_define_returns_finalized_model_with_runtime_indexes() -> None:
    model = hsm.Define(
        "FinalizedIndexes",
        hsm.Initial(hsm.Target("idle")),
        hsm.State(
            "idle",
            hsm.Transition(hsm.On("go"), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )

    assert isinstance(model, hsm.FinalizedModel)
    assert not hasattr(hsm.Model(qualified_name="/Plain"), "transition_map")
    assert "/FinalizedIndexes/idle" in model.transition_map
    assert "go" in model.transition_map["/FinalizedIndexes/idle"]

    transition = model.transition_map["/FinalizedIndexes/idle"]["go"][0]

    assert model.transition_paths[transition.qualified_name][
        "/FinalizedIndexes/idle"
    ].enter == [
        "/FinalizedIndexes/done"
    ]


@pytest.mark.parametrize(
    ("builder", "mode"),
    [
        (hsm.After, "after"),
        (hsm.At, "at"),
        (hsm.Every, "every"),
        (hsm.When, "when"),
    ],
)
def test_time_and_when_transitions_finalize_source_activity(
    builder, mode: str
) -> None:
    def duration(ctx, inst, event):
        del ctx, inst, event
        return datetime.timedelta(seconds=1)

    def timepoint(ctx, inst, event):
        del ctx, inst, event
        return datetime.datetime.now() + datetime.timedelta(seconds=1)

    def signal(ctx, inst, event):
        del ctx, inst, event
        return None

    expression = {"after": duration, "at": timepoint, "every": duration}.get(
        mode, signal
    )

    model = hsm.Define(
        f"{mode.title()}Finalization",
        hsm.Initial(hsm.Target("waiting")),
        hsm.State(
            "waiting",
            hsm.Transition(builder(expression), hsm.Target("../done")),
        ),
        hsm.State("done"),
    )

    state = model.members[f"/{mode.title()}Finalization/waiting"]
    assert isinstance(state, hsm.StateElement)
    assert len(state.activity) == 1

    activity = model.members[state.activity[0]]
    assert isinstance(activity, hsm.BehaviorElement)
    assert activity.kind == hsm.ConcurrentKind

    transition = next(
        member
        for member in model.members.values()
        if isinstance(member, hsm.TransitionElement)
        and member.source == state.qualified_name
    )
    assert len(transition.events) == 1
    event = model.events[transition.events[0]]
    assert event.kind == hsm.TimeEventKind
    assert callable(event.data)
    assert transition.events[0] in model.transition_map[state.qualified_name]


def test_concurrent_behavior_requires_async_operation() -> None:
    def sync_activity(ctx, inst, event):
        del ctx, inst, event
        return None

    with pytest.raises(
        hsm.ErrorValidatingModel,
        match="concurrent behavior must be an async function",
    ):
        hsm.Define(
            "SyncActivity",
            hsm.Initial(hsm.Target("active")),
            hsm.State("active", hsm.Activity(sync_activity)),
        )


def test_time_transition_from_choice_is_rejected_as_pseudostate_trigger() -> None:
    def duration(ctx, inst, event):
        del ctx, inst, event
        return datetime.timedelta(seconds=1)

    with pytest.raises(
        hsm.ErrorValidatingModel,
        match="outgoing pseudostate .* cannot have triggers",
    ):
        hsm.Define(
            "TimedChoice",
            hsm.Initial(hsm.Target("branch")),
            hsm.Choice(
                "branch",
                hsm.Transition(hsm.After(duration), hsm.Target("done")),
            ),
            hsm.State("done"),
        )


def test_define_requires_initial_state() -> None:
    with pytest.raises(hsm.ErrorValidatingModel, match="initial state is required"):
        hsm.Define("MissingInitial", hsm.State("idle"))


def test_define_rejects_missing_initial_target() -> None:
    with pytest.raises(hsm.ErrorValidatingModel, match="target .* not found"):
        hsm.Define(
            "MissingInitialTarget",
            hsm.Initial(hsm.Target("missing")),
            hsm.State("idle"),
        )


def test_define_rejects_missing_transition_target() -> None:
    with pytest.raises(hsm.ErrorValidatingModel, match="target .* not found"):
        hsm.Define(
            "MissingTransitionTarget",
            hsm.Initial(hsm.Target("idle")),
            hsm.State(
                "idle",
                hsm.Transition(hsm.On("go"), hsm.Target("../missing")),
            ),
        )


def test_define_allows_targetless_event_transition() -> None:
    model = hsm.Define(
        "InternalTransition",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("tick"))),
    )

    transitions = [
        member
        for member in model.members.values()
        if isinstance(member, hsm.TransitionElement)
        and member.source == "/InternalTransition/idle"
    ]

    assert len(transitions) == 1
    assert transitions[0].target == ""


def test_define_rejects_choice_without_transitions() -> None:
    with pytest.raises(hsm.ErrorValidatingModel, match="choice .* has no transitions"):
        hsm.Define(
            "EmptyChoice",
            hsm.Initial(hsm.Target("idle")),
            hsm.State("idle", hsm.Choice("branch")),
        )


def test_default_validator_rejects_final_state_behaviors() -> None:
    model = hsm.Model(qualified_name="/FinalValidation")
    model.initial = "/FinalValidation/.initial"
    initial = hsm.InitialElement(qualified_name="/FinalValidation/.initial")
    final = hsm.FinalStateElement(qualified_name="/FinalValidation/done")
    behavior = hsm.BehaviorElement(qualified_name="/FinalValidation/done/entry")
    model.members[initial.qualified_name] = initial
    model.members[final.qualified_name] = final
    model.members[behavior.qualified_name] = behavior
    final.entry.append(behavior.qualified_name)

    with pytest.raises(
        hsm.ErrorValidatingModel, match="final state cannot have an entry action"
    ):
        hsm.DefaultModelValidator().validate(model)


def test_define_rejects_async_entry_and_exit_behaviors() -> None:
    async def async_entry(ctx, inst, event):
        del ctx, inst, event

    async def async_exit(ctx, inst, event):
        del ctx, inst, event

    with pytest.raises(
        hsm.ErrorValidatingModel, match="entry must be a synchronous function"
    ):
        hsm.Define(
            "AsyncEntry",
            hsm.Initial(hsm.Target("idle")),
            hsm.State("idle", hsm.Entry(async_entry)),
        )

    with pytest.raises(
        hsm.ErrorValidatingModel, match="exit must be a synchronous function"
    ):
        hsm.Define(
            "AsyncExit",
            hsm.Initial(hsm.Target("idle")),
            hsm.State("idle", hsm.Exit(async_exit)),
        )


def test_define_rejects_initial_guard_and_user_trigger() -> None:
    def guard(ctx, inst, event):
        del ctx, inst, event
        return True

    with pytest.raises(
        hsm.ErrorValidatingModel, match="initial transition .* cannot have a guard"
    ):
        hsm.Define(
            "GuardedInitial",
            hsm.Initial(hsm.Target("idle"), hsm.Guard(guard)),
            hsm.State("idle"),
        )

    with pytest.raises(
        hsm.ErrorValidatingModel, match="initial transition .* cannot have triggers"
    ):
        hsm.Define(
            "TriggeredInitial",
            hsm.Initial(hsm.Target("idle"), hsm.On("start")),
            hsm.State("idle"),
        )


def test_default_validator_rejects_hand_built_region_cardinality() -> None:
    model = hsm.Model(qualified_name="/DuplicateInitial")
    model.initial = "/DuplicateInitial/.initial"
    model.members["/DuplicateInitial/.initial"] = hsm.InitialElement(
        qualified_name="/DuplicateInitial/.initial"
    )
    model.members["/DuplicateInitial/other"] = hsm.InitialElement(
        qualified_name="/DuplicateInitial/other"
    )

    with pytest.raises(
        hsm.ErrorValidatingModel, match="more than one initial vertex"
    ):
        hsm.DefaultModelValidator().validate(model)


def test_default_validator_rejects_final_state_regions_and_submachine() -> None:
    model = hsm.Model(qualified_name="/FinalRegions")
    model.initial = "/FinalRegions/.initial"
    model.members["/FinalRegions/.initial"] = hsm.InitialElement(
        qualified_name="/FinalRegions/.initial"
    )
    final = hsm.FinalStateElement(qualified_name="/FinalRegions/done")
    model.members[final.qualified_name] = final
    model.members["/FinalRegions/done/child"] = hsm.StateElement(
        qualified_name="/FinalRegions/done/child"
    )

    with pytest.raises(
        hsm.ErrorValidatingModel, match="final state cannot have regions"
    ):
        hsm.DefaultModelValidator().validate(model)

    model.members.pop("/FinalRegions/done/child")
    final.submachine = hsm.Model(qualified_name="/Child")

    with pytest.raises(
        hsm.ErrorValidatingModel, match="final state cannot reference a submachine"
    ):
        hsm.DefaultModelValidator().validate(model)
