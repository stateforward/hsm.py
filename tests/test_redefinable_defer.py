import pytest

from hsm import hsm


def test_defer_registers_event_names_on_state():
    model = hsm.Define(
        "DeferRedefine",
        hsm.State("holding", hsm.Defer("work", hsm.Event(name="pause"))),
        hsm.Initial(hsm.Target("holding")),
    )
    state = model.members["/DeferRedefine/holding"]
    assert state.deferred == ["work", "pause"]
    assert model.events["work"].name == "work"
    assert model.events["pause"].name == "pause"


def test_defer_outside_state_raises():
    defer = hsm.Defer("work")
    model = hsm.Model(qualified_name="/Bare")
    with pytest.raises(
        hsm.ErrorValidatingModel, match="defer must be called within a State"
    ):
        defer.redefine(model, [])


def test_defer_inside_transition_registers_on_owner_state():
    model = hsm.Define(
        "TransitionDeferOwner",
        hsm.State(
            "idle",
            hsm.Transition(
                hsm.On("go"),
                hsm.Defer("work"),
                hsm.Target("../done"),
            ),
        ),
        hsm.State("done"),
        hsm.Initial(hsm.Target("idle")),
    )

    state = model.members["/TransitionDeferOwner/idle"]
    assert state.deferred == ["work"]
