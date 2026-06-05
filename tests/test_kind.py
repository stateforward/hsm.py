import hsm
import hsm.kind as kind_module
from hsm.kind import Is, List, Make


def test_hsm():
    assert hsm.kind.Is(hsm.ChoiceKind, hsm.PseudostateKind)


def test_make_kind_builds_hierarchy_without_manual_ids():
    element = Make()
    namespace = Make(element)
    vertex = Make(element)
    state = Make(vertex, namespace)

    assert Is(state, state)
    assert Is(state, vertex)
    assert Is(state, namespace)
    assert Is(state, element)


def test_kind_exports_match_current_api():
    assert kind_module.Make is Make
    assert kind_module.Is is Is
    assert kind_module.List is List
    assert kind_module.__all__ == ["List", "Make", "Is"]

    base = Make()
    child = Make(base)

    assert Is(child, base)


def test_list_exposes_base_ids():
    base = Make()
    child = Make(base)

    assert List(child)[0] == base


def test_kind_module_is_exported_from_top_level_package():
    assert hsm.kind is kind_module
