import hsm
import hsm.kind as kind_module
from hsm.kind import IsKind, List, Make, MakeKind, is_kind, kind, make, make_kind
from hsm import Kinds


def test_hsm():
    assert is_kind(Kinds.Choice, Kinds.Pseudostate)


def test_make_kind_builds_hierarchy_without_manual_ids():
    element = MakeKind()
    namespace = MakeKind(element)
    vertex = MakeKind(element)
    state = MakeKind(vertex, namespace)

    assert IsKind(state, state)
    assert IsKind(state, vertex)
    assert IsKind(state, namespace)
    assert IsKind(state, element)


def test_snake_case_and_kind_aliases_match_pascal_case():
    assert Make is MakeKind
    assert make is MakeKind
    assert make_kind is MakeKind
    assert kind is MakeKind
    assert kind_module.list is List
    assert "list" in kind_module.__all__
    assert is_kind is IsKind

    base = MakeKind()
    child = make(base)
    sibling = kind(base)
    named = make_kind(base)

    assert is_kind(child, base)
    assert IsKind(sibling, base)
    assert IsKind(named, base)


def test_list_exposes_base_ids():
    base = MakeKind()
    child = MakeKind(base)

    assert List(child)[0] == base


def test_kind_helpers_are_exported_from_top_level_module():
    assert hsm.MakeKind is MakeKind
    assert hsm.make_kind is make_kind
    assert hsm.IsKind is IsKind
    assert hsm.is_kind is is_kind
