from hsm.kind import is_kind, kind
from hsm import Kinds


def test_hsm():
    assert is_kind(Kinds.Choice, Kinds.Pseudostate)
