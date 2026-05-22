from __future__ import annotations

import builtins

length = 64
id_length = 8
depth_max = length // id_length
id_mask = (1 << id_length) - 1

_counter = 0


def _extract_id(kind_value: int, depth: int) -> int:
    return (int(kind_value) >> (depth * id_length)) & id_mask


def _next_id() -> int:
    global _counter
    id_ = _counter & id_mask
    _counter += 1
    return id_


def List(kind_value: int) -> builtins.list[int]:
    return [_extract_id(kind_value, depth) for depth in range(1, depth_max)]


list_kind = List


def Bases(kind_value: int) -> builtins.list[int]:
    return List(kind_value)


bases = Bases


subkinds = Bases


def MakeKind(*base_kinds: int) -> int:
    id_ = _next_id()
    ids: set[int] = set()
    for base in base_kinds:
        for depth in range(depth_max):
            base_id = _extract_id(base, depth)
            if base_id == 0:
                break
            if base_id in ids:
                continue
            ids.add(base_id)
            id_ |= base_id << (id_length * len(ids))
    return id_


Make = MakeKind
make = MakeKind
make_kind = MakeKind
kind = MakeKind


def IsKind(kind_value: int, *base_kinds: int) -> bool:
    for base in base_kinds:
        base_id = int(base) & id_mask
        if int(kind_value) == base_id:
            return True
        for depth in range(depth_max):
            current_id = _extract_id(kind_value, depth)
            if current_id == base_id:
                return True
    return False


is_kind = IsKind
list = List


__all__ = [
    "Bases",
    "IsKind",
    "List",
    "Make",
    "MakeKind",
    "bases",
    "depth_max",
    "id_length",
    "id_mask",
    "is_kind",
    "kind",
    "length",
    "list",
    "list_kind",
    "make",
    "make_kind",
    "subkinds",
]
