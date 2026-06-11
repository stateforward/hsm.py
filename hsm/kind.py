from __future__ import annotations

import builtins
import typing

length = 64
id_length = 8
depth_max = length // id_length
id_mask = (1 << id_length) - 1

_counter = 0


Kind: typing.TypeAlias = int


def _extract_id(kind_value: Kind, depth: int) -> int:
    return (int(kind_value) >> (depth * id_length)) & id_mask


def _next_id() -> int:
    global _counter
    id_ = _counter & id_mask
    _counter += 1
    return id_


def List(kind_value: Kind) -> builtins.list[Kind]:
    return [_extract_id(kind_value, depth) for depth in range(1, depth_max)]


def Make(*base_kinds: Kind) -> Kind:
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


def Is(kind_value: Kind, *bases: Kind) -> bool:
    kind_int = int(kind_value)
    for base in bases:
        base_id = int(base) & id_mask
        if kind_int == base_id:
            return True
        current = kind_int
        for _ in range(depth_max):
            if current & id_mask == base_id:
                return True
            current >>= id_length
            if current == 0:
                break
    return False


__all__ = [
    "List",
    "Make",
    "Is",
]
