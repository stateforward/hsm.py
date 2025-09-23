length = 64
id_length = 8
depth_max = length // id_length
id_mask = (1 << id_length) - 1


def subkinds(kind: int) -> list[int]:
    subkinds: list[int] = []
    for i in range(depth_max):
        subkinds.append(kind | (1 << (i * id_length)))
    return subkinds


def is_kind(kind: int, *subkinds: int) -> bool:
    for subkind in subkinds:
        base_id = subkind & id_mask
        if kind == base_id:
            return True
        for i in range(depth_max):
            current_id = (kind >> (i * id_length)) & id_mask
            if current_id == base_id:
                return True
    return False


def kind(id: int, *subkinds: int) -> int:
    id = id & id_mask
    ids: set[int] = set()
    for subkind in subkinds:
        for i in range(depth_max):
            base_id = (subkind >> (i * id_length)) & id_mask
            if base_id == 0:
                break
            if base_id in ids:
                continue
            ids.add(base_id)
            id |= base_id << (id_length * len(ids))
    return id
