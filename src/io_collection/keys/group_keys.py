from itertools import groupby


def group_keys(keys: list[str]) -> dict[str, list[str]]:
    """
    Group keys based on individual key parts.

    A key is composed of parts separated by a single underscores (`_`). Keys are
    grouped such that each key group consists of the subset of keys with the
    matching part at the given position.

    Returns
    -------
    :
        The map of key groups to keys.
    """

    parts = [key.split("_") for key in keys]
    num_parts = len(parts[0])

    if not all(len(part) == num_parts for part in parts):
        message = "All keys must have the same number of parts"
        raise ValueError(message)

    groups = {}

    for group_index in range(num_parts):
        sorted_keys = sorted(parts, key=lambda k: k[group_index])
        for group, part in groupby(sorted_keys, key=lambda k: k[group_index]):
            groups[group] = ["_".join(p) for p in part]

    return groups
