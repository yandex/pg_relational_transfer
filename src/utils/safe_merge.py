from copy import deepcopy

from deepmerge.merger import Merger


merger = Merger(
    type_strategies=[(list, ["append"]), (dict, ["merge"]), (set, ["union"])],
    fallback_strategies=["override"],
    type_conflict_strategies=["override_if_not_empty"],
)


def safe_merge(d1: dict, d2: dict) -> dict:
    d3 = deepcopy(d1)
    merger.merge(d3, d2)
    return d3
