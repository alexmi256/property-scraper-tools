from collections import Counter
import functools
from deepmerge import Merger, STRATEGY_END

def merge_counters(config, path, base, nxt):
    """
    use all values in either base or nxt.
    """
    if isinstance(base, Counter) and isinstance(nxt, Counter):
        return base + nxt
    else:
        return STRATEGY_END


def merge_lists_with_dict_items(config, path, base, nxt):
    """
    use all values in either base or nxt.
    """
    if isinstance(base, list) and isinstance(nxt, list):
        custom_merger = Merger(
            [
                (Counter, merge_counters),
                (list, merge_lists_with_dict_items),
                (dict, ["merge"]),
                (set, ["union"]),
            ],
            ["override"],
            ["override"],
        )

        merged_items = {}
        items = base + nxt

        functools.reduce(lambda a, b: custom_merger.merge(a, b), items, merged_items)
        return [merged_items]
    else:
        return STRATEGY_END