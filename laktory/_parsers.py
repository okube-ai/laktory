import copy


def _snake_to_camel(snake_str):
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def camelize_keys(d, parent=None, excluded_parents=None):
    if isinstance(d, dict):
        if parent and excluded_parents and parent in excluded_parents:
            return d
        keys = list(d.keys())
        values = list(d.values())
        for key, value in zip(keys, values):
            new_key = _snake_to_camel(key)
            d[new_key] = camelize_keys(
                value, parent=key, excluded_parents=excluded_parents
            )
            if new_key != key:
                del d[key]

    elif isinstance(d, list):
        for i, item in enumerate(d):
            d[i] = camelize_keys(item)
    else:
        pass
    return d


def merge_dicts(d1: dict, d2: dict) -> dict:
    dm = copy.deepcopy(d1)

    def _merge_dicts(d1, d2):
        for key, value in d2.items():
            if key in d1 and isinstance(d1[key], dict) and isinstance(value, dict):
                _merge_dicts(d1[key], value)
            else:
                d1[key] = value

    _merge_dicts(dm, d2)

    return dm
