import copy


def _snake_to_camel(snake_str):
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def remove_empty(d):
    if isinstance(d, dict):
        keys = list(d.keys())
        values = list(d.values())
        for key, value in zip(keys, values):
            if value in [None, [], {}]:
                del d[key]
            else:
                d[key] = remove_empty(d[key])

    elif isinstance(d, list):
        for i, item in enumerate(d):
            if item in [None, [], {}]:
                del d[i]
            else:
                d[i] = remove_empty(item)

    return d


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
            elif key in d1 and isinstance(d1[key], list) and isinstance(value, dict):
                for index, sub_value in value.items():
                    idx = int(index)
                    if isinstance(d1[key][idx], dict):
                        _merge_dicts(d1[key][idx], sub_value)
                    else:
                        d1[key][idx] = sub_value
            else:
                d1[key] = value

    _merge_dicts(dm, d2)

    return dm
