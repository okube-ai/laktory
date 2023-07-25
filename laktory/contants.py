import inspect
from pyspark.sql import types


SUPPORTED_TYPES = {}
for k in vars(types):
    o = getattr(types, k)
    if "Type" in k and inspect.isclass(o):
        try:
            typ = o()
        except TypeError:
            continue

        if not hasattr(typ, "simpleString"):
            continue

        SUPPORTED_TYPES[typ.simpleString()] = typ
