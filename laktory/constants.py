SUPPORTED_TYPES = [
    "_any",
    "data",
    "void",
    "atomic",
    "numeric",
    "integral",
    "fractional",
    "string",
    "binary",
    "boolean",
    "date",
    "timestamp",
    "timestamp_ntz",
    "decimal(10,0)",
    "double",
    "float",
    "tinyint",
    "int",
    "bigint",
    "interval day to second",
    "smallint",
    "struct<>",
    "udt",
]

# CACHE_ROOT = "./laktory"
CACHE_ROOT = "./"
LAKTORY_WORKSPACE_ROOT = "/.laktory/"
# for k in vars(types):
#     o = getattr(types, k)
#     if "Type" in k and inspect.isclass(o):
#         try:
#             typ = o()
#         except TypeError:
#             continue
#
#         if not hasattr(typ, "simpleString"):
#             continue
#
#         SUPPORTED_TYPES[typ.simpleString()] = typ
