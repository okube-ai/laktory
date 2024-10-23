QUICKSTART_TEMPLATES = [
    "unity-catalog",
    "workspace",
    "workflows",
    "local-pipeline",
]

SUPPORTED_BACKENDS = [
    "pulumi",
    "terraform",
]

SUPPORTED_DFTYPES = [
    "POLARS",
    "SPARK",
]

DEFAULT_DFTYPE = "SPARK"

SUPPORTED_DATATYPES = [
    "binary",
    "byte",
    "int8",
    "tinyint",
    "short",
    "int16",
    "smallint",
    "int",
    "int32",
    "long",
    "int64",
    "bigint",
    "float",
    "float32",
    "double",
    "float64",
    "boolean",
    "string",
    "utf8",
    "date",
    "timestamp",
    "datetime",
    # "struct<>",
    # "struct",
    # "array",
    # "list",
]

# CACHE_ROOT = "./laktory"
CACHE_ROOT = "./"

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
#         SUPPORTED_DATATYPES[typ.simpleString()] = typ
