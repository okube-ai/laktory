import polars.datatypes as T


DATATYPES_MAP = {

    "binary": T.Binary,

    "byte": T.Int8,
    "int8": T.Int8,
    "tinyint": T.Int8,

    "short": T.Int16,
    "int16": T.Int16,
    "smallint": T.Int16,

    "int": T.Int32,
    "int32": T.Int32,

    "long": T.Int64,
    "int64": T.Int64,
    "bigint": T.Int64,

    "float": T.Float32,
    "float32": T.Float32,

    "double": T.Float64,
    "float64": T.Float64,

    "boolean": T.Boolean,

    "string": T.Utf8,
    "utf8": T.Utf8,

    "date": T.Date,

    "timestamp": T.Datetime,
    "datetime": T.Datetime,

    # "struct<>": T.Struct,
    # "struct": T.Struct,

    # "array": T.List,
    # "list": T.List,
}
