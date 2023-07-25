from pyspark.sql import types


SUPPORTED_TYPES = {}
for t in [
    types.DataType(),
    types.NullType(),
    types.AtomicType(),
    types.NumericType(),
    types.IntegralType(),
    types.FractionalType(),
    types.StringType(),
    types.BinaryType(),
    types.BooleanType(),
    types.DateType(),
    types.TimestampType(),
    types.TimestampNTZType(),
    types.DecimalType(10, 0),
    types.DoubleType(),
    types.FloatType(),
    types.ByteType(),
    types.IntegerType(),
    types.LongType(),
    types.DayTimeIntervalType(0, 3),
    types.ShortType(),
    types.StructType([]),
    types.UserDefinedType()
]:
    SUPPORTED_TYPES[t.simpleString()] = t


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
