# def _dict_to_sql(d, mode="data"):
#     if mode == "data":
#         statement = "named_struct("
#         for key, value in d.items():
#             statement += f"'{key}', {py_to_sql(value, mode=mode)}, "
#         statement = statement.rstrip(", ") + ")"
#
#     elif mode == "schema":
#         statement = "STRUCT<"
#         for key, value in d.items():
#             statement += f"{key}: {py_to_sql(value, mode=mode)}, "
#         statement = statement.rstrip(", ") + ">"
#
#     return statement
#
#
# def _list_to_sql(l, mode="data"):
#     if mode == "data":
#         statement = "ARRAY("
#         for value in l:
#             statement += f"{py_to_sql(value, mode=mode)}, "
#         statement = statement.rstrip(", ") + ")"
#     elif mode == "schema":
#         statement = "ARRAY<"
#         for value in l:
#             statement += f"{py_to_sql(value, mode=mode)}, "
#         statement = statement.rstrip(", ") + ">"
#
#     return statement
#
#
# def py_to_sql(value, mode="data"):
#     if isinstance(value, str):
#         if mode == "data":
#             return f"'{value}'"
#         elif mode == "schema":
#             return f"{value}"
#     elif value is None:
#         return f"null"
#     elif isinstance(value, list):
#         return _list_to_sql(value, mode=mode)
#     elif isinstance(value, dict):
#         return _dict_to_sql(value, mode=mode)
#     else:
#         return f"{value}"
#
