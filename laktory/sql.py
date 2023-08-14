def _dict_to_statement(d):
    statement = "named_struct("
    for key, value in d.items():
        statement += f"'{key}', {value_to_statement(value)}, "
    statement = statement.rstrip(", ") + ")"
    return statement


def _list_to_statement(l):
    statement = "array("
    for value in l:
        statement += f"{value_to_statement(value)}, "
    statement = statement.rstrip(", ") + ")"
    return statement


def value_to_statement(value):

    if isinstance(value, str):
        return f"'{value}'"
    elif value is None:
        return f"null"
    elif isinstance(value, list):
        return _list_to_statement(value)
    elif isinstance(value, dict):
        return _dict_to_statement(value)
    else:
        return f"{value}"


#
# def dict_to_schema(input_dict):
#     schema = "STRUCT<"
#     for key, value in input_dict.items():
#         if isinstance(value, dict):
#             nested_schema = dict_to_schema(value)
#             schema += f"{key}:{nested_schema}, "
#         elif isinstance(value, bool):
#             schema += f"{key}:boolean, "
#         elif isinstance(value, str):
#             schema += f"{key}:string, "
#         # Add more data type checks here if needed
#         else:
#             raise ValueError(f"Unsupported data type for key '{key}': {type(value).__name__}")
#
#     # Remove the trailing comma and space
#     schema = schema.rstrip(", ") + ">"
#     return schema