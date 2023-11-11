import re
import json
from pyspark.sql.dataframe import DataFrame


def schema_flat(df: DataFrame) -> list[str]:
    def get_fields(json_schema):
        field_names = []
        for f in json_schema.get("fields", []):
            f_name = f["name"]
            f_type = f["type"]
            if isinstance(f_type, dict):
                if f_type["type"] == "array":
                    e_type = f_type["elementType"]
                    field_names += [f_name]
                    if isinstance(e_type, dict):
                        _field_names = get_fields(f_type["elementType"])
                        field_names += [f"{f_name}[*].{v}" for v in _field_names]
                elif f_type["type"] == "struct":
                    _field_names = get_fields(f["type"])
                    field_names += [f_name]
                    field_names += [f"{f_name}.{v}" for v in _field_names]
                elif f_type["type"] == "map":
                    field_names += [f_name]
                else:
                    raise ValueError(f_type["type"])
            else:
                field_names += [f_name]
        return field_names

    return get_fields(json.loads(df.schema.json()))


def has_column(df: DataFrame, col: str) -> bool:
    _col = re.sub(r"\[(\d+)\]", r"[*]", col)
    _col = re.sub(r"`", "", _col)
    return _col in schema_flat(df)


# DataFrame Extensions
DataFrame.schema_flat = schema_flat
DataFrame.has_column = has_column

# Spark Connect DataFrame Extensions
try:
    from pyspark.sql.connect.dataframe import DataFrame

    DataFrame.schema_flat = schema_flat
    DataFrame.has_column = has_column
except:
    pass
