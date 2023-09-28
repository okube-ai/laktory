import json
import re

spark_installed = False
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.utils import AnalysisException

    spark_installed = True

except ModuleNotFoundError:

    class DataFrame:
        pass


def df_schema_flat(df):
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
                else:
                    raise ValueError(f_type["type"])
            else:
                field_names += [f_name]
        return field_names

    return get_fields(json.loads(df.schema.json()))


def df_has_column(df, col):
    _col = re.sub(r"\d+", "*", col)
    return _col in df_schema_flat(df)
