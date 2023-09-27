import json

spark_installed = False
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.utils import AnalysisException

    spark_installed = True

except ModuleNotFoundError:

    class DataFrame:
        pass


def df_schema_flat(df):
    # TODO: Manage list of dicts

    def get_fields(json_schema):
        field_names = []
        for f in json_schema.get("fields", []):
            f_name = f["name"]
            if isinstance(f["type"], dict):
                _field_names = get_fields(f["type"])
                if len(_field_names) == 0:
                    field_names += [f_name]
                else:
                    field_names += [f"{f_name}.{v}" for v in _field_names]
            else:
                field_names += [f_name]
        return field_names

    return get_fields(json.loads(df.schema.json()))


def df_has_column(df, col):
    return col in df_schema_flat(df)
