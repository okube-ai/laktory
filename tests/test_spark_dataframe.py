from pyspark.sql import SparkSession
from pyspark.sql import types as T

import laktory

schema = T.StructType(
    [
        T.StructField("x@x", T.IntegerType()),
        T.StructField("y", T.ArrayType(T.StringType())),
        T.StructField(
            "z",
            T.StructType(
                [
                    T.StructField("id", T.IntegerType()),
                    T.StructField("email", T.StringType()),
                ]
            ),
        ),
        T.StructField(
            "u",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("a", T.IntegerType()),
                        T.StructField("b", T.IntegerType()),
                    ]
                )
            ),
        ),
        T.StructField(
            "u2",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("a", T.IntegerType()),
                        T.StructField("b", T.IntegerType()),
                    ]
                )
            ),
        ),
    ]
)

data = [
    (
        1,
        ["a", "b"],
        {"id": 3, "email": "@gmail.com"},
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
    ),
    (
        2,
        ["b", "c"],
        {"id": 2, "email": "@gmail.com"},
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
    ),
    (
        3,
        ["c", "d"],
        {"id": 1, "email": "@gmail.com"},
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
    ),
]

spark = SparkSession.builder.appName("UnitTesting").getOrCreate()

df = spark.createDataFrame(data, schema=schema)


def test_df_schema_flat():
    df.show()
    df.printSchema()
    schema = df.schema_flat()
    assert schema == [
        "x@x",
        "y",
        "z",
        "z.id",
        "z.email",
        "u",
        "u[*].a",
        "u[*].b",
        "u2",
        "u2[*].a",
        "u2[*].b",
    ]


def test_df_has_column():
    assert df.has_column("x@x")
    assert df.has_column("`x@x`")
    assert df.has_column("u[0].a")


if __name__ == "__main__":
    test_df_schema_flat()
    test_df_has_column()
