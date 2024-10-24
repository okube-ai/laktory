import polars as pl
import numpy as np
import polars.datatypes as T

from laktory import models

df = pl.DataFrame(
    {
        "x": [1, 2, 3],
        "a": [1, -1, 1],
        "b": [2, 0, 2],
        "c": [3, 0, 3],
        "n": [4, 0, 4],
        "pi": [np.pi] * 3,
        "p": [2.0, 0.2, 0.05],
        "word": ["dog_cat", "dog_cat_mouse", "dog"],
    },
)
df0 = df.select(df.columns)
df1 = df.select(df.columns)


def test_func_arg(df0=df0):
    df = df0.select(df0.columns)

    new_cols = []
    # Values to parse
    for i, v in enumerate(
        [
            "lit(3)",
            "lit(3.0)",
            "lit('3')",
            "col('x')",
            "col('x') + lit(3)",
            "sql_expr('2*x+a')",
        ]
    ):
        new_col = f"c{i}"
        a = models.PolarsChainNodeFuncArg(value=v)
        df = df.with_columns(**{new_col: a.eval()})
        new_cols += [new_col]

    # Test new column types
    data = dict(df.select(new_cols).schema)
    assert data == {
        "c0": T.Int32,
        "c1": T.Float64,
        "c2": T.String,
        "c3": T.Int64,
        "c4": T.Int64,
        "c5": T.Int64,
    }

    # Values not to parse
    for v0 in [
        "x",
        "3",
    ]:
        v1 = models.PolarsChainNodeFuncArg(value=v0).eval()
        assert v1 == v0


def test_df_input(df0=df0):
    df = df0.select(df0.columns)

    # Define Chain
    sc = models.PolarsChain(
        nodes=[
            {
                "func_name": "laktory.union",
                "func_args": [df1],
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df.height == df0.height * 2
    assert df.columns == df0.columns


def test_sql_expression(df0=df0):
    df = df0.select(df0.columns)

    sc = models.PolarsChain(
        nodes=[
            {
                "sql_expr": "SELECT *, x*2 AS x2 FROM {df}",
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    df = df.collect()
    assert df.columns == ["x", "a", "b", "c", "n", "pi", "p", "word", "x2"]
    assert df["x2"].to_list() == (df["x"] * 2).to_list()


def test_sql_with_nodes():

    sc = models.PolarsChain(
        nodes=[
            {
                "sql_expr": "SELECT * FROM {df}",
            },
            {
                "sql_expr": "SELECT * FROM {df} UNION SELECT * FROM {nodes.node_01} UNION SELECT * FROM {nodes.node_02}",
            },
        ]
    )

    assert sc.nodes[0].parsed_sql_expr.node_data_sources == []
    assert sc.nodes[1].parsed_sql_expr.node_data_sources == [
        models.PipelineNodeDataSource(node_name="node_01", dataframe_type="POLARS"),
        models.PipelineNodeDataSource(node_name="node_02", dataframe_type="POLARS"),
    ]


def test_table_input(df0=df0):
    df = df0.select(df0.columns)

    sc = models.PolarsChain(
        nodes=[
            {
                "func_name": "laktory.union",
                "func_args": [
                    {"mock_df": df},
                ],
            }
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df.height == df0.height * 2
    assert df.columns == df0.columns


def test_column(df0=df0):
    df = df0.select(df0.columns)
    df = df.with_columns(xs=pl.lit([1, 2, 2]))

    sc = models.PolarsChain(
        nodes=[
            {
                "with_column": {
                    "name": "cos_x",
                    "type": "double",
                    "expr": "col('x').cos()",
                },
            },
            {
                "with_column": {
                    "name": "x2",
                    "type": "double",
                    "expr": "x*2",
                },
            },
            {
                "with_column": {
                    "name": "xplode",
                    "type": None,
                    "expr": "pl.col('xs').list.unique()",
                },
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert [(c.name, c.expr.value) for c in sc.nodes[0]._with_columns] == [
        ("cos_x", "col('x').cos()")
    ]
    assert [(c.name, c.expr.value) for c in sc.nodes[1]._with_columns] == [
        ("x2", "x*2")
    ]
    assert df["cos_x"].to_list() == np.cos(df["x"]).to_list()
    assert df["x2"].to_list() == (df["x"] * 2).to_list()

    # Test explode
    assert df["xplode"].to_list() == [[1, 2], [1, 2], [1, 2]]

    # Multi-columns
    sc = models.PolarsChain(
        nodes=[
            {
                "with_columns": [
                    {
                        "name": "sin_x",
                        "type": "double",
                        "expr": "col('x').sin()",
                    },
                    {
                        "name": "x3",
                        "type": "double",
                        "expr": "x*3",
                    },
                ]
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df["sin_x"].to_list() == np.sin(df["x"]).to_list()
    assert df["x3"].to_list() == (df["x"] * 3).to_list()


def test_udfs(df0=df0):
    df = df0.select(df0.columns)

    def mul3(c):
        return 3 * c

    def add_new_col(df, column_name, s=1):
        return df.with_columns(**{column_name: pl.col("x") * s})

    sc = models.PolarsChain(
        nodes=[
            {
                "with_column": {
                    "name": "rp",
                    "type": "double",
                    "expr": "pl.col('p').laktory.roundp(p=0.1)",
                },
            },
            {
                "with_column": {
                    "name": "x3",
                    "type": "double",
                    "expr": "mul3(col('x'))",
                },
            },
            {
                "func_name": "add_new_col",
                "func_args": ["y"],
                "func_kwargs": {"s": 5},
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df, udfs=[mul3, add_new_col])

    # Test
    assert df["rp"].to_list() == [2.0, 0.2, 0.1]
    assert df["x3"].to_list() == (df["x"] * 3).to_list()
    assert df["y"].to_list() == (df["x"] * 5).to_list()


def test_nested(df0=df0):
    df = df0.select(df0.columns)
    df = df.with_columns(_x2=pl.col("x").sqrt())

    sc = models.PolarsChain(
        nodes=[
            {
                "with_column": {
                    "name": "cos_x",
                    "type": "double",
                    "expr": "pl.col('x').cos()",
                },
            },
            {
                "nodes": [
                    {
                        "func_name": "rename",
                        "func_args": [
                            {"x": "x_tmp"},
                        ],
                    },
                    {
                        "with_column": {
                            "name": "x2",
                            "type": "double",
                            "expr": "pl.col('x_tmp').sqrt()",
                        },
                    },
                ],
            },
            {
                "func_name": "drop",
                "func_args": [
                    "x_tmp",
                ],
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert "x_tmp" not in df.columns
    assert df["x2"].to_list() == df["_x2"].to_list()
    assert sc.columns == [
        ["x", "a", "b", "c", "n", "pi", "p", "word", "_x2"],
        ["x", "a", "b", "c", "n", "pi", "p", "word", "_x2", "cos_x"],
        ["x_tmp", "a", "b", "c", "n", "pi", "p", "word", "_x2", "cos_x", "x2"],
    ]


# TODO: Re-enable when coalesce is ready
def atest_exceptions():

    return

    df = df0.select(df0.columns)

    # Input missing - missing allowed
    sc = models.PolarsChain(
        nodes=[
            {
                "with_column": {
                    "name": "xy",
                    "type": "double",
                    "expr": "pl.Expr.coalesce('x', 'y')",
                },
            },
        ]
    )
    df = sc.execute(df)
    assert "xy" in df.columns
    #
    # # All inputs missing
    # sc = models.PolarsChain(
    #     nodes=[
    #         {
    #             "column": {"name": "xy", "type": "double"},
    #             "func_name": "coalesce",
    #             "func_args": ["col('z')", "col('y')"],
    #             "allow_missing_column_args": True,
    #         },
    #     ]
    # )
    # with pytest.raises(MissingColumnsError):
    #     df = sc.execute(df)


if __name__ == "__main__":
    test_func_arg()
    test_df_input()
    test_sql_expression()
    test_sql_with_nodes()
    test_table_input()
    test_column()
    test_udfs()
    test_nested()
    # atest_exceptions()
