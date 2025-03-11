import narwhals as nw


def assert_dfs_equal(result, expected):
    # Convert to Narwhals
    result = nw.from_native(result)
    expected = nw.from_native(expected)

    # Compare columns
    assert result.columns == expected.columns

    # Convert to pandas
    if result.implementation == nw.Implementation.PYSPARK:
        result = result.to_native().toPandas()
    else:
        if isinstance(result, nw.LazyFrame):
            result = result.collect().to_pandas()
        else:
            result = result.to_pandas()

    if expected.implementation == nw.Implementation.PYSPARK:
        expected = expected.to_native().toPandas()
    else:
        if isinstance(expected, nw.LazyFrame):
            expected = expected.collect().to_pandas()
        else:
            expected = expected.to_pandas()

    # Compare rows
    assert len(result) == len(expected)

    # Compare content
    for c in expected.columns:
        assert result[c].to_list() == expected[c].to_list()
