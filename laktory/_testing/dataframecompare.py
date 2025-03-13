import narwhals as nw


def assert_dfs_equal(result, expected, sort=True) -> None:
    # Convert to Narwhals
    result = nw.from_native(result)
    expected = nw.from_native(expected)

    # Convert to pandas
    if result.implementation == nw.Implementation.PYSPARK:
        result = result.to_native().toPandas()
    else:
        if isinstance(result, nw.LazyFrame):
            result = result.collect()
        result = result.to_pandas()

    if expected.implementation == nw.Implementation.PYSPARK:
        expected = expected.to_native().toPandas()
    else:
        if isinstance(expected, nw.LazyFrame):
            expected = expected.collect()
        expected = expected.to_pandas()

    # Compare columns
    assert result.columns.to_list() == expected.columns.to_list()
    columns = result.columns.to_list()

    # Compare rows
    assert len(result) == len(expected)

    if sort:
        result = result.sort_values(columns)
        expected = expected.sort_values(columns)

    # Compare content
    for c in columns:
        assert result[c].to_list() == expected[c].to_list()
