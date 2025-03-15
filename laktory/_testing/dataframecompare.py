import narwhals as nw


def assert_dfs_equal(result, expected, sort=True) -> None:
    # Convert to Narwhals
    result = nw.from_native(result)
    expected = nw.from_native(expected)

    # Convert to pandas
    if isinstance(result, nw.LazyFrame):
        result = result.collect("pandas")
    result = result.to_pandas()

    if isinstance(expected, nw.LazyFrame):
        expected = expected.collect("pandas")
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
