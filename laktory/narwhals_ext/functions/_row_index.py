import narwhals as nw


def row_index() -> nw.Expr:
    """
    Window function: returns a sequential number starting at 1 within a window partition.

    Returns
    -------
    :
        Output column

    Examples
    --------
    # ```py
    # import narwhals as nw
    # import polars as pl
    #
    # import laktory as lk  # noqa: F401
    #
    # df = nw.from_native(
    #     pl.DataFrame(
    #         {
    #             "x": ["a", "a", "b", "b", "b", "c"],
    #             "z": ["11", "10", "22", "21", "20", "30"],
    #         }
    #     )
    # )
    # df = df.with_columns(y1=nw.laktory.row_number())
    # df = df.with_columns(y2=nw.laktory.row_number().over("x"))
    # df = df.with_columns(y3=nw.laktory.row_number().over("x", sort_by="z"))
    # print(df)
    # '''
    # Rows: 6
    # Columns: 5
    # $ x  <str> 'a', 'a', 'b', 'b', 'b', 'c'
    # $ z  <str> '11', '10', '22', '21', '20', '30'
    # $ y1 <u32> 1, 2, 3, 4, 5, 6
    # $ y2 <u32> 1, 2, 1, 2, 3, 1
    # $ y3 <u32> 2, 1, 3, 2, 1, 1
    # '''
    # ```
    """

    # TODO: This one needs to be implemented as function in Narwhals and it's not
    # trivial. We might take a shot at implementing it or simply use df.with_row_index()
    # https://github.com/narwhals-dev/narwhals/issues/2307
    raise NotImplementedError()
