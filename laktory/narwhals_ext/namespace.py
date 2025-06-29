import narwhals as nw


class NameSpace:
    def __init__(self, name: str, ns_cls: type):
        self._name = name
        self._ns_cls = ns_cls

    def __get__(self, instance, owner):
        if instance is None:
            return self._ns_cls
        ns_instance = self._ns_cls(instance)
        setattr(instance, self._name, ns_instance)
        return ns_instance


def register_expr_namespace(name: str):
    """
    Decorator for registering custom method to a Narwhals Expression.

    Parameters
    ----------
    name:
        Name of the namespace

    Examples
    -------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk


    @lk.api.register_expr_namespace("custom")
    class CustomNamespace:
        def __init__(self, _expr):
            self._expr = _expr

        def double(self):
            return self._expr * 2


    df = nw.from_native(pl.DataFrame({"x": [0, 1]}))

    df = df.with_columns(x2=nw.col("x").custom.double())

    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | x2 |    |
    |    |---|----|    |
    |    | 0 | 0  |    |
    |    | 1 | 2  |    |
    └──────────────────┘
    '''
    ```

    References
    ----------
    * [Narwhals Extension](https://www.laktory.ai/concepts/extension_custom/)
    """

    def wrapper(ns_cls: type):
        setattr(nw.Expr, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper


def register_anyframe_namespace(name: str):
    """
    Decorator for registering custom method to a Narwhals DataFrame and LazyFrame.

    Parameters
    ----------
    name:
        Name of the namespace

    Examples
    -------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk


    @lk.api.register_anyframe_namespace("custom")
    class CustomNamespace:
        def __init__(self, _df):
            self._df = _df

        def with_x2(self):
            return self._df.with_columns(x2=nw.col("x") * 2)


    df = nw.from_native(pl.DataFrame({"x": [0, 1]}))

    df = df.custom.with_x2()

    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | x2 |    |
    |    |---|----|    |
    |    | 0 | 0  |    |
    |    | 1 | 2  |    |
    └──────────────────┘
    '''
    ```

    References
    ----------
    * [Narwhals Extension](https://www.laktory.ai/concepts/extension_custom/)
    """

    def wrapper(ns_cls: type):
        setattr(nw.DataFrame, name, NameSpace(name, ns_cls))
        setattr(nw.LazyFrame, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper


def register_dataframe_namespace(name: str):
    """
    Decorator for registering custom method to a Narwhals DataFrame.

    Parameters
    ----------
    name:
        Name of the namespace

    Examples
    -------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk


    @lk.api.register_dataframe_namespace("custom")
    class CustomNamespace:
        def __init__(self, _df):
            self._df = _df

        def with_x2(self):
            return self._df.with_columns(x2=nw.col("x") * 2)


    df = nw.from_native(pl.DataFrame({"x": [0, 1]}))

    df = df.custom.with_x2()

    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | x2 |    |
    |    |---|----|    |
    |    | 0 | 0  |    |
    |    | 1 | 2  |    |
    └──────────────────┘
    '''
    ```

    References
    ----------
    * [Narwhals Extension](https://www.laktory.ai/concepts/extension_custom/)
    """

    def wrapper(ns_cls: type):
        setattr(nw.DataFrame, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper


def register_lazyframe_namespace(name: str):
    """
    Decorator for registering custom method to a Narwhals LazyFrame.

    Parameters
    ----------
    name:
        Name of the namespace

    Examples
    -------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk


    @lk.api.register_lazyframe_namespace("custom")
    class CustomNamespace:
        def __init__(self, _df):
            self._df = _df

        def with_x2(self):
            return self._df.with_columns(x2=nw.col("x") * 2)


    df = nw.from_native(pl.DataFrame({"x": [0, 1]}).lazy())

    df = df.custom.with_x2()

    print(df.collect().sort("x"))
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | x2 |    |
    |    |---|----|    |
    |    | 0 | 0  |    |
    |    | 1 | 2  |    |
    └──────────────────┘
    '''
    ```

    References
    ----------
    * [Narwhals Extension](https://www.laktory.ai/concepts/extension_custom/)
    """

    def wrapper(ns_cls: type):
        setattr(nw.LazyFrame, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper
